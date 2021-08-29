import time
from datetime import datetime
from typing import List, Tuple
from gzip import BadGzipFile

import requests
from loguru import logger

from .utils import get_revision_from_url
from .webdriver import WebDriver
from .db import WizDiffDatabase, FileUpdateType
from .delta import (
    DeletedFileDelta,
    ChangedFileDelta,
    CreatedFileDelta,
    FileDelta,
    DeletedWadFileDelta,
    CreatedWadFileDelta,
    ChangedWadfileDelta,
    WadInnerFileInfo,
)

from .dml_parser import parse_records_from_bytes

JOURNAL_RETRIES = 10
JOURNAL_SLEEP_TIME = 60


class UpdateNotifier:
    def __init__(self, *, sleep_time: float = 3_600, delete_old_revisions: bool = True):
        self.sleep_time = sleep_time
        self.delete_old_revisions = delete_old_revisions

        self.db = WizDiffDatabase()
        self.webdriver = WebDriver()

    def _get_wad_journal(self, wad_url: str):
        for _ in range(JOURNAL_RETRIES):
            try:
                return self.webdriver.get_wad_journal_crcs(wad_url)
            except requests.exceptions.HTTPError as error:
                logger.info(f"Got non-200 error code {error}")
            except BadGzipFile as error:
                logger.debug(
                    f"Couldn't decompress data from {wad_url} as gz data; error: {error}"
                )

            logger.info(f"Retrying {wad_url} in {JOURNAL_SLEEP_TIME} seconds")
            time.sleep(JOURNAL_SLEEP_TIME)

        raise ValueError(f"Could not fetch journal for {wad_url}")

    def init_db(self):
        self.db.init_database()
        file_list_url, base_url = self.webdriver.get_patch_urls()
        revision = get_revision_from_url(file_list_url)
        self.add_revision(revision)
        self._fill_db(file_list_url, base_url, revision)

    def _fill_db(self, file_list_url: str, base_url: str, revision: str):
        file_list_records = self.get_file_list_records(file_list_url)

        for table_name, records in file_list_records.items():
            # meta tables
            if table_name in ["_TableList", "About"]:
                continue

            for record in records:
                name = record["SrcFileName"]
                logger.debug(f"Filling db with versioned file {name}")

                self.db.add_versioned_file_info(
                    record["CRC"], record["Size"], revision, name
                )

                # wad archive
                if name.endswith(".wad"):
                    wad_url = base_url + "/" + name
                    journal_crcs = self._get_wad_journal(wad_url)

                    for inner_file, (crc, size, compressed_size, is_compressed) in journal_crcs.items():
                        logger.debug(f"Filling db with wad inner file {inner_file}")

                        self.db.add_wad_file_info(
                            crc,
                            size,
                            compressed_size,
                            is_compressed,
                            revision,
                            inner_file,
                            name,
                        )

        self.db.commit()

    def update_loop(self):
        while True:
            file_list_url, base_url = self.webdriver.get_patch_urls()
            revision = get_revision_from_url(file_list_url)

            if self.db.check_if_new_revision(revision):
                self.new_revision(revision, file_list_url, base_url)

            else:
                logger.info(f"No new revision found")

            logger.info(f"Sleeping for {self.sleep_time} seconds")
            time.sleep(self.sleep_time)

    def add_revision(self, name: str):
        logger.info(f"Adding revision {name}")
        self.db.add_revision_info(name, datetime.utcnow())

    def remove_revision(self, name: str):
        logger.info(f"Deleting old revision {name}")
        self.db.delete_revision_info(name)
        self.db.delete_versioned_file_infos_with_revision(name)
        self.db.delete_wad_file_infos_with_revision(name)

    def get_file_list_records(self, file_list_url: str):
        file_list_data = self.webdriver.get_url_data(file_list_url)
        return parse_records_from_bytes(file_list_data)

    def new_revision(self, revision_name: str, file_list_url: str, base_url: str):
        logger.info(f"New revision found: {revision_name}")
        self.notify_revision_update(revision_name)

        old_revision = self.db.get_latest_revision()
        self.check_if_versioned_files_changed(
            old_revision, revision_name, file_list_url, base_url
        )

        revision = get_revision_from_url(file_list_url)
        self.add_revision(revision)

        if self.delete_old_revisions:
            self.remove_revision(old_revision)

    def check_if_versioned_files_changed(
            self,
            old_revision: str,
            new_revision: str,
            file_list_url: str,
            base_url: str,
    ):
        records = self.get_file_list_records(file_list_url)

        if not old_revision:
            raise ValueError(f"Old revision must be a string not {type(old_revision)}")

        last_revision_files = self.db.get_all_versioned_files_from_revision(
            old_revision
        )

        new_file_names = []

        for table_name, records in records.items():
            # meta tables
            if table_name in ["_TableList", "About"]:
                continue

            for record in records:
                name = record["SrcFileName"]

                new_file_names.append(name)

                # logger.debug(f"Checking if {name} updated")

                res, (old_crc, old_size) = self.db.check_if_versioned_file_updated(
                    record["CRC"], record["Size"], old_revision, name
                )

                file_url = base_url + "/" + name

                # TODO: 3.10 switch to match
                if res is FileUpdateType.changed:
                    if name.endswith(".wad"):
                        deleted, created, changed = self.check_if_wad_files_updated(
                            file_url,
                            name,
                            new_revision,
                            old_revision,
                        )

                        delta = ChangedWadfileDelta(
                            name=name,
                            revision=new_revision,
                            url=file_url,
                            new_crc=record["CRC"],
                            new_size=record["Size"],
                            old_crc=old_crc,
                            old_size=old_size,
                            deleted_inner_files=deleted,
                            created_inner_files=created,
                            changed_inner_files=changed,
                        )

                        self.notify_all_file_update(delta)
                        self.notify_wad_file_update(delta)

                    else:
                        delta = ChangedFileDelta(
                            name=name,
                            revision=new_revision,
                            url=file_url,
                            new_crc=record["CRC"],
                            new_size=record["Size"],
                            old_crc=old_crc,
                            old_size=old_size,
                        )

                        self.notify_all_file_update(delta)
                        self.notify_non_wad_file_update(delta)

                elif res is FileUpdateType.new:
                    if name.endswith(".wad"):
                        deleted, created, changed = self.check_if_wad_files_updated(
                            file_url,
                            name,
                            new_revision,
                            old_revision,
                        )

                        if deleted or changed:
                            raise RuntimeError(
                                f"New wad file should not have deleted or changed inner files {deleted=} {changed=}"
                            )

                        delta = CreatedWadFileDelta(
                            name=name,
                            revision=new_revision,
                            url=file_url,
                            new_crc=record["CRC"],
                            new_size=record["Size"],
                            old_crc=old_crc,
                            old_size=old_size,
                            created_inner_files=created,
                        )

                        self.notify_all_file_update(delta)
                        self.notify_wad_file_update(delta)

                    else:
                        delta = CreatedFileDelta(
                            name=name,
                            revision=new_revision,
                            url=file_url,
                            new_crc=record["CRC"],
                            new_size=record["Size"],
                            old_crc=old_crc,
                            old_size=old_size,
                        )

                        self.notify_all_file_update(delta)
                        self.notify_non_wad_file_update(delta)

                else:
                    logger.debug(f"Unchanged file {name}")

                # need to be versioned even if unchanged
                self.db.add_versioned_file_info(
                    record["CRC"], record["Size"], new_revision, record["SrcFileName"]
                )

        self.db.commit()

        for crc, size, revision, name in last_revision_files:
            if name not in new_file_names:
                if name.endswith(".wad"):
                    deleted_inner_files = []

                    for inner_file in self.db.get_all_wad_files_from_wad_name_and_revision(name, old_revision):
                        deleted_inner_files.append(
                            #  0 crc, 1 size_, 2 name, 3 file_offset, 4 compressed_size, 5 is_compressed
                            WadInnerFileInfo(
                                inner_file[2],
                                name,
                                0,
                                0,
                                inner_file[4],
                                inner_file[5],
                                inner_file[1],
                                inner_file[0]
                            )
                        )

                    delta = DeletedWadFileDelta(
                        name=name,
                        revision=revision,
                        url=base_url + name,
                        old_crc=crc,
                        old_size=size,
                        deleted_inner_files=deleted_inner_files,
                    )

                    self.notify_all_file_update(delta)
                    self.notify_wad_file_update(delta)

                else:
                    delta = DeletedFileDelta(
                        name=name,
                        revision=revision,
                        url=base_url + name,
                        old_crc=crc,
                        old_size=size,
                    )

                    self.notify_all_file_update(delta)
                    self.notify_non_wad_file_update(delta)

    def check_if_wad_files_updated(self, wad_url: str, wad_name: str, revision_name: str, old_revision: str):
        """
        only called on wads that are created or changed

        -> (deleted, created, changed)
        """
        journal_crcs = self._get_wad_journal(wad_url)

        deleted_inner_files = []
        created_inner_files = []
        changed_inner_files = []

        for inner_file, (crc, size, compressed_size, is_compressed) in journal_crcs.items():
            res, (old_crc, old_size) = self.db.check_if_wad_file_updated(crc, size, old_revision, inner_file, wad_name)

            inner_file_info = WadInnerFileInfo(
                inner_file,
                wad_name,
                size,
                crc,
                compressed_size,
                is_compressed,
                old_size,
                old_crc
            )

            # TODO: 3.10 switch to match
            if res is FileUpdateType.new:
                created_inner_files.append(inner_file_info)

            elif res is FileUpdateType.changed:
                changed_inner_files.append(inner_file_info)

            # unchanged
            else:
                pass

            self.db.add_wad_file_info(
                crc,
                size,
                compressed_size,
                is_compressed,
                revision_name,
                inner_file,
                wad_name,
            )

        for inner_file in self.db.get_all_wad_files_from_wad_name_and_revision(wad_name, old_revision):
            if inner_file not in journal_crcs.keys():
                deleted_inner_files.append(
                    #  0 crc, 1 size_, 2 name, 3 file_offset, 4 compressed_size, 5 is_compressed
                    WadInnerFileInfo(
                        inner_file[2],
                        wad_name,
                        0,
                        0,
                        inner_file[4],
                        inner_file[5],
                        inner_file[1],
                        inner_file[0]
                    )
                )

        return deleted_inner_files, created_inner_files, changed_inner_files

    def notify_revision_update(self, revision: str):
        pass

    def notify_all_file_update(self, delta: FileDelta):
        pass

    def notify_non_wad_file_update(self, delta: FileDelta):
        pass

    def notify_wad_file_update(self, delta: FileDelta):
        pass


class WebhookUpdateNotifier(UpdateNotifier):
    def __init__(self, webhook_urls: List[str], thread_id: int = None, **kwargs):
        super().__init__(**kwargs)
        self.webhook_urls = webhook_urls
        self.thread_id = thread_id

    def send_to_webhook(
            self,
            webhook_url,
            content: str,
            *,
            file: Tuple[str, bytes] = None,
            params: dict = None,
            **json_fields,
    ):
        """
        file is a tuple of file name and file data
        """
        to_send = {
            "content": content,
        }
        to_send.update(json_fields)

        files = None

        if file:
            files = [file]

        if self.thread_id:
            if not params:
                params = {}

            params["thread_id"] = self.thread_id

        requests.post(
            webhook_url,
            json=to_send,
            files=files,
            params=params,
        )

    def send_to_all_webhooks(self, *args, **kwargs):
        for webhook_url in self.webhook_urls:
            self.send_to_webhook(webhook_url, *args, **kwargs)

    def notify_revision_update(self, revision: str):
        self.send_to_all_webhooks(f"New revision {revision} found")

    def notify_non_wad_file_update(self, delta: FileDelta):
        # TODO: 3.10 switch to match

        if (delta_type := type(delta)) is CreatedFileDelta:
            delta: CreatedFileDelta
            self.send_to_all_webhooks(
                f"New file {delta.name} found; download at: {delta.url}"
            )

        elif delta_type is ChangedFileDelta:
            delta: ChangedFileDelta
            message = f"Changed file {delta.name} found; "

            if delta.old_size > delta.new_size:
                message += f"{delta.old_size - delta.new_size} bytes smaller; "

            elif delta.old_size < delta.new_size:
                message += f"{delta.new_size - delta.old_size} bytes larger; "

            # crc change
            else:
                message += "size unchanged (new crc); "

            message += f"download at {delta.url}"

            self.send_to_all_webhooks(message)

        elif delta_type is DeletedFileDelta:
            delta: DeletedFileDelta
            self.send_to_all_webhooks(f"File {delta.name} was deleted")

        else:
            raise RuntimeError(f"Unhandled delta type {delta_type}")
