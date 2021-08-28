import time
from datetime import datetime
from typing import Optional, Tuple

import requests
from loguru import logger

from .utils import get_patch_urls, get_revision_from_url, get_url_data
from .db import WizDiffDatabase, FileUpdateType
from .delta import DeletedFileDelta, ChangedFileDelta, CreatedFileDelta, FileDelta

from .dml_parser import parse_records_from_bytes


class UpdateNotifier:
    def __init__(self, *, sleep_time: float = 3_600):
        self.sleep_time = sleep_time
        self.db = WizDiffDatabase()

    def init_db(self):
        self.db.init_database()
        file_list_url, base_url = get_patch_urls()
        revision = get_revision_from_url(file_list_url)
        self.add_revision(revision)
        self.fill_db(file_list_url, revision)

    def fill_db(self, file_list_url: str, revision: str):
        file_list_records = self.get_file_list_records(file_list_url)

        for table_name, records in file_list_records.items():
            # meta tables
            if table_name in ["_TableList", "About"]:
                continue

            for record in records:
                logger.debug(f"Filling db with {record}")
                self.db.add_versioned_file_info(
                    record["CRC"], record["Size"], revision, record["SrcFileName"]
                )

    def update_loop(self):
        while True:
            file_list_url, base_url = get_patch_urls()
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

    @staticmethod
    def get_file_list_records(file_list_url: str):
        file_list_data = get_url_data(file_list_url)
        return parse_records_from_bytes(file_list_data)

    def new_revision(self, revision_name: str, file_list_url: str, base_url: str):
        logger.info(f"New revision found: {revision_name}")
        self.revision_update(revision_name)

        old_revision = self.db.get_latest_revision()
        self.check_if_versioned_files_changed(old_revision, revision_name, file_list_url, base_url)

        revision = get_revision_from_url(file_list_url)
        self.add_revision(revision)

        self.fill_db(file_list_url, revision_name)

    def check_if_versioned_files_changed(
            self,
            old_revision: Optional[str],
            new_revision: str,
            file_list_url: str,
            base_url: str,
    ):
        records = self.get_file_list_records(file_list_url)

        # This should never happen?
        # TODO: error here instead?
        if old_revision:
            last_revision_files = self.db.get_all_versioned_files_from_revision(old_revision)

        else:
            last_revision_files = []

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
                    record["CRC"],
                    record["Size"],
                    old_revision,
                    name
                )

                file_url = base_url + "/" + name

                # TODO: 3.10 switch to match
                if res is FileUpdateType.changed:
                    delta = ChangedFileDelta(
                        name=name,
                        revision=new_revision,
                        url=file_url,
                        new_crc=record["CRC"],
                        new_size=record["Size"],
                        old_crc=old_crc,
                        old_size=old_size,
                    )
                    self.versioned_file_update(delta)

                elif res is FileUpdateType.new:
                    delta = CreatedFileDelta(
                        name=name,
                        revision=new_revision,
                        url=file_url,
                        new_crc=record["CRC"],
                        new_size=record["Size"],
                        old_crc=old_crc,
                        old_size=old_size,
                    )
                    self.versioned_file_update(delta)

                else:
                    logger.debug(f"Unchanged file {name}")

        for crc, size, revision, name in last_revision_files:
            if name not in new_file_names:
                delta = DeletedFileDelta(
                    name=name,
                    revision=revision,
                    url=base_url + name,
                    old_crc=crc,
                    old_size=size
                )
                self.versioned_file_update(delta)

    def revision_update(self, revision: str):
        """
        Runs before file check
        """
        raise NotImplementedError()

    def versioned_file_update(self, delta: FileDelta):
        raise NotImplementedError()

    # # TODO
    # def wad_file_update(self):
    #     pass


class WebhookUpdateNotifier(UpdateNotifier):
    def __init__(self, webhook_url: str, thread_id: int = None, *, sleep_time: float = 3_600):
        super().__init__(sleep_time=sleep_time)
        self.webhook_url = webhook_url
        self.thread_id = thread_id

    def send_to_webhook(self, content: str, *, file: Tuple[str, bytes] = None, params: dict = None, **json_fields):
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
            self.webhook_url,
            json=to_send,
            files=files,
            params=params,
        )

    def revision_update(self, revision: str):
        self.send_to_webhook(f"New revision {revision} found")

    def versioned_file_update(self, delta: FileDelta):
        # TODO: 3.10 switch to match

        if (delta_type := type(delta)) is CreatedFileDelta:
            delta: CreatedFileDelta
            self.send_to_webhook(f"New file {delta.name} found; download at: {delta.url}")

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

            self.send_to_webhook(message)

        elif delta_type is DeletedFileDelta:
            delta: DeletedFileDelta
            self.send_to_webhook(f"File {delta.name} was deleted")

        else:
            raise RuntimeError(f"Unhandled delta type {delta_type}")

