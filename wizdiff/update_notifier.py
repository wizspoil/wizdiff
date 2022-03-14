import asyncio
import zlib
from datetime import datetime
from gzip import BadGzipFile

import aiohttp
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

from .dml_parser import parse_records_from_bytes, parse_records_from_xml

JOURNAL_RETRIES = 10
JOURNAL_SLEEP_TIME = 60


class UpdateNotifier:
    def __init__(self, *, sleep_time: float = 3_600, delete_old_revisions: bool = True, use_xml_file_list: bool = True):
        self.sleep_time = sleep_time
        self.delete_old_revisions = delete_old_revisions
        self.use_xml_file_list = use_xml_file_list

        self.db = WizDiffDatabase()
        self.webdriver = WebDriver()

    async def _get_wad_journal(self, wad_url: str):
        for _ in range(JOURNAL_RETRIES):
            try:
                return await self.webdriver.get_wad_journal_crcs(wad_url)
            except aiohttp.ClientResponseError as error:
                logger.info(f"Got non-200 error code {error}")
            except BadGzipFile as error:
                logger.debug(
                    f"Couldn't decompress data from {wad_url} as gz data; error: {error}"
                )

            logger.info(f"Retrying {wad_url} in {JOURNAL_SLEEP_TIME} seconds")
            await asyncio.sleep(JOURNAL_SLEEP_TIME)

        raise ValueError(f"Could not fetch journal for {wad_url}")

    async def init_db(self):
        await self.db.init_database()
        file_list_url, base_url = await self.webdriver.get_patch_urls()
        revision = get_revision_from_url(file_list_url)
        await self.add_revision(revision)
        await self._fill_db(file_list_url, base_url, revision)

    async def _fill_db(self, file_list_url: str, base_url: str, revision: str):
        file_list_records = await self.get_file_list_records(file_list_url)

        for table_name, records in file_list_records.items():
            # meta tables
            if table_name in ["_TableList", "About"]:
                continue

            for record in records:
                name = record["SrcFileName"]
                logger.debug(f"Filling db with versioned file {name}")

                await self.db.add_versioned_file_info(
                    record["CRC"], record["Size"], revision, name
                )

                # wad archive
                if name.endswith(".wad"):
                    wad_url = base_url + "/" + name
                    journal_crcs = await self._get_wad_journal(wad_url)

                    for inner_file, (file_offset, crc, size, compressed_size, is_compressed) in journal_crcs.items():
                        logger.debug(f"Filling db with wad inner file {inner_file}")

                        await self.db.add_wad_file_info(
                            file_offset,
                            crc,
                            size,
                            compressed_size,
                            is_compressed,
                            revision,
                            inner_file,
                            name,
                        )

        await self.db.commit()

    async def update_loop(self):
        while True:
            file_list_url, base_url = await self.webdriver.get_patch_urls()
            revision = get_revision_from_url(file_list_url)

            if await self.db.check_if_new_revision(revision):
                await self.new_revision(revision, file_list_url, base_url)

            else:
                logger.info(f"No new revision found")

            logger.info(f"Sleeping for {self.sleep_time} seconds")
            await asyncio.sleep(self.sleep_time)

    async def add_revision(self, name: str):
        logger.info(f"Adding revision {name}")
        await self.db.add_revision_info(name, datetime.utcnow())

    async def remove_revision(self, name: str):
        logger.info(f"Deleting old revision {name}")
        await self.db.delete_revision_info(name)
        await self.db.delete_versioned_file_infos_with_revision(name)
        await self.db.delete_wad_file_infos_with_revision(name)

    async def get_file_list_records(self, file_list_url: str):
        if self.use_xml_file_list:
            file_list_data = await self.webdriver.get_url_data(file_list_url.replace(".bin", ".xml"))
            return parse_records_from_xml(file_list_data)

        else:
            file_list_data = await self.webdriver.get_url_data(file_list_url)
            return parse_records_from_bytes(file_list_data)

    async def new_revision(self, revision_name: str, file_list_url: str, base_url: str):
        logger.info(f"New revision found: {revision_name}")
        await self.notify_revision_update(revision_name)

        old_revision = await self.db.get_latest_revision()
        await self.check_if_versioned_files_changed(
            old_revision, revision_name, file_list_url, base_url
        )

        await self.add_revision(revision_name)

        if self.delete_old_revisions:
            await self.remove_revision(old_revision)

    async def check_if_versioned_files_changed(
            self,
            old_revision: str,
            new_revision: str,
            file_list_url: str,
            base_url: str,
    ):
        records = await self.get_file_list_records(file_list_url)

        if not old_revision:
            raise ValueError(f"Old revision must be a string not {type(old_revision)}")

        last_revision_files = await self.db.get_all_versioned_files_from_revision(
            old_revision
        )

        new_file_names = []
        unchanged_wads = []

        for table_name, records in records.items():
            # meta tables
            if table_name in ["_TableList", "About"]:
                continue

            for record in records:
                name = record["SrcFileName"]

                new_file_names.append(name)

                # logger.debug(f"Checking if {name} updated")

                res, (old_crc, old_size) = await self.db.check_if_versioned_file_updated(
                    record["CRC"], record["Size"], old_revision, name
                )

                file_url = base_url + "/" + name

                # TODO: 3.10 switch to match
                if res is FileUpdateType.changed:
                    if name.endswith(".wad"):
                        deleted, created, changed = await self.check_if_wad_files_updated(
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

                        await self.notify_all_file_update(delta)
                        await self.notify_wad_file_update(delta)

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

                        await self.notify_all_file_update(delta)
                        await self.notify_non_wad_file_update(delta)

                elif res is FileUpdateType.new:
                    if name.endswith(".wad"):
                        deleted, created, changed = await self.check_if_wad_files_updated(
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

                        await self.notify_all_file_update(delta)
                        await self.notify_wad_file_update(delta)

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

                        await self.notify_all_file_update(delta)
                        await self.notify_non_wad_file_update(delta)

                else:
                    if name.endswith(".wad"):
                        unchanged_wads.append(name)

                # need to be versioned even if unchanged
                await self.db.add_versioned_file_info(
                    record["CRC"], record["Size"], new_revision, record["SrcFileName"]
                )

        # this is so the wad inner files are updated to the latest revision
        await self.db.mass_update_wad_file_infos_revision_with_wad_names(unchanged_wads, new_revision)
        await self.db.commit()

        for crc, size, revision, name in last_revision_files:
            if name not in new_file_names:
                if name.endswith(".wad"):
                    deleted_inner_files = []

                    for inner_file in await self.db.get_all_wad_files_from_wad_name_and_revision(name, old_revision):
                        deleted_inner_files.append(
                            #  0 crc, 1 size_, 2 name, 3 file_offset, 4 compressed_size, 5 is_compressed
                            WadInnerFileInfo(
                                name=inner_file[2],
                                wad_name=name,
                                size=0,
                                crc=0,
                                compressed_size=inner_file[4],
                                is_compressed=inner_file[5],
                                file_offset=inner_file[3],
                                old_size=inner_file[1],
                                old_crc=inner_file[0],
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

                    await self.notify_all_file_update(delta)
                    await self.notify_wad_file_update(delta)

                else:
                    delta = DeletedFileDelta(
                        name=name,
                        revision=revision,
                        url=base_url + name,
                        old_crc=crc,
                        old_size=size,
                    )

                    await self.notify_all_file_update(delta)
                    await self.notify_non_wad_file_update(delta)

    async def check_if_wad_files_updated(self, wad_url: str, wad_name: str, revision_name: str, old_revision: str):
        """
        only called on wads that are created or changed

        -> (deleted, created, changed)
        """
        journal_crcs = await self._get_wad_journal(wad_url)

        deleted_inner_files = []
        created_inner_files = []
        changed_inner_files = []

        for inner_file, (file_offset, crc, size, compressed_size, is_compressed) in journal_crcs.items():
            res, (old_crc, old_size) = await self.db.check_if_wad_file_updated(
                crc,
                size,
                old_revision,
                inner_file,
                wad_name
            )

            inner_file_info = WadInnerFileInfo(
                name=inner_file,
                wad_name=wad_name,
                size=size,
                crc=crc,
                compressed_size=compressed_size,
                is_compressed=is_compressed,
                file_offset=file_offset,
                old_size=old_size,
                old_crc=old_crc
            )

            # TODO: 3.10 switch to match
            if res is FileUpdateType.new:
                created_inner_files.append(inner_file_info)

            elif res is FileUpdateType.changed:
                changed_inner_files.append(inner_file_info)

            # unchanged
            else:
                pass

            await self.db.add_wad_file_info(
                file_offset,
                crc,
                size,
                compressed_size,
                is_compressed,
                revision_name,
                inner_file,
                wad_name,
            )

        await self.db.commit()

        for inner_file in await self.db.get_all_wad_files_from_wad_name_and_revision(wad_name, old_revision):
            if inner_file not in journal_crcs.keys():
                deleted_inner_files.append(
                    #  0 crc, 1 size_, 2 name, 3 file_offset, 4 compressed_size, 5 is_compressed
                    WadInnerFileInfo(
                        name=inner_file[2],
                        wad_name=wad_name,
                        size=0,
                        crc=0,
                        compressed_size=inner_file[4],
                        is_compressed=inner_file[5],
                        file_offset=inner_file[3],
                        old_size=inner_file[1],
                        old_crc=inner_file[0],
                    )
                )

        return deleted_inner_files, created_inner_files, changed_inner_files

    async def get_wad_inner_file_data(self, wad_file_delta: FileDelta, inner_file: WadInnerFileInfo):
        if inner_file.is_compressed:
            data_range = (inner_file.file_offset, inner_file.file_offset + inner_file.compressed_size)
        else:
            data_range = (inner_file.file_offset, inner_file.file_offset + inner_file.size)

        data = await self.webdriver.get_url_data(wad_file_delta.url, data_range=data_range)

        if inner_file.is_compressed:
            return zlib.decompress(data)
        return data

    async def notify_revision_update(self, revision: str):
        pass

    async def notify_all_file_update(self, delta: FileDelta):
        pass

    async def notify_non_wad_file_update(self, delta: FileDelta):
        pass

    async def notify_wad_file_update(self, delta: FileDelta):
        pass
