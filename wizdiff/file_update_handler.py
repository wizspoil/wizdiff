import time
from datetime import datetime

from loguru import logger

from .utils import get_patch_urls, get_revision_from_url, get_url_data
from .db import (
    check_if_new_revision,
    add_revision_info,
    add_versioned_file_info,
    check_if_versioned_file_updated,
    get_latest_revision,
)
from .dml_parser import parse_records_from_bytes


class FileUpdateHandler:
    def __init__(self, *, sleep_time: float = 3_600):
        self.sleep_time = sleep_time

    def update_loop(self):
        while True:
            file_list_url, base_url = get_patch_urls()
            revision = get_revision_from_url(file_list_url)

            if check_if_new_revision(revision):
                self.new_revision(revision, file_list_url)

            else:
                logger.info(f"No new revision; sleeping for {self.sleep_time} seconds")

            time.sleep(self.sleep_time)

    def add_revision(self, name: str):
        logger.info(f"Adding revision {name}")
        add_revision_info(name, datetime.utcnow())

    def get_file_list_records(self, file_list_url: str):
        file_list_data = get_url_data(file_list_url)
        return parse_records_from_bytes(file_list_data)

    # TODO: dont forget to .add_revision
    def new_revision(self, name: str, file_list_url: str):
        logger.info(f"New revision found: {name}")
        old_revision = get_latest_revision()[0]
        self.check_if_versioned_files_changed(old_revision, file_list_url)

        revision = get_revision_from_url(file_list_url)
        self.add_revision(revision)

    # TODO: check for deleted files
    def check_if_versioned_files_changed(self, old_revision: str, file_list_url: str):
        records = self.get_file_list_records(file_list_url)

        for table_name, records in records.items():
            # meta tables
            if table_name in ["_TableList", "About"]:
                continue

            for record in records:
                name = record["SrcFileName"]
                res = check_if_versioned_file_updated(
                    record["CRC"],
                    record["Size"],
                    old_revision,
                    name
                )

                match res:
                    case True:
                        logger.info(f"File {name} updated")

                    case False:
                        pass

                    case None:
                        logger.info(f"New file {name}")

    def init_db(self):
        file_list_url, base_url = get_patch_urls()
        revision = get_revision_from_url(file_list_url)
        self.add_revision(revision)
        self.fill_db(file_list_url, "temp", revision)

    def fill_db(self, file_list_url: str, base_url: str, revision: str):
        file_list_records = self.get_file_list_records(file_list_url)

        for table_name, records in file_list_records.items():
            # meta tables
            if table_name in ["_TableList", "About"]:
                continue

            for record in records:
                logger.debug(f"Filling db with {record}")
                add_versioned_file_info(
                    record["CRC"], record["Size"], revision, record["SrcFileName"]
                )
