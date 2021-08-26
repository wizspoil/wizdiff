import time
from datetime import datetime

from .utils import get_patch_urls, get_revision_from_url
from .db import check_if_new_revision, add_revision_info


class FileUpdateHandler:
    def __init__(self, *, sleep_time: float = 3_600):
        self.sleep_time = sleep_time

    def update_loop(self):
        while True:
            file_list_url, base_url = get_patch_urls()
            revision = get_revision_from_url(file_list_url)

            if check_if_new_revision(revision):
                self.new_revision(revision, file_list_url)

            time.sleep(self.sleep_time)

    def new_revision(self, name: str, file_list: str):
        print(f"new revision {name}")
        add_revision_info(name, datetime.utcnow())

    def fill_db(self, file_list_url, base_url: str):
        pass
