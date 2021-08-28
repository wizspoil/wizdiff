import sqlite3
from typing import List, Optional
from functools import cached_property
from datetime import datetime
from enum import Enum


TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS RevisionInfo (
    revision_name TEXT,
    date_ DATE,
    PRIMARY KEY (revision_name)
);

CREATE TABLE IF NOT EXISTS VersionedFileInfo (
    crc INTEGER,
    size_ INTEGER,
    revision TEXT,
    name TEXT,
    PRIMARY KEY (revision, name)
);

CREATE TABLE IF NOT EXISTS WadFileInfo (
    crc INTEGER,
    size_ INTEGER,
    revision TEXT,
    revision_name TEXT,
    wad_name TEXT,
    PRIMARY KEY (revision, revision_name, wad_name)
);
""".strip()


class FileUpdateType(Enum):
    changed = 1
    unchanged = 2
    new = 3


class WizDiffDatabase:
    def __init__(self, sqlite_connection_name: str = "wizdiff.db"):
        self.sqlite_connection_name = sqlite_connection_name

    @cached_property
    def _connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.sqlite_connection_name)

    def init_database(self):
        self._connection.executescript(TABLE_SCRIPT)
        self._connection.commit()

    def add_revision_info(self, name: str, date: datetime = None):
        if date is None:
            date = datetime.utcnow()

        self._connection.execute("INSERT INTO RevisionInfo (revision_name, date_) VALUES (?, ?);", (name, date))
        self._connection.commit()

    def get_latest_revision(self) -> Optional[str]:
        cur = self._connection.execute("SELECT revision_name FROM RevisionInfo ORDER BY date_ DESC;")
        return cur.fetchone()

    def check_if_new_revision(self, name: str):
        cur = self._connection.execute("SELECT * FROM RevisionInfo WHERE revision_name = ?;", (name,))
        return cur.fetchone() is None

    def add_versioned_file_info(self, crc: int, size: int, revision: str, name: str):
        if crc < 0:
            raise ValueError("CRC cannot be negative")

        if size < 0:
            raise ValueError("Size cannot be negative")

        if not name:
            raise ValueError("Name cannot be empty")

        self._connection.execute(
            "INSERT INTO VersionedFileInfo (crc, size_, revision, name) VALUES (?, ?, ?, ?);",
            (crc, size, revision, name),
        )
        self._connection.commit()

    def check_if_versioned_file_updated(self, new_crc: int, new_size: int, old_revision: str, name: str):
        cur = self._connection.execute(
            "SELECT crc, size_ FROM VersionedFileInfo WHERE revision is (?) and revision_name is (?);",
            (old_revision, name),
        )
        old_ver = cur.fetchone()

        # new file
        if old_ver is None:
            return FileUpdateType.new

        old_crc, old_size = old_ver

        if old_crc != new_crc or old_size != new_size:
            return FileUpdateType.changed

        else:
            return FileUpdateType.unchanged

    def get_all_versioned_files_from_revision(self, revision: str) -> List[tuple]:
        cur = self._connection.execute("SELECT * FROM VersionedFileInfo WHERE revision is (?);", (revision,))
        return cur.fetchall()
