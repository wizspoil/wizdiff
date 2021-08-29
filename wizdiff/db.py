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
    file_offset INTEGER,
    crc INTEGER,
    size_ INTEGER,
    compressed_size INTEGER,
    is_compressed BOOLEAN,
    revision TEXT,
    name TEXT,
    wad_name TEXT,
    PRIMARY KEY (revision, name, wad_name)
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
        self.commit()

    def add_revision_info(self, name: str, date: datetime = None):
        if date is None:
            date = datetime.utcnow()

        self._connection.execute(
            "INSERT INTO RevisionInfo (revision_name, date_) VALUES (?, ?);",
            (name, date),
        )
        self.commit()

    def delete_revision_info(self, name: str):
        self._connection.execute(
            "DELETE FROM RevisionInfo WHERE revision_name is (?);", (name,)
        )
        self.commit()

    def get_latest_revision(self) -> Optional[str]:
        cur = self._connection.execute(
            "SELECT revision_name FROM RevisionInfo ORDER BY date_ DESC;"
        )
        res = cur.fetchone()

        if res:
            return res[0]

        return None

    def check_if_new_revision(self, name: str):
        cur = self._connection.execute(
            "SELECT * FROM RevisionInfo WHERE revision_name = ?;", (name,)
        )
        return cur.fetchone() is None

    def add_versioned_file_info(self, crc: int, size: int, revision: str, name: str):
        if size < 0:
            raise ValueError("Size cannot be negative")

        if not name:
            raise ValueError("Name cannot be empty")

        self._connection.execute(
            "INSERT INTO VersionedFileInfo (crc, size_, revision, name) VALUES (?, ?, ?, ?);",
            (crc, size, revision, name),
        )

    def delete_versioned_file_infos_with_revision(self, revision_name: str):
        self._connection.execute(
            "DELETE FROM VersionedFileInfo WHERE revision is (?);", (revision_name,)
        )
        self.commit()

    def check_if_versioned_file_updated(
        self, new_crc: int, new_size: int, old_revision: str, name: str
    ):
        cur = self._connection.execute(
            "SELECT crc, size_ FROM VersionedFileInfo WHERE revision is (?) and name is (?);",
            (old_revision, name),
        )
        old_ver = cur.fetchone()

        if old_ver is None:
            return FileUpdateType.new, (None, None)

        old_crc, old_size = old_ver

        if old_crc != new_crc or old_size != new_size:
            return FileUpdateType.changed, (old_crc, old_size)

        else:
            return FileUpdateType.unchanged, (old_crc, old_size)

    def get_all_versioned_files_from_revision(self, revision: str) -> List[tuple]:
        cur = self._connection.execute(
            "SELECT * FROM VersionedFileInfo WHERE revision is (?);", (revision,)
        )
        return cur.fetchall()

    def add_wad_file_info(
        self,
        file_offset: int,
        crc: int,
        size: int,
        compressed_size: int,
        is_compressed: bool,
        revision: str,
        file_name: str,
        wad_name: str,
    ):
        if size < 0:
            raise ValueError("Size cannot be negative")

        if not file_name:
            raise ValueError("Name cannot be empty")

        if not wad_name:
            raise ValueError(f"Wad name cannot be empty")

        self._connection.execute(
            "INSERT INTO WadFileInfo "
            "(file_offset, crc, size_, compressed_size, is_compressed, revision, name, wad_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (file_offset, crc, size, compressed_size, is_compressed, revision, file_name, wad_name),
        )

    def update_wad_file_infos_revision_with_wad_name(self, wad_name: str, new_revision: str):
        self._connection.execute(
            "UPDATE WadFileInfo SET revision = (?) WHERE wad_name is (?);",
            (new_revision, wad_name)
        )

    def delete_wad_file_infos_with_revision(self, revision_name: str):
        self._connection.execute(
            "DELETE FROM WadFileInfo WHERE revision is (?);", (revision_name,)
        )
        self.commit()

    # TODO: maybe merge this and check_if_versioned_file_updated together (past first line is duplicated)
    def check_if_wad_file_updated(
        self,
        new_crc: int,
        new_size: int,
        old_revision: str,
        file_name: str,
        wad_name: str,
    ):
        cur = self._connection.execute(
            "SELECT crc, size_ FROM WadFileInfo WHERE revision is (?) and name is (?) and wad_name is (?);",
            (old_revision, file_name, wad_name),
        )
        old_ver = cur.fetchone()

        if old_ver is None:
            return FileUpdateType.new, (None, None)

        old_crc, old_size = old_ver

        if old_crc != new_crc or old_size != new_size:
            return FileUpdateType.changed, (old_crc, old_size)

        else:
            return FileUpdateType.unchanged, (old_crc, old_size)

    def get_all_wad_files_from_wad_name_and_revision(
        self, wad_name: str, revision: str
    ):
        cur = self._connection.execute(
            "SELECT crc, size_, name, file_offset, compressed_size, is_compressed "
            "FROM WadFileInfo WHERE revision is (?) and name is (?);",
            (revision, wad_name),
        )
        return cur.fetchall()

    def commit(self):
        self._connection.commit()

    def vacuum(self):
        self._connection.execute("VACUUM;")
