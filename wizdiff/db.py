import sqlite3
from sqlite3 import Row
from typing import List, Optional, Iterable
from functools import cached_property
from datetime import datetime
from enum import Enum

import aiosqlite


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
        self._db_connection = None

    async def _connection(self) -> aiosqlite.Connection:
        if self._db_connection is None:
            self._db_connection = await aiosqlite.connect(self.sqlite_connection_name)

        return self._db_connection

    async def init_database(self):
        connection = await self._connection()
        await connection.executescript(TABLE_SCRIPT)
        await self.commit()

    async def add_revision_info(self, name: str, date: datetime = None):
        if date is None:
            date = datetime.utcnow()

        connection = await self._connection()
        await connection.execute(
            "INSERT INTO RevisionInfo (revision_name, date_) VALUES (?, ?);",
            (name, date),
        )
        await self.commit()

    async def delete_revision_info(self, name: str):
        connection = await self._connection()
        await connection.execute(
            "DELETE FROM RevisionInfo WHERE revision_name is (?);", (name,)
        )
        await self.commit()

    async def get_latest_revision(self) -> Optional[str]:
        connection = await self._connection()
        cursor = await connection.execute(
            "SELECT revision_name FROM RevisionInfo ORDER BY date_ DESC;"
        )
        res = await cursor.fetchone()

        if res:
            return res[0]

        return None

    async def check_if_new_revision(self, name: str):
        connection = await self._connection()
        cursor = await connection.execute(
            "SELECT * FROM RevisionInfo WHERE revision_name = ?;", (name,)
        )
        return await cursor.fetchone() is None

    async def add_versioned_file_info(self, crc: int, size: int, revision: str, name: str):
        if size < 0:
            raise ValueError("Size cannot be negative")

        if not name:
            raise ValueError("Name cannot be empty")

        connection = await self._connection()
        await connection.execute(
            "INSERT INTO VersionedFileInfo (crc, size_, revision, name) VALUES (?, ?, ?, ?);",
            (crc, size, revision, name),
        )

    async def delete_versioned_file_infos_with_revision(self, revision_name: str):
        connection = await self._connection()
        await connection.execute(
            "DELETE FROM VersionedFileInfo WHERE revision is (?);", (revision_name,)
        )
        await self.commit()

    async def check_if_versioned_file_updated(
        self, new_crc: int, new_size: int, old_revision: str, name: str
    ):
        connection = await self._connection()
        cursor = await connection.execute(
            "SELECT crc, size_ FROM VersionedFileInfo WHERE revision is (?) and name is (?);",
            (old_revision, name),
        )
        old_ver = await cursor.fetchone()

        if old_ver is None:
            return FileUpdateType.new, (None, None)

        old_crc, old_size = old_ver

        # logger.debug(f"Checking if {name} updated {old_crc=} {new_crc=} {old_size=} {new_size=}")

        if old_crc != new_crc or old_size != new_size:
            return FileUpdateType.changed, (old_crc, old_size)

        else:
            return FileUpdateType.unchanged, (old_crc, old_size)

    async def get_all_versioned_files_from_revision(self, revision: str) -> Iterable[Row]:
        connection = await self._connection()
        cursor = await connection.execute(
            "SELECT * FROM VersionedFileInfo WHERE revision is (?);", (revision,)
        )
        return await cursor.fetchall()

    async def add_wad_file_info(
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

        connection = await self._connection()
        await connection.execute(
            "INSERT INTO WadFileInfo "
            "(file_offset, crc, size_, compressed_size, is_compressed, revision, name, wad_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (file_offset, crc, size, compressed_size, is_compressed, revision, file_name, wad_name),
        )

    async def update_wad_file_infos_revision_with_wad_name(self, wad_name: str, new_revision: str):
        connection = await self._connection()
        await connection.execute(
            "UPDATE WadFileInfo SET revision = (?) WHERE wad_name is (?);",
            (new_revision, wad_name)
        )

    async def mass_update_wad_file_infos_revision_with_wad_names(self, wad_names: List[str], new_revision: str):
        # sqlite module doesn't have the best support for the `in` condition
        value_placers = ",".join("?" * len(wad_names))

        connection = await self._connection()
        await connection.execute(
            f"UPDATE WadFileInfo SET revision = (?) WHERE wad_name in ({value_placers})",
            (new_revision, *wad_names)
        )

        await self.commit()

    async def delete_wad_file_infos_with_revision(self, revision_name: str):
        connection = await self._connection()
        await connection.execute(
            "DELETE FROM WadFileInfo WHERE revision is (?);", (revision_name,)
        )
        await self.commit()

    # TODO: maybe merge this and check_if_versioned_file_updated together (past first line is duplicated)
    async def check_if_wad_file_updated(
        self,
        new_crc: int,
        new_size: int,
        old_revision: str,
        file_name: str,
        wad_name: str,
    ):
        connection = await self._connection()
        cursor = await connection.execute(
            "SELECT crc, size_ FROM WadFileInfo WHERE revision is (?) and name is (?) and wad_name is (?);",
            (old_revision, file_name, wad_name),
        )
        old_ver = await cursor.fetchone()

        if old_ver is None:
            return FileUpdateType.new, (None, None)

        old_crc, old_size = old_ver

        if old_crc != new_crc or old_size != new_size:
            return FileUpdateType.changed, (old_crc, old_size)

        else:
            return FileUpdateType.unchanged, (old_crc, old_size)

    async def get_all_wad_files_from_wad_name_and_revision(
        self, wad_name: str, revision: str
    ):
        connection = await self._connection()
        cursor = await connection.execute(
            "SELECT crc, size_, name, file_offset, compressed_size, is_compressed "
            "FROM WadFileInfo WHERE revision is (?) and name is (?);",
            (revision, wad_name),
        )
        return await cursor.fetchall()

    async def commit(self):
        connection = await self._connection()
        await connection.commit()

    async def vacuum(self):
        connection = await self._connection()
        await connection.execute("VACUUM;")
