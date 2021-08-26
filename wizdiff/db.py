import sqlite3
from typing import Optional

TABLE_SCRIPT = """
CREATE TABLE IF NOT EXISTS RevisionInfo (
    name TEXT,
    date_ DATE,
    PRIMARY KEY (name, date_)
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
    name TEXT,
    wad_name TEXT,
    PRIMARY KEY (revision, name, wad_name)
);
""".strip()


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect("wizdiff.db")


def init_db():
    with get_connection() as conn:
        conn.executescript(TABLE_SCRIPT)
        conn.commit()


def add_revision_info(name: str, date):
    with get_connection() as conn:
        conn.execute("INSERT INTO RevisionInfo (name, date_) VALUES (?, ?);", (name, date))
        conn.commit()


def get_latest_revision() -> Optional[tuple[str, "datetime"]]:
    with get_connection() as conn:
        cur = conn.execute("SELECT * FROM RevisionInfo ORDER BY date_ DESC;")
        return cur.fetchone()


def check_if_new_revision(name: str):
    with get_connection() as conn:
        cur = conn.execute("SELECT * FROM RevisionInfo WHERE name = ?;", (name,))
        res = cur.fetchone()

    return res is None


def add_versioned_file_info(crc: int, size: int, revision: str, name: str):
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO VersionedFileInfo (crc, size_, revision, name) VALUES (?, ?, ?, ?);",
            (crc, size, revision, name),
        )
        conn.commit()


def check_if_versioned_file_updated(new_crc: int, new_size: int, old_revision: str, name: str):
    """
    True -> changed file
    False -> unchanged file
    None -> new file
    """
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT crc, size_ FROM VersionedFileInfo WHERE revision is (?) and name is (?);",
            (old_revision, name),
        )
        old_ver = cur.fetchone()

    # new file
    if old_ver is None:
        return None

    old_crc, old_size = old_ver

    return old_crc != new_crc or old_size != new_size


if __name__ == "__main__":
    init_db()
    from datetime import datetime
    add_revision_info("latest2", datetime.utcnow())
    # print(get_revision_info("latest2"))
    print(get_latest_revision())

    # add_versioned_file_info(123123, 100, "latest", "Root")
    # check_if_versioned_file_updated(123123, 100, "latest2", "Root")
