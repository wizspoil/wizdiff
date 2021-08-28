"""
    db =
    db.init_database()
    db.add_revision_info("test_revision")
    print(f"{db.get_latest_revision()=}")
    db.add_versioned_file_info(10, 10, "test_revision", "Root.wad")
    print(f"{db.check_if_versioned_file_updated(11, 10, 'test_revision', 'Root.wad')=}")
    print(f"{db.check_if_versioned_file_updated(10, 10, 'test_revision', 'Root.wad')=}")
    print(f"{db.check_if_versioned_file_updated(11, 10, 'test_revision', 'new_file.wad')=}")
"""

from sqlite3 import IntegrityError

from hypothesis import given, settings, Phase
from hypothesis.strategies import text, integers

from wizdiff.db import WizDiffDatabase, FileUpdateType


@given(text())
def test_revision_info(revision_name: str):
    db = WizDiffDatabase(":memory:")
    db.init_database()

    db.add_revision_info(revision_name)

    latest_revision = db.get_latest_revision()
    assert latest_revision[0] == revision_name


@given(
    integers(max_value=1_000_000, min_value=-1_000_000),
    integers(max_value=1_000_000, min_value=-1_000_000),
    text(),
    text(),
)
def test_versioned_file_update(crc, size, revision, name):
    db = WizDiffDatabase(":memory:")
    db.init_database()

    should_throw = False

    if crc < 0 or size < 0 or not name:
        should_throw = True

    assert db.check_if_versioned_file_updated(crc, size, revision, name) is FileUpdateType.new

    try:
        db.add_versioned_file_info(crc, size, revision, name)
    except ValueError:
        assert should_throw is True
        return

    assert db.check_if_versioned_file_updated(crc, size, revision, name) is FileUpdateType.unchanged
    assert db.check_if_versioned_file_updated(crc + 1, size, revision, name) is FileUpdateType.changed
    assert db.check_if_versioned_file_updated(crc, size + 1, revision, name) is FileUpdateType.changed
    assert db.check_if_versioned_file_updated(crc, size, revision, name + "a") is FileUpdateType.new


