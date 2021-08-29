from dataclasses import dataclass
from typing import List, Optional


class Delta:
    """
    Base delta class
    """

    pass


@dataclass
class FileDelta(Delta):
    """
    Delta for file changes
    """

    name: str
    revision: str
    url: str


@dataclass
class DeletedFileDelta(FileDelta):
    """
    Delta of a deleted file
    """

    old_crc: int
    old_size: int


@dataclass
class CreatedFileDelta(FileDelta):
    """
    Delta of a created file
    """

    new_crc: int
    new_size: int
    old_crc: int
    old_size: int


@dataclass
class ChangedFileDelta(FileDelta):
    """
    Delta of a changed file
    """

    new_crc: int
    new_size: int
    old_crc: int
    old_size: int


@dataclass
class WadInnerFileInfo:
    name: str
    wad_name: str
    size: int
    crc: int
    compressed_size: int
    is_compressed: bool
    file_offset: int
    old_size: Optional[int]
    old_crc: Optional[int]


@dataclass
class DeletedWadFileDelta(DeletedFileDelta):
    """
    Delta of a deleted wad file
    """

    deleted_inner_files: List[WadInnerFileInfo]


@dataclass
class CreatedWadFileDelta(CreatedFileDelta):
    """
    Delta of a created wad file
    """

    created_inner_files: List[WadInnerFileInfo]


@dataclass
class ChangedWadfileDelta(ChangedFileDelta):
    """
    Delta of a changed wad file
    """

    deleted_inner_files: List[WadInnerFileInfo]
    created_inner_files: List[WadInnerFileInfo]
    changed_inner_files: List[WadInnerFileInfo]
