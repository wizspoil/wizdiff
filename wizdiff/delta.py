from dataclasses import dataclass


class Delta:
    """
    Base delta class
    """
    pass


@dataclass()
class FileDelta(Delta):
    """
    Delta for file changes
    """
    name: str
    revision: str
    url: str


@dataclass()
class DeletedFileDelta(FileDelta):
    """
    Delta of a deleted file
    """
    old_crc: int
    old_size: int


@dataclass()
class CreatedFileDelta(FileDelta):
    """
    Delta of a created file
    """
    new_crc: int
    new_size: int
    old_crc: int
    old_size: int


@dataclass()
class ChangedFileDelta(FileDelta):
    """
    Delta of a changed file
    """
    new_crc: int
    new_size: int
    old_crc: int
    old_size: int
