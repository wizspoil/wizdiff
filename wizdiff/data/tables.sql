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
