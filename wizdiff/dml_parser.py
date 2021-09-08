import struct
from io import SEEK_END
from functools import cached_property
from collections import defaultdict
from pathlib import Path
from typing import Union
from xml.etree import ElementTree


type_format_dict = {
    "char": "<c",
    "signed char": "<b",
    "unsigned char": "<B",
    "bool": "?",
    "short": "<h",
    "unsigned short": "<H",
    "int": "<i",
    "unsigned int": "<I",
    "long": "<l",
    "unsigned long": "<L",
    "long long": "<q",
    "unsigned long long": "<Q",
    "float": "<f",
    "double": "<d",
}


wiz_type_conversion = {
    0: "long long",
    1: "int",
    2: "unsigned int",
    3: "float",
    4: "signed char",
    5: "unsigned char",
    6: "unsigned short",
    7: "double",
    8: "string",
    9: "wstring",
    10: "short",
}


IGNORED = frozenset(("_TableList", "About"))


class TypedReader:
    def read_data(self, size: int) -> bytes:
        raise NotImplementedError()

    def read_typed(self, type_name: str):
        format_str = type_format_dict[type_name]
        size = struct.calcsize(format_str)
        data = self.read_data(size)
        return struct.unpack(format_str, data)[0]

    def read_str(self, encoding: str = "utf-8"):
        size = self.read_typed("unsigned short")
        str_data = self.read_data(size)
        return str_data.decode(encoding)


class TypedFileReader(TypedReader):
    def __init__(self, fp):
        self.fp = fp

    @cached_property
    def size(self):
        current = self.fp.tell()
        self.fp.seek(0, SEEK_END)
        size = self.fp.tell()
        self.fp.seek(current)
        return size

    @property
    def is_eof(self):
        return self.fp.tell() == self.size

    def read_data(self, size: int) -> bytes:
        return self.fp.read(size)


class TypedBytesReader(TypedReader):
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    @cached_property
    def size(self):
        return len(self.data)

    @property
    def is_eof(self):
        return self.offset == self.size

    def read_data(self, size: int) -> bytes:
        start = self.offset
        end = self.offset + size

        if end > self.size:
            raise RuntimeError("Read more data than in buffer")

        self.offset = end

        return self.data[start:end]


def parse_record_template(data: TypedBytesReader):
    record_template = {}

    while not data.is_eof:
        name = data.read_str()

        if name == "_TargetTable":
            data.offset += 2
            record_template_name = data.read_str()
            return record_template_name, record_template

        type_index = data.read_typed("unsigned char")
        forty_check = data.read_typed("unsigned char")

        if forty_check != 40:
            raise RuntimeError(f"Forty check was {forty_check} not forty")

        record_template[name] = type_index

    raise RuntimeError("No target table")


def parse_record(data: TypedBytesReader, record_template):
    record = {}
    for name, type_index in record_template.items():
        # for some reason I need to subtract 1?
        format_type = wiz_type_conversion[type_index - 1]

        # TODO: 3.10 switch to match
        if format_type == "wstring":
            value = data.read_str("utf-16-le")

        elif format_type == "string":
            value = data.read_str()

        else:
            value = data.read_typed(format_type)

        record[name] = value

    return record


def consume_data(reader: Union[TypedBytesReader, TypedFileReader]):
    records = defaultdict(list)
    current_record_template = None
    current_record_name = None

    while not reader.is_eof:
        record_number = reader.read_typed("unsigned int")

        # +1 to include the template
        for _ in range(record_number + 1):
            structure_marker = reader.read_typed("unsigned char")

            if structure_marker != 2:
                raise RuntimeError(
                    f"Got a structure marker of {structure_marker} instead of 2"
                )

            structure_type = reader.read_typed("unsigned char")

            included_data_size = reader.read_typed("unsigned short")
            # included size includes the record header
            data_size = included_data_size - 4

            data = reader.read_data(data_size)
            typed_data = TypedBytesReader(data)

            # TODO: 3.10 switch to match
            if structure_type == 1:
                current_record_name, current_record_template = parse_record_template(
                    typed_data
                )

            elif structure_type == 2:
                if not current_record_template:
                    raise RuntimeError("No record template to use for record")

                records[current_record_name].append(
                    parse_record(typed_data, current_record_template)
                )

            else:
                raise RuntimeError(f"Unknown structure type {structure_type}")

    return records


def parse_records_from_file(to_parser: Union[str, Path]):
    to_parser = Path(to_parser)

    # TODO: does SEEK_END work with this?
    typed_reader = TypedFileReader(to_parser.open("rb"))
    records = consume_data(typed_reader)

    return records


def parse_records_from_bytes(data: bytes):
    reader = TypedBytesReader(data)
    return consume_data(reader)


def parse_records_from_xml(data: bytes):
    root = ElementTree.fromstring(data.decode())

    records = []

    for child in root:
        if child.tag not in IGNORED:
            # can be multiple records
            for subchild in child:
                record = {}
                for subsubchild in subchild:
                    if subsubchild.text and subsubchild.text.isnumeric():
                        record[subsubchild.tag] = int(subsubchild.text)

                    else:
                        record[subsubchild.tag] = subsubchild.text

                records.append(record)

    return {"records": records}
