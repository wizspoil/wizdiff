import json
from pathlib import Path
from typing import Union, Optional

from printrospector import TypeCache, BinarySerializer
from printrospector.binary import FLAG_STATEFUL_FLAGS
from printrospector.object import DynamicObject


class Deserializer:
    def __init__(self, type_definitions_path: Union[str, Path]):
        with open(type_definitions_path) as fp:
            self.type_cache = TypeCache(json.load(fp))

        self.serializer = BinarySerializer(self.type_cache, FLAG_STATEFUL_FLAGS, True)

    def deserialize(self, data: bytes) -> Optional[DynamicObject]:
        if data[:4] != b"BINd":
            raise ValueError("Invalid magic bytes")

        return self.serializer.deserialize(data[4:])
