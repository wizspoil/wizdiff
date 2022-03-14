import asyncio
import struct
import gzip
from typing import Tuple

import aiohttp
from loguru import logger

JOURNAL_ENTRY = "<lll?ll"
JOURNAL_ENTRY_SIZE = struct.calcsize(JOURNAL_ENTRY)


class WebDriver:
    def __init__(self):
        self.session = aiohttp.ClientSession()

    @staticmethod
    async def get_patch_urls() -> Tuple[str, str]:
        reader, writer = await asyncio.open_connection("patch.us.wizard101.com", 12500)

        writer.write(b"\x0D\xF0\x24\x00\x00\x00\x00\x00\x08\x01\x20" + bytes(29))
        await reader.read(4096)  # session offer or whatever

        data = await reader.read(4096)
        writer.close()

        def _read_url(start: int):
            str_len_data = data[start: start + 2]
            str_len = struct.unpack("<H", str_len_data)[0]

            str_data = data[start + 2: start + 2 + str_len]

            return str_data.decode()

        # -2 for the str len
        file_list_url_start = data.find(b"http") - 2
        base_url_start = data.rfind(b"http") - 2

        return _read_url(file_list_url_start), _read_url(base_url_start)

    async def get_url_data(self, url: str, *, data_range: Tuple[int, int] = None) -> bytes:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0"
        }

        if data_range:
            headers["Range"] = f"bytes={data_range[0]}-{data_range[1]}"

        logger.debug(f"Getting url data of {url} with data range {data_range}")

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 206:
                    logger.debug("Got partial content")

                return await response.content.read()

        # async with self.session.get(url, headers=headers) as res:
        #     res.raise_for_status()
        #     return await res.content.read()

    async def get_wad_journal_crcs(self, wad_url: str) -> dict:
        # .hdr.gz = header gzipped
        wad_header_data = await self.get_url_data(wad_url + ".hdr.gz")

        try:
            wad_header_data = gzip.decompress(wad_header_data)
        except gzip.BadGzipFile:
            pass

        signature, version, file_count = struct.unpack("<5sII", wad_header_data[:13])

        if signature != b"KIWAD":
            raise RuntimeError(f"Invalid signature {signature}")

        data_offset = 13

        if version >= 2:
            data_offset += 1

        res = {}

        for _ in range(file_count):
            journal_entry_data = wad_header_data[
                                 data_offset: data_offset + JOURNAL_ENTRY_SIZE
                                 ]
            offset, size, zsize, is_zip, crc, name_length = struct.unpack(
                JOURNAL_ENTRY, journal_entry_data
            )

            name_start = data_offset + JOURNAL_ENTRY_SIZE
            name_data = wad_header_data[name_start: name_start + name_length]
            name = name_data.decode()[:-1]

            data_offset += JOURNAL_ENTRY_SIZE + name_length

            res[name] = (offset, crc, size, zsize, is_zip)

        return res
