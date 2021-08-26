import struct
import re
from socket import create_connection


REVERSION_URL_REGEX = re.compile(r"WizPatcher/([^/]+)")


def get_latest_file_list_url():
    with create_connection(("patch.us.wizard101.com", 12500)) as socket:
        socket.send(b"\x0D\xF0\x24\x00\x00\x00\x00\x00\x08\x01\x20" + bytes(29))
        socket.recv(4096)  # session offer or whatever
        data = socket.recv(4096)

    # -2 for the str len
    url_str_start = data.find(b"http") - 2
    str_len_data = data[url_str_start:url_str_start + 2]
    str_len = struct.unpack("<H", str_len_data)[0]

    str_data = data[url_str_start + 2: url_str_start + 2 + str_len]

    return str_data.decode("utf-8")


def get_reversion_from_url(url: str):
    res = REVERSION_URL_REGEX.search(url)

    if res is None:
        raise ValueError(f"Reversion string not found in {url}")

    return res.group(1)


if __name__ == "__main__":
    print(get_latest_file_list_url())
