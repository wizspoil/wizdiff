import re


REVERSION_URL_REGEX = re.compile(r"WizPatcher/([^/]+)")


def get_revision_from_url(url: str):
    res = REVERSION_URL_REGEX.search(url)

    if res is None:
        raise ValueError(f"Reversion string not found in {url}")

    return res.group(1)
