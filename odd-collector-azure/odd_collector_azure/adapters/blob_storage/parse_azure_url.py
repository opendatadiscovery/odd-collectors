from typing import Tuple
from urllib.parse import urlparse


def parse_azure_url(url: str) -> Tuple[str, str]:
    parsed_url = urlparse(url)
    container = parsed_url.netloc
    key = parsed_url.path.lstrip("/")

    return container, key
