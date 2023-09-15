from typing import Tuple
from urllib.parse import urlparse


def parse_s3_url(url: str) -> Tuple[str, str]:
    parsed_url = urlparse(url)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")

    return bucket, key
