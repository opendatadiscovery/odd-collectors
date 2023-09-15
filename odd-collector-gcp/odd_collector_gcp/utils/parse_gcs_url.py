from urllib.parse import urlparse


def parse_gcs_url(url: str) -> tuple[str, str]:
    parsed_url = urlparse(url)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")

    return bucket, key
