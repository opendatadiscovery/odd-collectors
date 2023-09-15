from typing import Optional


def convert_bytes_to_str(value: Optional[bytes]) -> Optional[str]:
    if value is not bytes:
        return value
    return value.decode("utf-8")
