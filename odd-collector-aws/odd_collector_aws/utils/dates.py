import datetime


def from_ms(ms) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)


def add_utc_timezone(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(tzinfo=datetime.timezone.utc)
