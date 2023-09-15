def remove_protocol(path: str) -> str:
    if path.startswith("s3://"):
        return path.removeprefix("s3://")
    elif path.startswith(("s3a://", "s3n://")):
        return path[6:]
    else:
        return path
