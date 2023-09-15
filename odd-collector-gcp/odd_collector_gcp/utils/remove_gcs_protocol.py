def remove_protocol(path: str) -> str:
    if path.startswith("gcs://"):
        return path.removeprefix("gcs://")
    elif path.startswith("gs://"):
        return path.removeprefix("gs://")
    else:
        return path
