import pathlib


def get_file_extension(filename: str):
    """
    Get file extension from filename or path.
    Example:
        get_file_extension("file.csv") -> ".csv"
        get_file_extension("/path/to/file.csv.gz") -> ".csv.gz"
    """
    return "".join(pathlib.Path(filename).suffixes)
