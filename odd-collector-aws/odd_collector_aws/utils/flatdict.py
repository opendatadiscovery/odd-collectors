from flatdict import FlatDict


def flatdict(data, delimiter: str = "."):
    return FlatDict(data, delimiter)
