from typing import List, Optional, TypeVar

T = TypeVar("T")


def safe_list_get(xs: List[T], idx: int) -> Optional[T]:
    """
    Safe getting element from list by index, else None
    """
    try:
        return xs[idx]
    except IndexError:
        return None
