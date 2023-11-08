from odd_models import Type

from ..models import Column
from .types import TYPES_SQL_TO_ODD


def has_vector_column(columns: list[Column]) -> bool:
    """
    The function checks if there is at least one column of vector data type.
    """
    for c in columns:
        if TYPES_SQL_TO_ODD.get(c.data_type) == Type.TYPE_VECTOR:
            return True
    return False
