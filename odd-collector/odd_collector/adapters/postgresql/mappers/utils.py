from odd_models import Type

from ..models import Table
from .types import TYPES_SQL_TO_ODD


def data_entity_has_vector_column(data_entity: Table) -> bool:
    """
    The function checks if data_entity (in our case table/view) has at least one column
    of vector data type.
    """
    for c in data_entity.columns:
        if TYPES_SQL_TO_ODD.get(c.data_type) == Type.TYPE_VECTOR:
            return True
    return False
