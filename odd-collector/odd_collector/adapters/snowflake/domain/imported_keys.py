from typing import Any, Dict

from funcy import omit
from pydantic import BaseModel


class ImportedKey(BaseModel):
    created_on: Any
    pk_database_name: str
    pk_schema_name: str
    pk_table_name: str
    pk_columns: tuple[str, ...]
    fk_database_name: str
    fk_schema_name: str
    fk_table_name: str
    fk_columns: tuple[str, ...]
    # key_sequence: int
    # update_rule: Any
    # delete_rule: Any
    fk_name: str
    pk_name: str
    # deferrability: Any
    # rely: bool
    # comment: Any

    # @property
    # def metadata(self) -> Dict[str, Any]:
    #     excluded = [
    #         "table_catalog",
    #         "table_schema",
    #         "table_name",
    #         "column_name",
    #         "column_default",
    #         "is_nullable",
    #         "data_type",
    #     ]
    #     return omit(self.__dict__, excluded)
