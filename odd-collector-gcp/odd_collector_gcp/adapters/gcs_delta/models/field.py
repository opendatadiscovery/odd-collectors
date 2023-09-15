from typing import Union

from deltalake import Field
from deltalake._internal import PrimitiveType, ArrayType, MapType, StructType


class DField:
    def __init__(self, field: Field):
        self.field = field

    @property
    def odd_metadata(self) -> dict:
        return self.field.metadata

    @property
    def name(self) -> str:
        return self.field.name

    @property
    def type(self) -> Union[PrimitiveType, ArrayType, MapType, StructType]:
        return self.field.type

    @property
    def nullable(self) -> bool:
        return self.nullable
