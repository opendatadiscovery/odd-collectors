from pathlib import Path
from typing import Any

from lark import Lark, Token, Tree
from odd_models import DataSetField, DataSetFieldType, Type
from oddrn_generator import Generator

from ..logger import logger
from ..utils.metadata import DefinitionType, extract_metadata
from .exceptions import NonTypeObjectError, StructureError, UnexpectedTypeError
from .field_types import (
    ArrayType,
    BasicType,
    Field,
    MapType,
    ParseType,
    StructType,
    UnionType,
)


class DatasetFieldBuilder:
    def __init__(
        self,
        data_source: str,
        oddrn_generator: Generator,
        parser_config_path: Path,
        odd_types_map: dict,
        parser_type: str = "lalr",
    ):
        self.data_source = data_source
        self.odd_types_map = odd_types_map
        self.oddrn_generator = oddrn_generator
        self.parser = Lark.open(
            str(parser_config_path), rel_to=__file__, parser=parser_type, start="type"
        )

    def build_dataset_field(self, field: Any) -> list[DataSetField]:
        data_source = self.data_source
        oddrn_generator = self.oddrn_generator
        parser_input = str(field.type)
        logger.debug(f"Build dataset field for {field.name} with type {parser_input}")
        type_tree = self.parser.parse(parser_input)
        field_type = self.traverse_tree(type_tree)
        generated_dataset_fields = []

        def _build_ds_field_from_type(
            field_name: str, field_type: ParseType, parent_oddrn=None
        ):
            if parent_oddrn is None:
                oddrn = oddrn_generator.get_oddrn_by_path("columns", field_name)
            else:
                oddrn = f"{parent_oddrn}/keys/{field_name}"

            if isinstance(field_type, StructType):
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=field_name,
                        metadata=[
                            extract_metadata(
                                data_source, field, DefinitionType.DATASET_FIELD
                            )
                        ],
                        type=DataSetFieldType(
                            type=Type.TYPE_STRUCT,
                            logical_type=field_type.to_logical_type(),
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    )
                )
                for field_name, _type in field_type.fields.items():
                    _build_ds_field_from_type(field_name, _type, oddrn)
            elif isinstance(field_type, ArrayType):
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=field_name,
                        metadata=[
                            extract_metadata(
                                data_source, field, DefinitionType.DATASET_FIELD
                            )
                        ],
                        type=DataSetFieldType(
                            type=Type.TYPE_LIST,
                            logical_type=field_type.to_logical_type(),
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    )
                )
                _build_ds_field_from_type("Element", field_type.element_type, oddrn)
            elif isinstance(field_type, MapType):
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=field_name,
                        metadata=[
                            extract_metadata(
                                data_source, field, DefinitionType.DATASET_FIELD
                            )
                        ],
                        type=DataSetFieldType(
                            type=Type.TYPE_MAP,
                            logical_type=field_type.to_logical_type(),
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    )
                )
                _build_ds_field_from_type("Key", field_type.key_type, oddrn)
                _build_ds_field_from_type("Value", field_type.value_type, oddrn)
            elif isinstance(field_type, UnionType):
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=field_name,
                        metadata=[
                            extract_metadata(
                                data_source, field, DefinitionType.DATASET_FIELD
                            )
                        ],
                        type=DataSetFieldType(
                            type=Type.TYPE_UNION,
                            logical_type=field_type.to_logical_type(),
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    )
                )

            else:
                odd_type = self.get_odd_type(field_type)
                logical_type = field_type.to_logical_type()
                logger.debug(
                    f"Column {field_name} has ODD type {odd_type} and logical type {logical_type}"
                )
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=field_name,
                        metadata=[
                            extract_metadata(
                                data_source, field, DefinitionType.DATASET_FIELD
                            )
                        ],
                        type=DataSetFieldType(
                            type=odd_type,
                            logical_type=logical_type,
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    )
                )

        _build_ds_field_from_type(field.name, field_type)
        return generated_dataset_fields

    def get_odd_type(self, basic_type: BasicType) -> Type:
        return self.odd_types_map.get(basic_type.type_name, Type.TYPE_UNKNOWN)

    def traverse_tree(self, node):
        if isinstance(node, Tree):
            if node.data == "array" or node.data == "list":
                if len(node.children) > 2:
                    raise StructureError(
                        object_type=node.data,
                        expected_size="<= 2",
                        actual_size=len(node.children),
                    )

                child = node.children[0]
                child_value = self.traverse_tree(child)

                if not isinstance(child_value, ParseType):
                    raise NonTypeObjectError(parent_object_type=node.data, child=child)

                return ArrayType(child_value)

            elif node.data == "map":
                subtypes = []
                for child in node.children:
                    child_value = self.traverse_tree(child)
                    if not isinstance(child_value, ParseType):
                        NonTypeObjectError(parent_object_type=node.data, child=child)
                    subtypes.append(child_value)
                return MapType(subtypes[0], subtypes[1])

            elif node.data == "struct":
                fields = {}
                for child in node.children:
                    value = self.traverse_tree(child)
                    if isinstance(value, Field):
                        fields[value.name] = value.type_value
                    else:
                        raise UnexpectedTypeError(
                            object_name="child",
                            received_type=type(value),
                            expected_type=Field,
                        )
                return StructType(fields)

            elif node.data == "union":
                fields = {}
                for child in node.children:
                    value = self.traverse_tree(child)
                    if isinstance(value, Field):
                        fields[value.name] = value.type_value
                    else:
                        raise UnexpectedTypeError(
                            object_name="child",
                            received_type=type(value),
                            expected_type=Field,
                        )
                return UnionType(fields)

            elif node.data == "field":
                if len(node.children) > 3:
                    raise StructureError(
                        object_type=node.data,
                        expected_size="<= 3",
                        actual_size=len(node.children),
                    )
                field_name_node, field_type_node = node.children[0], node.children[1]
                field_name = self.traverse_tree(field_name_node)
                if not isinstance(field_name, str):
                    raise UnexpectedTypeError(
                        object_name="field name",
                        received_type=type(field_name),
                        expected_type=str,
                    )
                field_type = self.traverse_tree(field_type_node)
                if not isinstance(field_type, ParseType):
                    raise UnexpectedTypeError(
                        object_name="field type",
                        received_type=type(field_type),
                        expected_type=ParseType,
                    )
                return Field(field_name, field_type)

            elif node.data == "primitive_type":
                if len(node.children) != 1:
                    raise StructureError(
                        object_type=node.data,
                        expected_size="= 1",
                        actual_size=len(node.children),
                    )
                return BasicType(node.children[0])

            else:
                raise UnexpectedTypeError(
                    object_name="tree",
                    received_type=node.data,
                    expected_type="array, list, map, struct, field, union, primitive_type",
                )

        elif isinstance(node, Token):
            if node.type == "BASIC_TYPE":
                return BasicType(node.value)
            elif node.type == "FIELD_NAME":
                return node.value
            else:
                raise UnexpectedTypeError(
                    object_name="token",
                    received_type=node.type,
                    expected_type="BASIC_TYPE, FIELD_NAME",
                )
        else:
            raise UnexpectedTypeError(
                object_name="node",
                received_type=type(node),
                expected_type="Tree, Token",
            )
