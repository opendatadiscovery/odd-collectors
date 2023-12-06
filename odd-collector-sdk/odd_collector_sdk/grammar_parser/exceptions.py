from typing import Any

from odd_collector_sdk.grammar_parser.field_types import ParseType


class ParserError(Exception):
    pass


class NonTypeObjectError(ParserError):
    def __init__(self, parent_object_type: str, child: Any):
        self.message = f"""
        {parent_object_type} got a non-type object {child}
        Which cannot be mapped to the {ParseType}
        """
        super().__init__(self.message)


class UnexpectedTypeError(ParserError):
    def __init__(self, object_name: str, received_type: Any, expected_type: Any):
        self.message = f"""
           Unexpected {object_name} type: {received_type}
           Expected type(s): {expected_type}
           """
        super().__init__(self.message)


class StructureError(ParserError):
    def __init__(self, object_type: str, expected_size: str, actual_size: int):
        self.message = f"""
        Invalid {object_type} structure: expected children count {expected_size} , got: {actual_size}
        """
        super().__init__(self.message)
