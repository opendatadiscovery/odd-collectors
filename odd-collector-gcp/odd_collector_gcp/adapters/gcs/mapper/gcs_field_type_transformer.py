from lark import Token, Transformer


class GCSFieldTypeTransformer(Transformer):
    def field_definition(self, items):
        return items[1] | {"field_name": str(items[0])}

    def struct_type(self, items):
        return {"type": "struct", "children": items}

    def dictionary_type(self, items):
        return {"type": "dictionary", "children": items}

    def type(self, items):
        obj = items[0]
        return (
            {"type": str(obj).lower()}
            if isinstance(obj, Token) and obj.type == "SIMPLE_TYPE"
            else obj
        )

    def varchar_type(self, items):
        if not len(items):
            return {"type": "varchar"}

        return {"type": "varchar", "logical_type": f"varchar({items[0]})"}

    def decimal_type(self, items):
        if not len(items):
            return {"type": "decimal"}

        return {"type": "decimal", "logical_type": f"decimal({items[0]}, {items[1]})"}

    def iterable_type(self, items):
        return {"type": "list", "children": items}

    def map_type(self, items):
        return {
            "type": "map",
            "key_type": items[0],
            "value_type": items[1],
        }

    def union_type(self, items):
        return {"type": "union", "children": items}

    def list_type(self, items):
        return {"type": "list", "children": items}

    def timestamp_type(self, items):
        if not len(items):
            return {"type": "timestamp"}

        return {"type": "timestamp", "logical_type": f"timestamp[{items[0].value}]"}


field_type_transformer = GCSFieldTypeTransformer()
