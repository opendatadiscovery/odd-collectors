"""
In order to serialize custom data types you will need to update this module with appropriate logic.
"""

import json
from uuid import UUID


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)
