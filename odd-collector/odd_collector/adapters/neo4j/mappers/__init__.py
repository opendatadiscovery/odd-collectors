from collections import namedtuple

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/neo4j.json#/definitions/Neo4j"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"

_data_set_metadata_excluded_keys: set = {"node_labels"}

_node_metadata: str = "node_labels, nodes_count, properties"

_relationship_metadata: str = "source_node_labels, relation_type, target_node_labels, relations_count, relation_properties"

_field_metadata: str = "field_name, data_type"

NodeMetadata = namedtuple("NodeMetadata", _node_metadata)

RelationshipMetadata = namedtuple("RelationshipMetadata", _relationship_metadata)

FieldMetadata = namedtuple("FieldMetadata", _field_metadata)
