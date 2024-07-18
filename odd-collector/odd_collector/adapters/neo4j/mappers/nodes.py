from typing import Dict, NamedTuple

from odd_collector.adapters.neo4j.mappers.utils import (
    _group_nodes_by_labels,
    _group_nodes_by_relationships_presence,
)
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import Neo4jGenerator

from . import (
    FieldMetadata,
    NodeMetadata,
    RelationshipMetadata,
    _data_set_metadata_excluded_keys,
    _data_set_metadata_schema_url,
)
from .fields import map_field
from .metadata import append_metadata_extension


def map_nodes(
    oddrn_generator: Neo4jGenerator, nodes: list, relations: list
) -> dict[str, DataEntity]:
    data_entities: dict[str, DataEntity] = {}

    nodes_map: Dict[str, list[NamedTuple]] = {}

    _group_nodes_by_labels(nodes_map, nodes)

    _group_nodes_by_relationships_presence(nodes_map, relations)

    for node_name in nodes_map:
        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("nodes", node_name),
            name=node_name,
            type=DataEntityType.GRAPH_NODE,
            metadata=[],
        )
        # Dataset
        data_entity.dataset = DataSet(
            parent_oddrn=oddrn_generator.get_oddrn_by_path("databases"), field_list=[]
        )
        fields: set = set()
        for metadata in nodes_map[node_name]:
            items = metadata._asdict()
            if "properties" in items:
                fields = set.union(fields, set(items["properties"]))

            excluded_keys = (
                _data_set_metadata_excluded_keys if items.get("node_labels") else None
            )
            append_metadata_extension(
                data_entity.metadata,
                _data_set_metadata_schema_url,
                metadata,
                excluded_keys,
            )

        for field in [(field_name, "string") for field_name in fields]:
            meta: FieldMetadata = FieldMetadata(*field)
            data_entity.dataset.field_list.append(
                map_field(meta, oddrn_generator, data_entity.owner)
            )

        data_entities[node_name] = data_entity

    return data_entities
