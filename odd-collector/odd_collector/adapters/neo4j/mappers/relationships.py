from collections import defaultdict

from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataRelationship,
    GraphRelationship,
    RelationshipType,
)
from oddrn_generator import Neo4jGenerator


def map_relationships(
    oddrn_generator: Neo4jGenerator,
    relationships: list,
    node_entities: dict[str, DataEntity],
) -> list[DataEntity]:
    data_entities = []

    grouped_relationships = _group_relationships(relationships)

    for relationship in grouped_relationships:
        source_dataset_name = relationship[0][0]
        target_dataset_name = relationship[2][0]

        source_dataset_oddrn = node_entities[source_dataset_name].oddrn
        target_dataset_oddrn = node_entities[target_dataset_name].oddrn

        relationship_name = (
            f"{source_dataset_name}_{relationship[1]}_{target_dataset_name}"
        )

        oddrn_generator.set_oddrn_paths(
            **{"nodes": source_dataset_name, "relationships": relationship_name}
        )

        data_entity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("relationships"),
            name=relationship_name,
            type=DataEntityType.GRAPH_RELATIONSHIP,
            data_relationship=DataRelationship(
                relationship_type=RelationshipType.GRAPH,
                source_dataset_oddrn=source_dataset_oddrn,
                target_dataset_oddrn=target_dataset_oddrn,
                details=GraphRelationship(
                    is_directed=True,
                    relationship_entity_name="GraphRelationship",
                    attributes={
                        property_name: "UNKNOWN"
                        for property_name in relationship[4]
                    },
                ),
            ),
        )
        data_entities.append(data_entity)

    return data_entities


def _group_relationships(relationships: list) -> list:
    # Group by (source_node, rel_type, target_node, sorted rel_properties)
    grouped_data = defaultdict(lambda: [0, set()])

    for row in relationships:
        key = (tuple(row[0]), row[1], tuple(row[2]), frozenset(row[4]))
        grouped_data[key][0] += row[3]
        grouped_data[key][1].update(row[4])

    result = [
        [list(key[0]), key[1], list(key[2]), count, list(properties)]
        for key, (count, properties) in grouped_data.items()
    ]
    return result
