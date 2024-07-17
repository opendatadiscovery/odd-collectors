from odd_collector.adapters.neo4j.mappers.utils import (
    _get_node_name,
    _group_relationships,
)
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
        source_dataset_name = _get_node_name(relationship[0])
        target_dataset_name = _get_node_name(relationship[2])

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
                        property_name: "UNKNOWN" for property_name in relationship[4]
                    },
                ),
            ),
        )
        data_entities.append(data_entity)

    return data_entities
