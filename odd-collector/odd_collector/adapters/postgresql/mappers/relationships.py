from odd_collector.adapters.postgresql.models import Relationship
from odd_models import (
    Relationship as RelationshipEntity,
    DataEntity,
    ERDRelationship,
)
from oddrn_generator import PostgresqlGenerator


def map_relationship(
    generator: PostgresqlGenerator, relationship: Relationship, table_entities: dict[str, DataEntity]
) -> RelationshipEntity:
    source_dataset = table_entities[
        f"{relationship.namespace}.{relationship.table_name}"
    ]
    target_dataset = table_entities[
        f"{relationship.namespace}.{relationship.referenced_table_name}"
    ]

    source_dataset_field_oddrns_list = [
        s_ds_field.oddrn
        for s_ds_field in source_dataset.dataset.field_list
        if s_ds_field.name in relationship.foreign_key
    ]
    target_dataset_field_oddrns_list = [
        t_ds_field.oddrn
        for t_ds_field in target_dataset.dataset.field_list
        if t_ds_field.name in relationship.referenced_foreign_key
    ]
    return RelationshipEntity(
        oddrn="filler",
        name=relationship.constraint_name,
        source_dataset_oddrn=source_dataset.oddrn,
        target_dataset_oddrn=target_dataset.oddrn,
        erd_relationship=ERDRelationship(
            source_dataset_field_oddrns_list=source_dataset_field_oddrns_list,
            target_dataset_field_oddrns_list=target_dataset_field_oddrns_list
        )
    )
