from odd_collector.adapters.postgresql.models import Relationship
from odd_models import DataEntity, ERDRelationship
from odd_models import Relationship as RelationshipEntity
from oddrn_generator import PostgresqlGenerator


def map_relationship(
    generator: PostgresqlGenerator,
    relationship: Relationship,
    table_entities: dict[str, DataEntity],
) -> RelationshipEntity:
    generator.set_oddrn_paths(
        **{
            "schemas": relationship.schema_name,
            "tables": relationship.table_name,
            "relationships": (
                f"references_{relationship.referenced_table_name}_with_{relationship.constraint_name}"
            ),
        }
    )

    source_dataset = table_entities[
        f"{relationship.schema_name}.{relationship.table_name}"
    ]
    target_dataset = table_entities[
        f"{relationship.schema_name}.{relationship.referenced_table_name}"
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
        oddrn=generator.get_oddrn_by_path("relationships"),
        name=relationship.constraint_name,
        source_dataset_oddrn=source_dataset.oddrn,
        target_dataset_oddrn=target_dataset.oddrn,
        erd_relationship=ERDRelationship(
            source_dataset_field_oddrns_list=source_dataset_field_oddrns_list,
            target_dataset_field_oddrns_list=target_dataset_field_oddrns_list,
        ),
    )
