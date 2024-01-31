from odd_collector.adapters.postgresql.models import ForeignKeyConstraint
from odd_models import DataEntity, DataEntityType, ERDRelationship
from odd_models import Relationship as DataEntityRelationship
from odd_models import RelationshipType
from oddrn_generator import PostgresqlGenerator


def map_relationship(
    generator: PostgresqlGenerator,
    fk_constraint: ForeignKeyConstraint,
    table_entities: dict[str, DataEntity],
) -> DataEntity:
    generator.set_oddrn_paths(
        schemas=fk_constraint.schema_name,
        tables=fk_constraint.table_name,
        relationships=(
            f"references_{fk_constraint.referenced_table_name}_with_{fk_constraint.constraint_name}"
        ),
    )

    source_dataset = table_entities[
        f"{fk_constraint.schema_name}.{fk_constraint.table_name}"
    ]
    target_dataset = table_entities[
        f"{fk_constraint.schema_name}.{fk_constraint.referenced_table_name}"
    ]

    source_dataset_field_list = [
        ds_field
        for ds_field in source_dataset.dataset.field_list
        if ds_field.name in fk_constraint.foreign_key
    ]
    target_dataset_field_list = [
        ds_field
        for ds_field in target_dataset.dataset.field_list
        if ds_field.name in fk_constraint.referenced_foreign_key
    ]

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("relationships"),
        name=fk_constraint.constraint_name,
        type=DataEntityType.RELATIONSHIP,
        data_entity_relationship=DataEntityRelationship(
            relationship_type=RelationshipType.ERD,
            source_dataset_oddrn=source_dataset.oddrn,
            target_dataset_oddrn=target_dataset.oddrn,
            details=ERDRelationship(
                source_dataset_field_oddrns_list=[
                    ds_field.oddrn for ds_field in source_dataset_field_list
                ],
                target_dataset_field_oddrns_list=[
                    ds_field.oddrn for ds_field in target_dataset_field_list
                ],
                is_identifying=all(
                    ds_field.is_primary_key for ds_field in target_dataset_field_list
                ),
                relationship_entity_name="ERDRelationship",
            ),
        ),
    )
