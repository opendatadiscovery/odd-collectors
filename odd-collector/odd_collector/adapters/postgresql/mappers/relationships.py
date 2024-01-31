from odd_collector.adapters.postgresql.models import Relationship
from odd_models import DataEntity, DataEntityType, ERDRelationship
from odd_models import Relationship as DataEntityRelationship
from odd_models import RelationshipType
from oddrn_generator import PostgresqlGenerator


def map_relationship(
    generator: PostgresqlGenerator,
    relationship: Relationship,
    table_entities: dict[str, DataEntity],
) -> DataEntity:
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

    source_dataset_field_list = [
        ds_field
        for ds_field in source_dataset.dataset.field_list
        if ds_field.name in relationship.foreign_key
    ]
    target_dataset_field_list = [
        ds_field
        for ds_field in target_dataset.dataset.field_list
        if ds_field.name in relationship.referenced_foreign_key
    ]

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("relationships"),
        name=relationship.constraint_name,
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
