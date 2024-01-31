from odd_models.models import DataEntity, RelationshipType, DataEntityType, ERDRelationship, Relationship, DataSetField
from oddrn_generator import PostgresqlGenerator

from adapters.postgresql.models import ForeignKeyConstraint, UniqueConstraint


class RelationshipMapper:
    def __init__(
            self,
            generator: PostgresqlGenerator,
            unique_constraints: dict[str, UniqueConstraint],
            table_entities: dict[str, DataEntity],
    ):
        self.generator = generator
        self.unique_constraints_dict = unique_constraints
        self.table_entities = table_entities

    def _get_dataset(self, schema_name: str, table_name: str) -> DataEntity:
        return self.table_entities[f"{schema_name}.{table_name}"]

    def _get_source_field_list(self, foreign_key_constraint: ForeignKeyConstraint) -> list[DataSetField]:
        source_dataset = self._get_dataset(foreign_key_constraint.schema_name, foreign_key_constraint.table_name)

        return [
            ds_field
            for ds_field in source_dataset.dataset.field_list
            if ds_field.name in foreign_key_constraint.foreign_key
        ]

    def _get_target_field_list(self, foreign_key_constraint: ForeignKeyConstraint) -> list[DataSetField]:
        target_dataset = self._get_dataset(
            foreign_key_constraint.schema_name, foreign_key_constraint.referenced_table_name
        )

        return [
            ds_field
            for ds_field in target_dataset.dataset.field_list
            if ds_field.name in foreign_key_constraint.referenced_foreign_key
        ]

    def _build_relationship(self, foreign_key_constraint: ForeignKeyConstraint):
        source_dataset = self._get_dataset(foreign_key_constraint.schema_name, foreign_key_constraint.table_name)

        target_dataset = self._get_dataset(
            foreign_key_constraint.schema_name, foreign_key_constraint.referenced_table_name
        )

        Relationship(
            relationship_type=RelationshipType.ERD,
            source_dataset_oddrn=source_dataset.oddrn,
            target_dataset_oddrn=target_dataset.oddrn,
            details=self._build_erd_relationship(foreign_key_constraint),
        ),

    def _build_erd_relationship(self, foreign_key_constraint: ForeignKeyConstraint):
        return ERDRelationship(
            source_dataset_field_oddrns_list=[
                ds_field.oddrn for ds_field in self._get_source_field_list(foreign_key_constraint)
            ],
            target_dataset_field_oddrns_list=[
                ds_field.oddrn for ds_field in self._get_target_field_list(foreign_key_constraint)
            ],
            is_identifying=check_is_identifying(),
            cardinality=check_cardinality(),
            relationship_entity_name="ERDRelationship",
        )

    def map(self, fk_constraints: list[ForeignKeyConstraint]):
        for fkc in fk_constraints:
            self.generator.set_oddrn_paths(
                schemas=fkc.schema_name,
                tables=fkc.table_name,
                relationships=(
                    f"references_{fkc.referenced_table_name}_with_{fkc.constraint_name}"
                ),
            )

            return DataEntity(
                oddrn=self.generator.get_oddrn_by_path("relationships"),
                name=fkc.constraint_name,
                type=DataEntityType.RELATIONSHIP,
                data_entity_relationship=self._build_relationship(fkc),
            )
