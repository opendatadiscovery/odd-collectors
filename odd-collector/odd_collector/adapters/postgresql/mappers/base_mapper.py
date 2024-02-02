from typing import Protocol, TypeVar, Union

from odd_collector.adapters.postgresql.models import (
    ForeignKeyConstraint,
    UniqueConstraint,
)
from odd_models.models import (
    CardinalityType,
    DataEntity,
    DataEntityType,
    ERDRelationship,
    Relationship,
    RelationshipType,
)
from oddrn_generator import PostgresqlGenerator

T = TypeVar("T")


class Mappable(Protocol[T]):
    def map(self, data: Union[list[T], T]) -> list[DataEntity]:
        ...


class DataEntityRelationshipMapper(Mappable[ForeignKeyConstraint]):
    def __init__(
        self,
        oddrn_generator: PostgresqlGenerator,
        unique_constraints: dict[str, UniqueConstraint],
        datasets: dict[str, DataEntity],
    ):
        self.oddrn_generator = oddrn_generator
        self.unique_constraints = unique_constraints
        self.datasets = datasets

    def map(self, data: list[ForeignKeyConstraint]) -> list[DataEntity]:
        return [self._map_single_constraint(fkc) for fkc in data]

    def _map_single_constraint(self, fk_constraint: ForeignKeyConstraint) -> DataEntity:
        relationship_name = (
            f"references_{fk_constraint.referenced_table_name}_"
            f"with_{fk_constraint.constraint_name}"
        )
        self.oddrn_generator.set_oddrn_paths(
            schemas=fk_constraint.schema_name,
            tables=fk_constraint.table_name,
            relationships=relationship_name,
        )

        return DataEntity(
            oddrn=self.oddrn_generator.get_oddrn_by_path("relationships"),
            name=fk_constraint.constraint_name,
            type=DataEntityType.RELATIONSHIP,
            data_entity_relationship=self._build_relationship(fk_constraint),
        )

    def _get_dataset(self, schema_name: str, table_name: str) -> DataEntity:
        return self.datasets[f"{schema_name}.{table_name}"]

    def _get_dataset_field_list(self, schema_name: str, table_name: str):
        return self._get_dataset(schema_name, table_name).dataset.field_list

    def _get_dataset_field_list_filter_on_constraint_foreign_key(
        self,
        fk_constraint: ForeignKeyConstraint,
    ):
        return [
            dsf
            for dsf in self._get_dataset_field_list(
                fk_constraint.schema_name, fk_constraint.table_name
            )
            if dsf.name in fk_constraint.foreign_key
        ]

    def _get_dataset_field_list_filter_on_constraint_referenced_foreign_key(
        self,
        fk_constraint: ForeignKeyConstraint,
    ):
        return [
            dsf
            for dsf in self._get_dataset_field_list(
                fk_constraint.schema_name, fk_constraint.referenced_table_name
            )
            if dsf.name in fk_constraint.referenced_foreign_key
        ]

    def _get_unique_constraint(
        self, schema_name: str, table_name: str
    ) -> Union[UniqueConstraint, None]:
        return self.unique_constraints.get(f"{schema_name}.{table_name}")

    def _build_relationship(self, fk_constraint: ForeignKeyConstraint) -> Relationship:
        source_dataset_oddrn = self._get_dataset(
            fk_constraint.schema_name, fk_constraint.table_name
        ).oddrn
        target_dataset_oddrn = self._get_dataset(
            fk_constraint.schema_name, fk_constraint.referenced_table_name
        ).oddrn

        return Relationship(
            relationship_type=RelationshipType.ERD,
            source_dataset_oddrn=source_dataset_oddrn,
            target_dataset_oddrn=target_dataset_oddrn,
            details=self._build_details(fk_constraint),
        )

    def _build_details(self, fk_constraint: ForeignKeyConstraint):
        source_dataset_field_oddrns_list = [
            dsf.oddrn
            for dsf in self._get_dataset_field_list_filter_on_constraint_foreign_key(
                fk_constraint
            )
        ]
        target_dataset_field_oddrns_list = [
            dsf.oddrn
            for dsf in self._get_dataset_field_list_filter_on_constraint_referenced_foreign_key(
                fk_constraint
            )
        ]
        return ERDRelationship(
            source_dataset_field_oddrns_list=source_dataset_field_oddrns_list,
            target_dataset_field_oddrns_list=target_dataset_field_oddrns_list,
            is_identifying=self._check_is_identifying(fk_constraint),
            cardinality=self._check_cardinality(fk_constraint),
            relationship_entity_name="ERDRelationship",
        )

    def _check_is_identifying(self, fk_constraint: ForeignKeyConstraint) -> bool:
        ref_fk_field_list = [
            dsf.name
            for dsf in self._get_dataset_field_list_filter_on_constraint_referenced_foreign_key(
                fk_constraint
            )
        ]

        ref_all_field_list = self._get_dataset_field_list(
            fk_constraint.schema_name, fk_constraint.referenced_table_name
        )
        source_all_field_list = self._get_dataset_field_list(
            fk_constraint.schema_name, fk_constraint.table_name
        )
        ref_pk_columns = {
            ds_field.name for ds_field in ref_all_field_list if ds_field.is_primary_key
        }
        source_pk_columns = {
            ds_field.name
            for ds_field in source_all_field_list
            if ds_field.is_primary_key
        }

        return all(cn in source_pk_columns for cn in ref_pk_columns) and all(
            cn in ref_pk_columns for cn in ref_fk_field_list
        )

    def _check_cardinality(
        self, fk_constraint: ForeignKeyConstraint
    ) -> CardinalityType:
        ref_fk_field_name_list = [
            dsf.name
            for dsf in self._get_dataset_field_list_filter_on_constraint_referenced_foreign_key(
                fk_constraint
            )
        ]

        ref_fk_field_is_nullable_status_list = [
            dsf.type.is_nullable
            for dsf in self._get_dataset_field_list_filter_on_constraint_referenced_foreign_key(
                fk_constraint
            )
        ]

        uc_by_full_name = self._get_unique_constraint(
            fk_constraint.schema_name, fk_constraint.referenced_table_name
        )

        ref_uc_column_names = (
            uc_by_full_name.column_names
            if uc_by_full_name is not None
            else uc_by_full_name
        )

        referenced_to_unique = (
            any(cn in ref_uc_column_names for cn in ref_fk_field_name_list)
            if ref_uc_column_names is not None
            else False
        )

        if referenced_to_unique:
            if all(ref_fk_field_is_nullable_status_list):
                return CardinalityType.ONE_TO_ZERO_OR_ONE
            else:
                return CardinalityType.ONE_TO_EXACTLY_ONE
        else:
            if all(ref_fk_field_is_nullable_status_list):
                return CardinalityType.ONE_TO_ZERO_ONE_OR_MORE
            else:
                return CardinalityType.ONE_TO_ONE_OR_MORE
