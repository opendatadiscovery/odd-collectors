from functools import cached_property

from odd_collector.adapters.snowflake.domain import (
    ForeignKeyConstraint,
    UniqueConstraint,
)
from odd_collector.adapters.snowflake.mappers.relationships.cardinality_checker import (
    CardinalityChecker,
)
from odd_collector.adapters.snowflake.mappers.relationships.identifying_checker import (
    IdentifyingChecker,
)
from odd_models.models.models import (
    DataEntity,
    DataEntityType,
    ERDRelationship,
    DataRelationship,
    RelationshipType,
)


class RelationshipMapper:
    def __init__(
        self,
        fk_cons: ForeignKeyConstraint,
        oddrn: str,
        source: DataEntity,
        target: DataEntity,
        unique_constraints: list[UniqueConstraint],
    ):
        self.fk_cons = fk_cons
        self.source = source
        self.target = target
        self.oddrn = oddrn
        self.identifying_checker = IdentifyingChecker(
            foreign_key=self.fk_cons.foreign_key,
            ref_foreign_key=self.fk_cons.referenced_foreign_key,
            source_field_list=self._source_field_list,
            target_field_list=self._target_field_list,
        )
        self.cardinality_checker = CardinalityChecker(
            fk_field_list=self._fk_field_list,
            unique_constraints=unique_constraints,
        )

    def build_data_entity(self):
        return DataEntity(
            oddrn=self.oddrn,
            name=self.fk_cons.constraint_name,
            type=DataEntityType.ENTITY_RELATIONSHIP,
            data_relationship=self._build_relationship(),
        )

    @cached_property
    def _ref_fk_field_list(self):
        return [
            fl
            for fl in self._target_field_list
            if fl.name in self.fk_cons.referenced_foreign_key
        ]

    @cached_property
    def _fk_field_list(self):
        return [
            fl for fl in self._source_field_list if fl.name in self.fk_cons.foreign_key
        ]

    @cached_property
    def _target_field_list(self):
        return self.target.dataset.field_list

    @cached_property
    def _source_field_list(self):
        return self.source.dataset.field_list

    def _build_relationship(self) -> DataRelationship:
        return DataRelationship(
            relationship_type=RelationshipType.ERD,
            source_dataset_oddrn=self.source.oddrn,
            target_dataset_oddrn=self.target.oddrn,
            details=self._build_details(),
        )

    def _build_details(self):
        return ERDRelationship(
            source_dataset_field_oddrns_list=[fl.oddrn for fl in self._fk_field_list],
            target_dataset_field_oddrns_list=[
                fl.oddrn for fl in self._ref_fk_field_list
            ],
            is_identifying=self.identifying_checker.is_identifying(),
            cardinality=self.cardinality_checker.get_cardinality(),
            relationship_entity_name="ERDRelationship",
        )
