from itertools import chain
from odd_models.models.models import CardinalityType, DataSetField

from adapters.postgresql.models import UniqueConstraint


class CardinalityChecker:
    def __init__(self,
                 ref_fk_field_list: list[DataSetField],
                 unique_constraints: list[UniqueConstraint]):
        """
        :param ref_fk_field_list: list of DataSetField of referenced foreign key
        :param unique_constraints: list of UniqueConstraint of target table
        """
        self.ref_fk_field_list = ref_fk_field_list
        self.unique_constraints = unique_constraints

    def get_cardinality(self) -> CardinalityType:
        is_ref_to_unique = self._is_ref_to_unique()
        is_ref_to_nullable = self._is_ref_to_nullable()

        if is_ref_to_unique & is_ref_to_nullable:
            return CardinalityType.ONE_TO_ZERO_OR_ONE

        if is_ref_to_unique & (not is_ref_to_nullable):
            return CardinalityType.ONE_TO_EXACTLY_ONE

        if is_ref_to_nullable & (not is_ref_to_unique):
            return CardinalityType.ONE_TO_ZERO_ONE_OR_MORE

        return CardinalityType.ONE_TO_ONE_OR_MORE

    def _is_ref_to_nullable(self) -> bool:
        """
        Check if the referenced foreign key refers to nullable column of target table
        """
        return all(dsf.type.is_nullable for dsf in self.ref_fk_field_list)

    def _is_ref_to_unique(self) -> bool:
        """
        Check if the referenced foreign key refers to unique constraint of target table
        """
        ref_fk_columns = {dsf.name for dsf in self.ref_fk_field_list}
        ref_uc_columns = list(chain.from_iterable(
            uc.column_names for uc in self.unique_constraints
        ))

        return any(cn in ref_uc_columns for cn in ref_fk_columns)
