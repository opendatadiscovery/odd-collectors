from functools import cached_property
from itertools import chain

from odd_collector.adapters.postgresql.models import UniqueConstraint
from odd_models.models.models import CardinalityType, DataSetField


class CardinalityChecker:
    def __init__(
        self,
        fk_field_list: list[DataSetField],
        unique_constraints: list[UniqueConstraint],
    ):
        """
        :param fk_field_list: list of DataSetField of source table foreign key
        :param unique_constraints: list of UniqueConstraint of source table
        """
        self.fk_field_list = fk_field_list
        self.unique_constraints = unique_constraints

    def get_cardinality(self) -> CardinalityType:
        if self._is_ref_to_unique:
            return CardinalityType.ONE_TO_ZERO_OR_ONE

        return CardinalityType.ONE_TO_ZERO_ONE_OR_MORE

    @cached_property
    def _is_ref_to_unique(self) -> bool:
        """
        Check if the source table foreign key refers to unique constraint
        or primary key of source table
        """
        fk_columns = {dsf.name for dsf in self.fk_field_list}
        uc_columns = list(
            chain.from_iterable(uc.column_names for uc in self.unique_constraints)
        )

        return any(cn in uc_columns for cn in fk_columns) or any(
            dsf.is_primary_key for dsf in self.fk_field_list
        )
