from odd_models.models.models import CardinalityType, DataSetField


class CardinalityChecker:
    def __init__(
        self,
        ref_fk_field_list: list[DataSetField],
    ):
        """
        :param ref_fk_field_list: list of DataSetField of referenced foreign key
        """
        self.ref_fk_field_list = ref_fk_field_list

    def get_cardinality(self) -> CardinalityType:
        """
        Because of specific rules of Snowflake's foreign key constraint declaring
        (referenced fk must be built either of pk columns, or unique columns) - we
        can't have this 2 types of cardinality: CardinalityType.ONE_TO_ZERO_ONE_OR_MORE and
        CardinalityType.ONE_TO_ONE_OR_MORE.
        """
        if self._is_ref_to_nullable:
            return CardinalityType.ONE_TO_ZERO_OR_ONE

        return CardinalityType.ONE_TO_EXACTLY_ONE

    @property
    def _is_ref_to_nullable(self) -> bool:
        """
        Check if the referenced foreign key refers to nullable column of target table
        """
        return all(dsf.type.is_nullable for dsf in self.ref_fk_field_list)
