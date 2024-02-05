from odd_models.models.models import DataSetField

from adapters.postgresql.models import ForeignKeyConstraint


class IdentifyingChecker:
    def __init__(self,
                 foreign_key: tuple[str],
                 ref_foreign_key: tuple[str],
                 source_field_list: list[DataSetField],
                 target_field_list: list[DataSetField]):
        self.foreign_key = foreign_key
        self.ref_foreign_key = ref_foreign_key
        self.source_field_list = source_field_list
        self.target_field_list = target_field_list

    def is_identifying(self) -> bool:
        return self._is_fk_part_of_pk() and self._is_ref_fk_refers_to_pk()

    def _is_fk_part_of_pk(self) -> bool:
        """
        Check if the foreign key is part of primary key source table
        """
        fk_columns = {dsf.name for dsf in self.source_field_list if dsf.name in self.foreign_key}
        source_pk_columns = {dsf.name for dsf in self.source_field_list if dsf.is_primary_key}
        return all(cn in source_pk_columns for cn in fk_columns)

    def _is_ref_fk_refers_to_pk(self) -> bool:
        """
        Check if the referenced foreign key refers to primary key of target table
        """
        ref_fk_columns = {dsf.name for dsf in self.target_field_list if dsf.name in self.ref_foreign_key}
        ref_pk_columns = {dsf.name for dsf in self.target_field_list if dsf.is_primary_key}
        return ref_fk_columns == ref_pk_columns

