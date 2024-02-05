from adapters.postgresql.models import UniqueConstraint, Table


def group_uniques_constraints_by_table(
        unique_constraints: list[UniqueConstraint],
) -> dict[str, UniqueConstraint]:
    """
    Groups unique constraints by table name.

    Takes a list of UniqueConstraint objects and returns a dictionary
    with the table name as the key and the UniqueConstraint object as the value.

    Input:
    unique_constraints = [
      UniqueConstraint(schema_name='schema1', table_name='users', ...),
      UniqueConstraint(schema_name='schema1', table_name='posts', ...),
      UniqueConstraint(schema_name='schema2', table_name='comments', ...)
    ]

    Output:
    {
      'schema1.users': UniqueConstraint(...),
      'schema1.posts': UniqueConstraint(...),
      'schema2.comments': UniqueConstraint(...)
    }
    """
    return {f"{uc.schema_name}.{uc.table_name}": uc for uc in unique_constraints}


def filter_views(tables: list[Table]) -> list[Table]:
    """Filter a list of tables to only include view tables."""
    return list(filter(lambda table: table.table_type in ("v", "m"), tables))
