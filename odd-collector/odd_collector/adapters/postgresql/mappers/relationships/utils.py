from collections import defaultdict

from adapters.postgresql.models import UniqueConstraint


def group_uniques_constraints_by_table(
        unique_constraints: list[UniqueConstraint],
) -> dict[str, list[UniqueConstraint]]:
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
      'schema1.users': [UniqueConstraint(...)],
      'schema1.posts': [UniqueConstraint(...), ...],
      'schema2.comments': [UniqueConstraint(...), ...]
    }
    """
    grouped = defaultdict(list)

    for uc in unique_constraints:
        key = f"{uc.schema_name}.{uc.table_name}"
        grouped[key].append(uc)
    return dict(grouped)
