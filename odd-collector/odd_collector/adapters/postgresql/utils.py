from odd_collector.adapters.postgresql.models import Table


def filter_views(tables: list[Table]) -> list[Table]:
    """Filter a list of tables to only include view tables."""
    return list(filter(lambda table: table.table_type in ("v", "m"), tables))
