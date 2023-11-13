import pytest
from odd_models import DataEntityType


@pytest.mark.parametrize(
    "table_fixture, expected_data_entity_type",
    [
        ("table", DataEntityType.TABLE),
        ("table_with_vector_column", DataEntityType.VECTOR_STORE),
    ]
)
def test_map_table(postgresql_generator, table_fixture, expected_data_entity_type, request):
    from odd_collector.adapters.postgresql.mappers.tables import map_table

    table = request.getfixturevalue(table_fixture)

    data_entity = map_table(postgresql_generator, table)
    assert data_entity.type == expected_data_entity_type
