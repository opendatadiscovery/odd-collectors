import pytest
from odd_models import DataEntityType


@pytest.mark.parametrize(
    "view_fixture, expected_data_entity_type",
    [
        ("view", DataEntityType.VIEW),
        ("view_with_vector_column", DataEntityType.VECTOR_STORE),
        ("materialized_view", DataEntityType.VIEW),
        ("materialized_view_with_vector_column", DataEntityType.VECTOR_STORE),
    ],
)
def test_map_view(
    postgresql_generator, view_fixture, expected_data_entity_type, request
):
    from odd_collector.adapters.postgresql.mappers.views import map_view

    view = request.getfixturevalue(view_fixture)

    data_entity = map_view(postgresql_generator, view)
    assert data_entity.type == expected_data_entity_type
