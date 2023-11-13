import pytest
import odd_models


@pytest.mark.parametrize(
    "view_fixture, expected_data_entity_type",
    [
        ("view_without_vector_column", odd_models.DataEntityType.VIEW),
        ("view_with_vector_column", odd_models.DataEntityType.VECTOR_STORE),
        ("materialized_view_without_vector_column", odd_models.DataEntityType.VIEW),
        ("materialized_view_with_vector_column", odd_models.DataEntityType.VECTOR_STORE),
    ]
)
def test_map_view(postgresql_generator, view_fixture, expected_data_entity_type, request):
    from odd_collector.adapters.postgresql.mappers.views import map_view

    view = request.getfixturevalue(view_fixture)

    data_entity = map_view(postgresql_generator, view)
    assert isinstance(data_entity, odd_models.DataEntity)
    assert hasattr(data_entity, "type")
    assert data_entity.type == expected_data_entity_type
