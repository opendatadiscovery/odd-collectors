import pytest


@pytest.mark.parametrize(
    "resource_name, expected_entity_name",
    [
        ("test name", "test name"),
        (None, "CKAN_test_organization_name_408d4834-6075-4952-8772-8d2ff0afa118"),
    ],
)
def test_map_resource(
    ckan_generator, create_resource, resource_name, expected_entity_name
):
    from odd_collector.adapters.ckan.mappers.resource import map_resource

    organization_name, dataset_name = "test_organization_name", "test_dataset_name"
    ckan_generator.set_oddrn_paths(
        organizations=organization_name, datasets=dataset_name
    )

    resource = create_resource(name=resource_name)
    resource_data_entity = map_resource(ckan_generator, organization_name, resource, [])

    assert resource_data_entity.name == expected_entity_name
