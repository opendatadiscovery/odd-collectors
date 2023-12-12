from unittest.mock import AsyncMock

import pytest
from odd_collector.adapters.ckan.adapter import Adapter


@pytest.mark.asyncio
async def test_get_data_entity_list_order(
    create_ckan_adapter_config,
    create_group,
    create_organization,
    create_dataset,
):
    adapter = Adapter(config=create_ckan_adapter_config())

    group_name, organization_name = "test group name", "test organization name"
    dataset_name, resource_name = "test dataset name", "test resource name"

    adapter.client.get_organizations = AsyncMock(
        return_value=[
            create_organization(name=organization_name),
        ]
    )
    adapter.client.get_groups = AsyncMock(
        return_value=[
            group_name,
        ]
    )
    adapter.client.get_datasets = AsyncMock(
        return_value=[
            create_dataset(name=dataset_name),
        ]
    )
    adapter.client.get_resource_fields = AsyncMock(return_value=[])
    adapter.client.get_group_details = AsyncMock(
        return_value=create_group(name=group_name)
    )

    result = await adapter.get_data_entity_list()
    assert result.items[0].name == group_name
    assert result.items[1].name == organization_name
    assert result.items[2].name == dataset_name
    assert result.items[3].name == resource_name
