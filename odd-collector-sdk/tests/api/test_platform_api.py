import pytest
from aioresponses import aioresponses
from odd_models.models import DataEntityList, DataSourceList

from odd_collector_sdk.api.datasource_api import PlatformApi


@pytest.fixture
def platform_api():
    return PlatformApi(
        token="test-token",
        platform_url="http://test-platform-url",
        connection_timeout_seconds=300,
        verify_ssl=False,
    )


@pytest.fixture
def data_source_list():
    return DataSourceList(items=[])


@pytest.fixture
def data_entity_list():
    return DataEntityList(data_source_oddrn="//test_datasource/host/localhost", items=[])


@pytest.mark.asyncio
async def test_register_datasource(platform_api, data_source_list):
    with aioresponses() as mock:
        url = f"{platform_api.platform_url}/ingestion/datasources"

        mock.post(url, status=200, payload={"message": "some success message"})
        response = await platform_api.register_datasource(data_source_list)
        assert response.status == 200

        mock.post(url, status=404, payload={"message": "some error message"})
        with pytest.raises(Exception) as exc_info:
            await platform_api.register_datasource(data_source_list)
        assert "Platform response" in str(exc_info.value)
        assert "{'message': 'some error message'}" in str(exc_info.value)


@pytest.mark.asyncio
async def test_ingest_data(platform_api, data_entity_list):
    with aioresponses() as mock:
        url = f"{platform_api.platform_url}/ingestion/entities"

        mock.post(url, status=200, payload={"message": "success"})
        response = await platform_api.ingest_data(data_entity_list)
        assert response.status == 200

        mock.post(url, status=404, payload={"message": "some error message"})
        with pytest.raises(Exception) as exc_info:
            await platform_api.ingest_data(data_entity_list)
        assert "Platform response" in str(exc_info.value)
        assert "{'message': 'some error message'}" in str(exc_info.value)
