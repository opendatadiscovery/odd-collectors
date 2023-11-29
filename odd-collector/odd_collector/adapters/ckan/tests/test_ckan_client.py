import pytest
from aioresponses import aioresponses
from unittest.mock import AsyncMock

from odd_collector.adapters.ckan.client import CKANRestClient


@pytest.mark.parametrize(
    "config, endpoint, expected_endpoint",
    [
        ("ckan_adapter_config", "", ""),
        ("ckan_adapter_config", "/", ""),
        ("ckan_adapter_config", "/ckan-endpoint", "/ckan-endpoint"),
        ("ckan_adapter_config", "/ckan-endpoint/", "/ckan-endpoint"),
        ("ckan_adapter_config", "ckan-endpoint/", "/ckan-endpoint"),
    ],
)
def test_ckan_rest_client_endpoint(config, endpoint, expected_endpoint, request):
    ckan_config = request.getfixturevalue(config)
    ckan_rest_client = CKANRestClient(config=ckan_config(endpoint))
    assert ckan_rest_client._ckan_endpoint == expected_endpoint


@pytest.mark.parametrize(
    "response, expected_result",
    [
        ({}, False),
        ({"test_key": "test_value"}, False),
        ("test", False),
        ({"success": False}, False),
        ({"success": True}, True),
    ],
)
def test_is_response_successful(ckan_adapter_config, response, expected_result):
    ckan_rest_client = CKANRestClient(ckan_adapter_config("/ckan-endpoint"))

    assert ckan_rest_client.is_response_successful(response) == expected_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_params, expected_result",
    [
        ((15, 5), (15, 3)),
        ((210, 1), (200, 200)),
    ],
)
async def test_pagination_request(
    ckan_adapter_config,
    valid_pagination_request_result,
    request_params,
    expected_result,
):
    ckan_rest_client = CKANRestClient(ckan_adapter_config("/ckan-endpoint"))

    count_number, rows_number = request_params
    ckan_rest_client._get_request = AsyncMock(
        return_value=valid_pagination_request_result(
            count_number=count_number, rows_number=rows_number
        )
    )
    resp = await ckan_rest_client._pagination_request(
        "https://test.com/api", {"rows": rows_number, "start": 0}
    )

    expected_result_len, expected_get_request_call_count = expected_result
    assert len(resp) == expected_result_len
    assert ckan_rest_client._get_request.call_count == expected_get_request_call_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "aiohttp_payload_status, expected_result",
    [
        (
            ({"success": True, "author": "test"}, 200),
            {"success": True, "author": "test"},
        ),
        (({"success": False}, 100), {"success": False}),
        (("Bad request", 400), {"success": False}),
    ],
)
async def test_client_post_request(
    ckan_adapter_config, aiohttp_payload_status, expected_result
):
    responses = aioresponses()

    with responses:
        test_url = "https://test.com/api"

        payload, status = aiohttp_payload_status
        responses.post(test_url, payload=payload, status=status)

        ckan_rest_client = CKANRestClient(ckan_adapter_config("/ckan-endpoint"))
        result = await ckan_rest_client._post_request(url=test_url, payload={})

        assert result == expected_result
