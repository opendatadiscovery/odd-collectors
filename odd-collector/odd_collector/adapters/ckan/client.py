from asyncio import gather

import aiohttp
from odd_collector.domain.plugin import CKANPlugin
from odd_collector_sdk.errors import DataSourceError

from .logger import logger
from .mappers.models import Dataset, Group, Organization, ResourceField


class CKANRestClient:
    def __init__(self, config: CKANPlugin):
        self.__host = f"https://{config.host}:{config.port}"
        self._ckan_endpoint = (
            f"/{config.ckan_endpoint.rstrip('/')}"
            if config.ckan_endpoint and config.ckan_endpoint[0] != "/"
            else config.ckan_endpoint.rstrip("/")
        )
        self.__headers = (
            {"Authorization": config.token.get_secret_value()} if config.token else None
        )

    @staticmethod
    def is_response_successful(resp: dict):
        if isinstance(resp, dict) and resp.get("success"):
            return True
        return False

    async def _get_request(self, url: str, params: dict = None) -> dict:
        async with aiohttp.ClientSession(
            self.__host,
            headers=self.__headers,
        ) as session:
            try:
                async with session.get(url, params=params) as resp:
                    result = await resp.json()
                    logger.debug(f"Result of request {url} is {result}")
                    if not self.is_response_successful(result):
                        raise DataSourceError(
                            f"Request: {url}, Error: {result['error']}"
                        )
                return result
            except Exception as e:
                raise DataSourceError(
                    f"Error during getting data from host {self.__host}: {e}"
                ) from e

    async def _post_request(self, url: str, payload: dict = None) -> dict:
        async with aiohttp.ClientSession(
            self.__host,
            headers=self.__headers,
        ) as session:
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status not in list(range(200, 300)):
                        logger.debug(
                            f"POST request for {url} with payload={payload} has not been successful "
                            f"with a response status code = {resp.status}"
                        )
                        result = {"success": False}
                    else:
                        result = await resp.json()
                        logger.debug(
                            f"POST request for {url} with payload={payload} has been successful "
                            f"with a result = {result}"
                        )
                return result
            except Exception as e:
                raise DataSourceError(
                    f"Error during getting data from host {self.__host}: {e}"
                ) from e

    async def _pagination_request(self, url: str, params: dict) -> list[dict]:
        """
        Custom pagination function for getting all the datasets from request on specified url
        with given query parameters.
        params["rows"] and params["start"] are essential for pagination, as "start" specifies
        the start index of result response and "rows" specifies how much entites we will get
        once per request.
        """
        pagination_interval = params["rows"]

        result = []
        for _ in range(200):  # Limit the number of iterations to 200
            resp = await self._get_request(url, params)
            result.extend(resp["result"]["results"])
            if resp["result"]["count"] == len(result):
                break
            params["start"] += pagination_interval
        return result

    async def get_organizations(self) -> list[Organization]:
        url = f"{self._ckan_endpoint}/api/action/organization_list"
        resp = await self._get_request(url)
        org_names = resp["result"]
        response = await gather(
            *[
                self.get_organization_details(organization_name)
                for organization_name in org_names
            ]
        )
        return response

    async def get_organization_details(self, organization_name: str) -> Organization:
        url = f"{self._ckan_endpoint}/api/action/organization_show"
        params = {"id": organization_name}
        resp = await self._get_request(url, params)
        return Organization(resp["result"])

    async def get_groups(self) -> list[str]:
        url = f"{self._ckan_endpoint}/api/action/group_list"
        resp = await self._get_request(url)
        return resp["result"]

    async def get_group_details(self, group_name: str) -> Group:
        url = f"{self._ckan_endpoint}/api/action/group_show"
        params = {"id": group_name, "include_datasets": "True"}
        resp = await self._get_request(url, params)
        return Group(resp["result"])

    async def get_datasets(self, organization_id: str) -> list[Dataset]:
        url = f"{self._ckan_endpoint}/api/action/package_search"
        stable_params = {"q": f"owner_org:{organization_id}", "rows": 1000, "start": 0}
        try:
            params = {**stable_params, "include_private": "True"}
            resp = await self._pagination_request(url, params)
        except:
            logger.debug(
                "Private datasets are unavailable or API doesn't support 'include_private' parameter. "
                "Excluding {'include_private': 'True'} query parameter. "
                f"Trying to get public datasets for owner_org: {organization_id}."
            )
            resp = await self._pagination_request(url, stable_params)
        return [Dataset(dataset) for dataset in resp]

    async def get_resource_fields(self, resource_id: str) -> list[ResourceField]:
        url = f"{self._ckan_endpoint}/api/action/datastore_info"
        payload = {"id": resource_id}
        resp = await self._post_request(url, payload)
        if self.is_response_successful(resp) and resp.get("result").get("fields"):
            return [ResourceField(field) for field in resp["result"]["fields"]]
        return []
