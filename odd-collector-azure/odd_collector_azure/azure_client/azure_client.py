from asyncio import gather
from typing import Any, Dict, List, NamedTuple, Optional

from aiohttp import ClientSession

from odd_collector_azure.domain.plugin import AzurePlugin


class RequestArgs(NamedTuple):
    method: str
    url: str
    params: Optional[Dict[Any, Any]] = None
    headers: Optional[Dict[Any, Any]] = None
    payload: Optional[Dict[Any, Any]] = None


class AzureClient:
    def __init__(self, config: AzurePlugin, resource: str):
        self.resource = resource
        self.__config = config

    async def __get_access_token(self) -> str:
        payload = {
            "grant_type": "password",
            "scope": "openid",
            "resource": self.resource,
            "client_id": self.__config.client_id,
            "client_secret": self.__config.client_secret,
            "username": self.__config.username,
            "password": self.__config.password,
        }
        async with ClientSession() as session:
            response = await self.fetch_async_response(
                session,
                RequestArgs(
                    method="POST",
                    url="https://login.microsoftonline.com/common/oauth2/token",
                    payload=payload,
                ),
            )
            return response.get("access_token")

    async def build_headers(self) -> Dict[str, str]:
        return {"Authorization": "Bearer " + await self.__get_access_token()}

    @staticmethod
    async def fetch_async_response(
        session, request_args: RequestArgs
    ) -> Dict[Any, Any]:
        async with session.request(
            request_args.method,
            url=request_args.url,
            params=request_args.params,
            headers=request_args.headers,
            data=request_args.payload,
        ) as response:
            return await response.json()

    async def fetch_all_async_responses(self, request_args_list: List[RequestArgs]):
        async with ClientSession() as session:
            return await gather(
                *[
                    self.fetch_async_response(session, request_args=request_args)
                    for request_args in request_args_list
                ],
                return_exceptions=True,
            )
