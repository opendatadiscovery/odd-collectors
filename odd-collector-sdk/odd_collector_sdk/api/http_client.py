from typing import Any

from aiohttp import ClientSession, ClientTimeout, TCPConnector

from odd_collector_sdk.errors import PlatformApiError


class HttpClient:
    def __init__(
        self,
        token: str,
        connection_timeout_seconds: int = 300,
        verify_ssl: bool = False,
    ) -> None:
        self.headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        self.verify_ssl = verify_ssl
        self.timeout = ClientTimeout(total=connection_timeout_seconds)

    async def post(self, url: str, data: Any, **kwargs):
        async with ClientSession(
            connector=TCPConnector(verify_ssl=self.verify_ssl)
        ) as session:
            try:
                response = await session.post(
                    url=url, data=data, headers=self.headers, **kwargs
                )

                response.raise_for_status()
                return response
            except Exception as e:
                raise PlatformApiError(e) from e
