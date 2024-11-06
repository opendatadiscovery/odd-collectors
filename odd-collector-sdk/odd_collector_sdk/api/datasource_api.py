import json
import sys
from datetime import timedelta
from timeit import default_timer as timer

from aiohttp import ClientSession, ClientTimeout
from odd_models.models import DataEntityList, DataSourceList

from ..logger import logger


class PlatformApi:
    def __init__(
        self,
        token: str,
        platform_url: str,
        connection_timeout_seconds: int = 300,
        verify_ssl: bool = False,
    ) -> None:
        self.platform_url = platform_url
        self.headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        self.verify_ssl = verify_ssl
        self.timeout = ClientTimeout(total=connection_timeout_seconds)

    async def register_datasource(self, data_source_list: DataSourceList):
        async with ClientSession(timeout=self.timeout) as session:
            try:
                response = await session.post(
                    url=f"{self.platform_url}/ingestion/datasources",
                    data=data_source_list.json(),
                    headers=self.headers,
                    ssl=self.verify_ssl,
                )
                response.raise_for_status()
                return response
            except Exception as e:
                response = response if "response" in locals() else None
                raise Exception(self._get_exception_message(e, response))

    async def ingest_data(self, data_entity_list: DataEntityList):
        async with ClientSession(timeout=self.timeout) as session:
            try:
                json_start = timer()
                data = data_entity_list.json()
                logger.debug(
                    f"Encoded size: {sys.getsizeof(data)/(1024*1024):.3f} MB, took: {timedelta(seconds=timer() - json_start)}"
                )

                ingest_start = timer()
                response = await session.post(
                    url=f"{self.platform_url}/ingestion/entities",
                    data=data,
                    headers=self.headers,
                    ssl=self.verify_ssl,
                )
                response.raise_for_status()
                ingest_end = timer()

                logger.debug(
                    f"Ingestion to platform took {timedelta(seconds=ingest_end - ingest_start)}"
                )
                return response
            except Exception as e:
                response = response if "response" in locals() else None
                raise Exception(self._get_exception_message(e, response))

    @staticmethod
    def _get_exception_message(e, response):
        if response:
            platform_response = json.loads(str(response.content._buffer[0].decode("utf-8")))
            error_msg = f"Platform response: {platform_response}.\n Exception message: {str(e)}"
            return error_msg
        error_msg = (
            "No response from platform has been sent. "
            "Possible reasons: platform is not running, incorrect <platform_host_url> configuration."
        )
        return error_msg
