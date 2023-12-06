import sys
from datetime import timedelta
from timeit import default_timer as timer

from odd_models.models import DataEntityList, DataSourceList

from ..errors import IngestionDataError, PlatformApiError, RegisterDataSourceError
from ..logger import logger
from .http_client import HttpClient


class PlatformApi:
    def __init__(self, http_client: HttpClient, platform_url: str) -> None:
        self.__client = http_client
        self.__platform_url = platform_url

    async def register_datasource(self, requests: DataSourceList):
        try:
            return await self.__client.post(
                f"{self.__platform_url}/ingestion/datasources", requests.json()
            )
        except PlatformApiError as e:
            raise RegisterDataSourceError(e.response, requests) from e
        except Exception:
            raise

    async def ingest_data(self, data_entity_list: DataEntityList):
        try:
            json_start = timer()
            data = data_entity_list.json()
            logger.debug(
                f"Encoded size: {sys.getsizeof(data)/(1024*1024):.3f} MB, took: {timedelta(seconds=timer()  - json_start )}"
            )

            ingest_start = timer()
            response = await self.__client.post(
                f"{self.__platform_url}/ingestion/entities",
                data,
            )
            response.raise_for_status()
            ingest_end = timer()
            logger.debug(
                f"Ingestion to platform took {timedelta(seconds=ingest_end  - ingest_start )}"
            )
            return response
        except PlatformApiError as e:
            raise IngestionDataError(e.response, data_entity_list) from e
        except Exception:
            raise
