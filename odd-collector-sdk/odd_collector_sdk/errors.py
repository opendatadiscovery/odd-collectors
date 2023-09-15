from typing import Optional

from odd_models.models import DataEntityList, DataSourceList


class DataSourceError(Exception):
    pass


class DataSourceAuthorizationError(DataSourceError):
    pass


class DataSourceConnectionError(DataSourceError):
    pass


class MappingDataError(Exception):
    pass


class LoadConfigError(Exception):
    def __init__(self, original_error, *args: object) -> None:
        super().__init__(f"Couldn't handle config. Reason {original_error}", *args)


class PlatformApiError(Exception):
    response: str

    def __init__(self, response) -> None:
        self.response = response
        super().__init__(self.message)

    @property
    def message(self):
        return f"Platform API error.\n {self.response}"

    @property
    def request(self) -> Optional[str]:
        return None


class IngestionDataError(PlatformApiError):
    data_entity_list: DataEntityList

    @property
    def message(self):
        return f"Could not ingest data. Reason: {self.response}"

    def __init__(self, response, data_entity_list: DataEntityList) -> None:
        super().__init__(response)
        self.data_entity_list = data_entity_list


class RegisterDataSourceError(PlatformApiError):
    data_source_list: DataSourceList

    def __init__(self, response, data_source_list: DataSourceList) -> None:
        super().__init__(response)
        self.data_source_list = data_source_list

    @property
    def message(self):
        return f"Could not create data sources. Reason: {self.response}"

    @property
    def request(self) -> Optional[str]:
        return self.data_source_list.json()


class ExtractMetadataError(Exception):
    pass
