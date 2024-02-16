from typing import List, Optional, Tuple, Type
from functools import cached_property

from odd_collector.domain.plugin import SnowflakePlugin
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import Generator, SnowflakeGenerator

from .client import SnowflakeClient, SnowflakeClientBase
from .domain import Pipe, Table, View, RawPipe, RawStage, ImportedKey
from .map import map_database, map_pipe, map_schemas, map_table, map_view


class Adapter(BaseAdapter):
    config: SnowflakePlugin
    generator: SnowflakeGenerator

    def __init__(self, config: SnowflakePlugin) -> None:
        self._database_name = config.database.upper()
        super().__init__(config)
        self._metadata = self._get_database_metadata()

    def create_generator(self) -> Generator:
        return SnowflakeGenerator(
            host_settings=self.config.host,
            databases=self._database_name,
        )

    def _get_database_metadata(self) -> dict[str, list]:
        with SnowflakeClient(self.config) as client:
            return {
                "tables": client.get_tables(),
                "raw_pipes": client.get_raw_pipes(),
                "raw_stages": client.get_raw_stages(),
                "imported_keys": client.get_imported_keys(),
            }

    @cached_property
    def _tables(self) -> list[Table]:
        return self._metadata["tables"]

    @cached_property
    def _raw_pipes(self) -> list[RawPipe]:
        return self._metadata["raw_pipes"]

    @cached_property
    def _raw_stages(self) -> list[RawStage]:
        return self._metadata["raw_stages"]


    @cached_property
    def _imported_keys(self) -> list[ImportedKey]:
        return self._metadata["imported_keys"]

    @cached_property
    def _pipe_entities(self) -> list[DataEntity]:
        pipes: List[Pipe] = []
        for raw_pipe in self._raw_pipes:
            pipes.extend(
                Pipe(
                    name=raw_pipe.pipe_name,
                    definition=raw_pipe.definition,
                    stage_url=raw_stage.stage_url,
                    stage_type=raw_stage.stage_type,
                    downstream=raw_pipe.downstream,
                )
                for raw_stage in self._raw_stages
                if raw_pipe.stage_full_name == raw_stage.stage_full_name
            )
        return [map_pipe(pipe, self.generator) for pipe in pipes]

    @cached_property
    def _table_entities(self) -> list[tuple[Table, DataEntity]]:
        return self._get_tables_entities(self._tables)

    @cached_property
    def _schema_entities(self) -> list[DataEntity]:
        return self._get_schemas_entities(self._table_entities)

    @cached_property
    def _database_entity(self) -> DataEntity:
        return self._get_database_entity(self._schema_entities)

    def get_data_entity_list(self) -> DataEntityList:
        try:
            imported_keys = self._metadata["imported_keys"]
            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[
                    *[te[1] for te in self._table_entities],
                    *self._schema_entities,
                    self._database_entity,
                    *self._pipe_entities,
                ]
            )
        except Exception as e:
            raise MappingDataError("Error during mapping") from e

    def _get_tables_entities(
        self, tables: List[Table]
    ) -> List[Tuple[Table, DataEntity]]:
        result = []

        for table in tables:
            if isinstance(table, View):
                result.append((table, map_view(table, self.generator)))
            else:
                result.append((table, map_table(table, self.generator)))

        return result

    def _get_schemas_entities(
        self, tables_with_entities: List[Tuple[Table, DataEntity]]
    ) -> List[DataEntity]:
        return map_schemas(tables_with_entities, self.generator)

    def _get_database_entity(self, schemas: List[DataEntity]) -> DataEntity:
        return map_database(self._database_name, schemas, self.generator)
