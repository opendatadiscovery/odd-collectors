from cachetools.func import ttl_cache
from odd_collector.domain.plugin import SnowflakePlugin
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import Generator, SnowflakeGenerator

from .client import SnowflakeClient
from .domain import (
    ForeignKeyConstraint,
    Pipe,
    RawPipe,
    RawStage,
    Table,
    UniqueConstraint,
    View,
)
from .mappers import map_database, map_pipe, map_schemas, map_table, map_view
from .mappers.relationships import DataEntityRelationshipsMapper


class Adapter(BaseAdapter):
    config: SnowflakePlugin
    generator: SnowflakeGenerator
    # TODO: to connect CACHE_TTL with default_pulling_interval
    #  (requires changes in odd-collector-sdk, might affect other adapters)
    CACHE_TTL = 60  # in seconds, after this time cache of _get_metadata() clears

    def __init__(self, config: SnowflakePlugin) -> None:
        self._database_name = config.database.upper()
        super().__init__(config)

    def create_generator(self) -> Generator:
        return SnowflakeGenerator(
            host_settings=self.config.host,
            databases=self._database_name,
        )

    # We use temporary cache to avoid multiple method executions (each produces a connection with queries
    # invocations all the time). So in one collector run we use this method once, after ttl cache clears and the next
    # scheduled run will rerun method with all querying to get the latest schemas updates.
    @ttl_cache(ttl=CACHE_TTL)
    def _get_metadata(self) -> dict[str, list]:
        with SnowflakeClient(self.config) as client:
            return {
                "tables": client.get_tables(),
                "raw_pipes": client.get_raw_pipes(),
                "raw_stages": client.get_raw_stages(),
                "fk_constraints": client.get_fk_constraints(),
                "unique_constraints": client.get_unique_constraints(),
            }

    @property
    def _tables(self) -> list[Table]:
        return self._get_metadata()["tables"]

    @property
    def _raw_pipes(self) -> list[RawPipe]:
        return self._get_metadata()["raw_pipes"]

    @property
    def _raw_stages(self) -> list[RawStage]:
        return self._get_metadata()["raw_stages"]

    @property
    def _fk_constraints(self) -> list[ForeignKeyConstraint]:
        return self._get_metadata()["fk_constraints"]

    @property
    def _unique_constraints(self) -> list[UniqueConstraint]:
        return self._get_metadata()["unique_constraints"]

    @property
    def _pipe_entities(self) -> list[tuple[Pipe, DataEntity]]:
        pipes: list[Pipe] = []
        for raw_pipe in self._raw_pipes:
            pipes.extend(
                Pipe(
                    catalog=raw_pipe.pipe_catalog,
                    schema_name=raw_pipe.pipe_schema,
                    name=raw_pipe.pipe_name,
                    definition=raw_pipe.definition,
                    stage_url=raw_stage.stage_url,
                    stage_type=raw_stage.stage_type,
                    downstream=raw_pipe.downstream,
                )
                for raw_stage in self._raw_stages
                if raw_pipe.stage_full_name == raw_stage.stage_full_name
            )
        return [(pipe, map_pipe(pipe, self.generator)) for pipe in pipes]

    @property
    def _table_entities(self) -> list[tuple[Table, DataEntity]]:
        result = []

        for table in self._tables:
            if isinstance(table, View):
                result.append((table, map_view(table, self.generator)))
            else:
                result.append((table, map_table(table, self.generator)))
        return result

    @property
    def _relationship_entities(self) -> list[DataEntity]:
        return DataEntityRelationshipsMapper(
            oddrn_generator=self.generator,
            unique_constraints=self._unique_constraints,
            table_entities_pair=self._table_entities,
        ).map(self._fk_constraints)

    @property
    def _schema_entities(self) -> list[DataEntity]:
        return map_schemas(self._table_entities, self._pipe_entities, self.generator)

    @property
    def _database_entity(self) -> DataEntity:
        return map_database(self._database_name, self._schema_entities, self.generator)

    def get_data_entity_list(self) -> DataEntityList:
        try:
            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[
                    *[te[1] for te in self._table_entities],
                    *self._schema_entities,
                    self._database_entity,
                    *[pe[1] for pe in self._pipe_entities],
                    *self._relationship_entities,
                ],
            )
        except Exception as e:
            raise MappingDataError("Error during mapping") from e
