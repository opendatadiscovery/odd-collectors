import copy
from collections import defaultdict

from cachetools.func import ttl_cache
from odd_collector.adapters.postgresql.models import (
    ForeignKeyConstraint,
    Schema,
    Table,
    UniqueConstraint,
)
from odd_collector.domain.plugin import PostgreSQLPlugin
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models import DataEntity
from odd_models.models import DataEntityList
from oddrn_generator import PostgresqlGenerator

from .logger import logger
from .mappers.database import map_database
from .mappers.relationships import DataEntityRelationshipsMapper
from .mappers.schemas import map_schema
from .mappers.tables import map_tables
from .repository import ConnectionParams, PostgreSQLRepository
from .utils import filter_views


class Adapter(BaseAdapter):
    config: PostgreSQLPlugin
    generator: PostgresqlGenerator
    # TODO: to connect CACHE_TTL with default_pulling_interval
    #  (requires changes in odd-collector-sdk, might affect other adapters)
    CACHE_TTL = 60  # in seconds, after this time cache of _get_metadata() clears

    def __init__(self, config: PostgreSQLPlugin) -> None:
        super().__init__(config)
        self.generator.set_oddrn_paths(databases=self.config.database)

    def create_generator(self) -> PostgresqlGenerator:
        return PostgresqlGenerator(
            host_settings=self.config.host, databases=self.config.database
        )

    # We use temporary cache to avoid multiple (10) method executions (each produces a connection with queries
    # invocations all the time). So in one collector run we use this method once, after ttl cache clears and the next
    # scheduled run will rerun method with all querying to get the latest schemas updates.
    @ttl_cache(ttl=CACHE_TTL)
    def _get_metadata(self) -> dict[str, list]:
        with PostgreSQLRepository(
            ConnectionParams.from_config(self.config), self.config.schemas_filter
        ) as repo:
            return {
                "schemas": repo.get_schemas(),
                "tables": repo.get_tables(),
                "fk_constraints": repo.get_foreign_key_constraints(),
                "unique_constraints": repo.get_unique_constraints(),
            }

    @property
    def _schemas(self) -> list[Schema]:
        return self._get_metadata()["schemas"]

    @property
    def _tables(self) -> list[Table]:
        return self._get_metadata()["tables"]

    @property
    def _fk_constraints(self) -> list[ForeignKeyConstraint]:
        return self._get_metadata()["fk_constraints"]

    @property
    def _unique_constraints(self) -> list[UniqueConstraint]:
        return self._get_metadata()["unique_constraints"]

    @property
    def _table_entities(self) -> dict[str, DataEntity]:
        return map_tables(self.generator, self._tables)

    @property
    def _schema_entities(self) -> list[DataEntity]:
        result = []
        table_entities_by_schema = self._group_table_entities_by_schema()
        for schema in self._schemas:
            table_entities = table_entities_by_schema.get(schema.schema_name, [])
            result.append(map_schema(self.generator, schema, table_entities))
        return result

    @property
    def _relationship_entities(self) -> list[DataEntity]:
        return DataEntityRelationshipsMapper(
            oddrn_generator=self.generator,
            unique_constraints=self._unique_constraints,
            datasets=self._table_entities,
        ).map(self._fk_constraints)

    @property
    def _database_entity(self) -> DataEntity:
        return map_database(self.generator, self.config.database, self._schema_entities)

    def _group_table_entities_by_schema(self) -> dict[str, list[DataEntity]]:
        result = defaultdict(list)
        for dependency, table_entity in self._table_entities.items():
            schema_name = dependency.split(".")[0]
            result[schema_name].append(table_entity)
        return result

    def get_data_entity_list(self) -> DataEntityList:
        res_table_entities = create_lineage(self._tables, self._table_entities)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[
                *res_table_entities.values(),
                *self._schema_entities,
                *self._relationship_entities,
                self._database_entity,
            ],
        )


def create_lineage(
    tables: list[Table], data_entities: dict[str, DataEntity]
) -> dict[str, DataEntity]:
    res_entities = copy.deepcopy(data_entities)

    views = filter_views(tables)

    for view in views:
        entity = res_entities.get(view.as_dependency.uid)
        if not entity or not entity.data_transformer:
            continue

        try:
            for dependency in view.dependencies:
                dependency_entity = res_entities.get(dependency.uid)
                if dependency_entity is None:
                    continue
                if dependency_entity.oddrn not in entity.data_transformer.inputs:
                    entity.data_transformer.inputs.append(dependency_entity.oddrn)
        except Exception as e:
            logger.warning(f"Error creating lineage for {view.table_name} {e=}")
    return res_entities
