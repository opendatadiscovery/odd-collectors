from collections import defaultdict

from odd_collector.adapters.postgresql.models import Relationship, Schema, Table
from odd_collector.domain.plugin import PostgreSQLPlugin
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models import DataEntity
from odd_models.models import DataEntityList
from oddrn_generator import PostgresqlGenerator

from .logger import logger
from .mappers.database import map_database
from .mappers.relationships import map_relationship
from .mappers.schemas import map_schema
from .mappers.tables import map_tables
from .repository import ConnectionParams, PostgreSQLRepository


class Adapter(BaseAdapter):
    config: PostgreSQLPlugin
    generator: PostgresqlGenerator

    def __init__(self, config: PostgreSQLPlugin) -> None:
        super().__init__(config)
        self._cashed_query_results = self._get_query_results_from_repository()

    def _get_query_results_from_repository(self) -> dict[str, list]:
        with PostgreSQLRepository(
            ConnectionParams.from_config(self.config), self.config.schemas_filter
        ) as repo:
            return {
                "schemas_query": repo.get_schemas(),
                "tables_query": repo.get_tables(),
                "relationships_query": repo.get_relationships(),
            }

    def _get_schemas(self) -> list[Schema]:
        return self._cashed_query_results["schemas_query"]

    def _get_tables(self) -> list[Table]:
        return self._cashed_query_results["tables_query"]

    def _get_relationships(self) -> list[Relationship]:
        return self._cashed_query_results["relationships_query"]

    def create_generator(self) -> PostgresqlGenerator:
        return PostgresqlGenerator(
            host_settings=self.config.host, databases=self.config.database
        )

    def _map_entities(self) -> dict:
        schema_entities: list[DataEntity] = []

        all_table_entities: dict[str, DataEntity] = {}

        self.generator.set_oddrn_paths(**{"databases": self.config.database})

        tables_by_schema = defaultdict(list)
        for table in self._get_tables():
            tables_by_schema[table.table_schema].append(table)

        for schema in self._get_schemas():
            tables_per_schema: list[Table] = tables_by_schema.get(
                schema.schema_name, []
            )
            table_entities_tmp = map_tables(self.generator, tables_per_schema)
            schema_entities.append(
                map_schema(self.generator, schema, list(table_entities_tmp.values()))
            )
            all_table_entities |= table_entities_tmp

        database_entity = map_database(
            self.generator, self.config.database, schema_entities
        )

        create_lineage(self._get_tables(), all_table_entities)

        relationship_entities = []
        for relationship in self._get_relationships():
            relationship_entities.append(
                map_relationship(self.generator, relationship, all_table_entities)
            )

        return {
            "database_entity": database_entity,
            "schema_entities": schema_entities,
            "table_entities": all_table_entities.values(),
            "relationship_entities": relationship_entities,
        }

    def get_data_entity_list(self) -> DataEntityList:
        mapped_entities = self._map_entities()

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[
                *mapped_entities["table_entities"],
                *mapped_entities["relationship_entities"],
                *mapped_entities["schema_entities"],
                mapped_entities["database_entity"],
            ],
        )


def create_lineage(tables: list[Table], data_entities: dict[str, DataEntity]) -> None:
    views = [table for table in tables if table.table_type in ("v", "m")]

    for view in views:
        try:
            depending_entity = data_entities.get(view.as_dependency.uid)

            if depending_entity.data_transformer is None:
                continue

            for dependency in view.dependencies:
                if dependency_entity := data_entities.get(dependency.uid):
                    if (
                        dependency_entity.oddrn
                        not in depending_entity.data_transformer.inputs
                    ):
                        depending_entity.data_transformer.inputs.append(
                            dependency_entity.oddrn
                        )
        except Exception as e:
            logger.warning(f"Error creating lineage for {view.table_name} {e=}")
