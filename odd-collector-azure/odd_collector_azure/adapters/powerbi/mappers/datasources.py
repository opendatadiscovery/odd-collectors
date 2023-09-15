from typing import Any, Dict, List, Type

from odd_models.models import DataEntity, DataEntityType
from oddrn_generator.utils.external_generators import (
    ExternalDbSettings,
    ExternalGeneratorBuilder,
    ExternalMssqlGenerator,
    ExternalPostgresGenerator,
)


class PowerBiExternalGeneratorBuilder(ExternalGeneratorBuilder):
    def __init__(self, datasource_node: Dict[str, Any]):
        self.connection_details: Dict[str, str] = datasource_node.get(
            "connectionDetails"
        )

    host_key: str
    db_name_key: str

    def build_db_settings(self) -> ExternalDbSettings:
        return ExternalDbSettings(
            host=self.connection_details[self.host_key],
            database_name=self.connection_details[self.db_name_key],
        )


class JdbcEngine(PowerBiExternalGeneratorBuilder):
    host_key = "server"
    db_name_key = "database"


class OdbcEngine(PowerBiExternalGeneratorBuilder):
    host_key = "connectionString"
    db_name_key = "connectionString"
    type = "ODBC"
    external_generator = ExternalMssqlGenerator


class MssqlEngine(JdbcEngine):
    type = "Sql"
    external_generator = ExternalMssqlGenerator


class PostgresSqlEngine(JdbcEngine):
    type = "PostgreSql"
    external_generator = ExternalPostgresGenerator


datasources: List[Type[PowerBiExternalGeneratorBuilder]] = [
    MssqlEngine,
    PostgresSqlEngine,
    OdbcEngine,
]
datasources_factory: Dict[str, Type[PowerBiExternalGeneratorBuilder]] = {
    datasource.type: datasource for datasource in datasources
}


def map_datasource(builder: PowerBiExternalGeneratorBuilder):
    external_generator = builder.get_external_generator()
    db_name = builder.connection_details[builder.db_name_key]
    oddrn = external_generator.get_generator_for_database_lvl().get_oddrn_by_path(
        external_generator.database_path_name
    )
    return DataEntity(name=db_name, oddrn=oddrn, type=DataEntityType.DATABASE_SERVICE)
