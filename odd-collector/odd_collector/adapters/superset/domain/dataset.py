from dataclasses import dataclass, field
from typing import Any, Optional

from odd_collector.adapters.superset.domain.column import Column
from odd_models.models import MetadataExtension
from oddrn_generator import (
    MssqlGenerator,
    MysqlGenerator,
    PostgresqlGenerator,
    SQLiteGenerator,
)
from sqlalchemy.engine.url import make_url

from ..logger import logger
from .database import Database


@dataclass
class Dataset:
    id: int
    name: str
    database: Database
    kind: str
    schema: str
    columns: Optional[list[Column]] = field(default_factory=list)
    metadata: Optional[list[MetadataExtension]] = None
    owner: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, dataset: Any):
        return cls(
            id=dataset["id"],
            metadata=[],
            description=dataset.get("description"),
            name=dataset.get("table_name"),
            database=dataset.get("database"),
            kind=dataset.get("kind"),
            schema=dataset.get("schema"),
        )


def _connection(database) -> tuple[Optional[str], Optional[str]]:
    """Host and database name for a Superset connection.

    ``parameters`` is only populated for connections created through Superset's
    dynamic form. One added as a plain SQLAlchemy URI -- what the API produces
    -- leaves it empty, so fall back to the URI.
    """
    params = database.parameters or {}
    host, name = params.get("host"), params.get("database")
    if not host or not name:
        url = make_url(database.sqlalchemy_uri)
        host, name = host or url.host, name or url.database
    return host, name


class _SqlGeneratorAdaptee:
    """Resolve a dataset to the ODDRN of the table behind it.

    Subclasses only differ by generator, and each one must mint the identifier
    that source's own collector mints, or the chart points at a catalog object
    that does not exist.
    """

    generator_cls = None

    def get_dataset_oddrn(self, dataset: "Dataset") -> Optional[str]:
        try:
            database = dataset.database
            host, name = _connection(database)
            params = {
                "host_settings": host,
                "databases": name,
                "schemas": dataset.schema,
            }
            dataset_type = database.schemas[dataset.schema].tables[dataset.name].type
            if dataset_type not in ["view", "table"]:
                logger.warning(f"Dataset type {dataset_type} is not supported")
                return None
            dataset_type = "views" if dataset_type == "view" else "tables"
            params[dataset_type] = dataset.name
            return self.generator_cls(**params).get_oddrn_by_path(dataset_type)
        except Exception as e:
            logger.warning(f"Failed to generate dataset oddrn: {e}")
            return None


class PostgresGeneratorAdaptee(_SqlGeneratorAdaptee):
    generator_cls = PostgresqlGenerator


class SqliteGeneratorAdaptee:
    def get_dataset_oddrn(self, dataset: Dataset) -> Optional[str]:
        try:
            database = dataset.database
            if not dataset.database.sqlalchemy_uri:
                raise AttributeError("Sqlite database uri must be set")

            table_schema = dataset.schema
            table_name = dataset.name

            url = make_url(database.sqlalchemy_uri)
            params = {
                "path": url.database,
            }
            dataset_type = database.schemas[table_schema].tables[table_name].type
            if dataset_type not in ["view", "table"]:
                logger.warning(f"Dataset type {dataset_type} is not supported")
                return None

            dataset_type = "views" if dataset_type == "view" else "tables"
            params[dataset_type] = table_name

            generator = SQLiteGenerator(**params)

            return generator.get_oddrn_by_path(dataset_type)
        except Exception as e:
            logger.warning(f"Failed to generate dataset oddrn: {e}")
            return None


class MssqlGeneratorAdaptee(_SqlGeneratorAdaptee):
    generator_cls = MssqlGenerator


class MysqlGeneratorAdaptee(_SqlGeneratorAdaptee):
    generator_cls = MysqlGenerator


SUPPORTED_BACKENDS = {
    "postgresql": PostgresGeneratorAdaptee,
    "sqlite": SqliteGeneratorAdaptee,
    "mssql": MssqlGeneratorAdaptee,
    "mysql": MysqlGeneratorAdaptee,
}


def create_dataset_oddrn(dataset: Dataset) -> Optional[str]:
    database = dataset.database
    backend = SUPPORTED_BACKENDS.get(database.backend)

    if not backend:
        logger.warning(
            f"Database backend {database.backend} is not supported for generating dataset oddrn"
        )
        return None

    return backend().get_dataset_oddrn(dataset)
