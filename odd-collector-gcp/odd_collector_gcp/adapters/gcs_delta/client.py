import traceback as tb
from typing import Any, Iterable, Optional

from deltalake import DeltaTable
from funcy import last, partial, silent, walk

from odd_collector_gcp.domain.plugin import DeltaTableConfig, GCSDeltaPlugin
from odd_collector_gcp.filesystem.pyarrow_fs import FileSystem
from ..gcs.domain.parameters import GCSAdapterParams

from ...utils.dates import add_utc_timezone, from_ms
from .logger import logger
from .models.table import DTable
from ...utils.remove_gcs_protocol import remove_protocol


def handle_values(
    obj: dict, handler: tuple[str, callable]
) -> tuple[str, Optional[any]]:
    key, callback = handler
    return key, silent(callback)(obj.get(key))


class IsNotDeltaTable(Exception):
    ...


class DeltaClient:
    def __init__(self, config: GCSDeltaPlugin) -> None:
        self.storage_options: GCSAdapterParams = config.parameters
        self.fs = FileSystem(config.parameters)

    def load_delta_table(self, delta_table_config: DeltaTableConfig) -> DeltaTable:
        storage_options = self.storage_options.dict() if self.storage_options else None
        try:
            return DeltaTable(
                delta_table_config.path,
                storage_options=storage_options,
            )
        except Exception as e:
            logger.error(f"Error message: {e}")
            raise IsNotDeltaTable() from e

    def handle_folder(self, config: DeltaTableConfig) -> Iterable[DTable]:
        logger.debug(f"Getting delta tables from folder {config.path}")

        objects = self.fs.get_file_info(remove_protocol(config.path))

        folders = filter(lambda obj: not obj.is_file, objects)
        allowed = filter(lambda folder: folder.base_name, folders)
        filtered = filter(lambda item: config.allow(item.path), allowed)

        for obj in filtered:
            new_config = config.append_prefix(obj.base_name)
            yield from self.get_table(new_config)

    def get_table(self, delta_table_config: DeltaTableConfig) -> Iterable[DTable]:
        # sourcery skip: raise-specific-error
        try:
            logger.debug(f"Getting delta table {delta_table_config.path}")
            table = self.load_delta_table(delta_table_config)

            metadata = get_metadata(table)

            yield DTable(
                table_uri=table.table_uri,
                schema=table.schema(),
                num_rows=metadata.get("num_records"),
                metadata=metadata,
                created_at=silent(from_ms)(metadata.get("created_time")),
                updated_at=silent(add_utc_timezone)(metadata.get("modification_time")),
            )
        except IsNotDeltaTable:
            logger.warning(
                f"Path {delta_table_config.path} is not a delta table. Searching for"
                " delta tables in subfolders."
            )
            yield from self.handle_folder(delta_table_config)
        except Exception as e:
            raise Exception(
                f"Failed to get delta table {delta_table_config.path}. {e}"
            ) from e


def get_metadata(table: DeltaTable) -> dict[str, Any]:
    metadata = {}

    try:
        logger.debug(f"Getting actions list for {table.table_uri}")
        actions = table.get_add_actions(flatten=True).to_pydict()

        metadata |= walk(
            partial(handle_values, actions),
            {"size_bytes": sum, "num_records": sum, "modification_time": last},
        )
    except Exception as e:
        logger.error(f"Failed to get actions list for {table.table_uri}")

    try:
        logger.debug(f"Getting metadata for {table.table_uri}")
        delta_metadata = table.metadata()
        metadata |= {
            "id": delta_metadata.id,
            "name": delta_metadata.name,
            "description": delta_metadata.description,
            "partition_columns": ",".join(delta_metadata.partition_columns),
            "configuration": delta_metadata.configuration,
            "created_time": delta_metadata.created_time,
        }
    except Exception as e:
        logger.debug(tb.format_exc())
        logger.error(f"Failed to get metadata for {table.table_uri}. {e}")

    return metadata
