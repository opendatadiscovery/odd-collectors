import traceback as tb
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Optional

from deltalake import DeltaTable
from funcy import complement, isnone, last, partial, select_values, silent, walk

from odd_collector_aws.domain.plugin import DeltaTableConfig, S3DeltaPlugin
from odd_collector_aws.filesystem.pyarrow_fs import FileSystem

from ...utils.dates import add_utc_timezone, from_ms
from ...utils.remove_s3_protocol import remove_protocol
from .logger import logger
from .models.table import DTable


def handle_values(
    obj: dict, handler: tuple[str, callable]
) -> tuple[str, Optional[any]]:
    key, callback = handler
    return key, silent(callback)(obj.get(key))


@dataclass
class StorageOptions:
    DEFAULT_REGION = "us-east-1"

    aws_access_key_id: str = None
    aws_secret_access_key: str = None
    aws_region: str = None
    aws_session_token: str = None
    aws_storage_allow_http: str = None
    endpoint_url: str = None
    aws_profile: str = None
    aws_role_session_name: str = None

    @classmethod
    def from_config(cls, config: S3DeltaPlugin) -> "StorageOptions":
        return cls(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            aws_region=config.aws_region or cls.DEFAULT_REGION,
            aws_session_token=config.aws_session_token,
            endpoint_url=config.endpoint_url,
            aws_storage_allow_http="true" if config.aws_storage_allow_http else None,
            aws_profile=config.profile_name,
            aws_role_session_name=config.aws_role_session_name,
        )

    def to_dict(self) -> dict[str, str]:
        return select_values(complement(isnone), asdict(self))


class IsNotDeltaTable(Exception):
    ...


class DeltaClient:
    def __init__(self, config: S3DeltaPlugin) -> None:
        self.storage_options: StorageOptions = StorageOptions.from_config(config)
        self.fs = FileSystem(config)

    def load_delta_table(self, delta_table_config: DeltaTableConfig) -> DeltaTable:
        try:
            return DeltaTable(
                delta_table_config.path,
                storage_options=self.storage_options.to_dict(),
            )
        except Exception as e:
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
