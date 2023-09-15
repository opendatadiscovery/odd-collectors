from odd_collector_sdk.utils.metadata import (
    DefinitionType,
    MetadataExtension,
    extract_metadata,
)

from ..logger import logger
from ..models.table import DTable


def map_metadata(table: DTable) -> list[MetadataExtension]:
    try:
        return [
            extract_metadata(
                datasource="gcs",
                entity=table,
                definition=DefinitionType.DATASET,
            )
        ]
    except Exception as e:
        logger.error("Failed to extract metadata from, error: %s", e)
        return []
