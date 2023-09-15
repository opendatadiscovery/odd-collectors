from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import GCSGenerator

from odd_collector_gcp.utils.parse_gcs_url import parse_gcs_url
from ..models.field import DField

from ..models.table import DTable
from .field import map_field
from .metadata import map_metadata


def map_delta_table(generator: GCSGenerator, delta_table: DTable) -> DataEntity:
    bucket, key = parse_gcs_url(delta_table.table_uri)
    fields = [DField(field) for field in delta_table.schema.fields]
    generator.set_oddrn_paths(buckets=bucket, keys=key)

    field_list = []
    for field in fields:
        processed_ds_fields = map_field(generator, field)
        field_list.extend(processed_ds_fields)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("keys"),
        name=key,
        type=DataEntityType.TABLE,
        created_at=delta_table.created_at,
        updated_at=delta_table.updated_at,
        owner=None,
        metadata=map_metadata(delta_table),
        dataset=DataSet(rows_number=delta_table.num_rows, field_list=field_list),
    )
