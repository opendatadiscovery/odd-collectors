from typing import Any, Dict

from odd_models.models import DataConsumer, DataEntity, DataEntityType
from oddrn_generator import QuicksightGenerator

from . import metadata_extractor


def map_quicksight_analysis(
    raw_table_data: Dict[str, Any], account_id: str, region_name: str
) -> DataEntity:
    oddrn_gen = QuicksightGenerator(
        cloud_settings={"account": account_id, "region": region_name},
        analyses=raw_table_data["AnalysisId"],
    )
    dataset_ids = [k.split("/")[-1] for k in raw_table_data["DataSetArns"]]
    consumers = []
    for id in dataset_ids:
        oddrn_gen.set_oddrn_paths(datasets=id)
        consumers.append(oddrn_gen.get_oddrn_by_path("datasets"))

    return DataEntity(
        oddrn=oddrn_gen.get_oddrn_by_path("analyses"),
        name=raw_table_data["Name"],
        type=DataEntityType.UNKNOWN,
        created_at=raw_table_data["CreatedTime"].isoformat(),
        updated_at=raw_table_data["LastUpdatedTime"].isoformat(),
        metadata=[metadata_extractor.extract_analysis_metadata(raw_table_data)],
        data_consumer=DataConsumer(inputs=consumers),
    )
