from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityType, DataTransformerRun
from oddrn_generator import AzureDataFactoryGenerator

from odd_collector_azure.adapters.azure_data_factory.domain import ADFActivityRun

from ..utils import ADFMetadataEncoder


def map_activity_run(
    oddrn_generator: AzureDataFactoryGenerator,
    run: ADFActivityRun,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("activities_runs", run.id),
        name=run.id,
        type=DataEntityType.JOB_RUN,
        metadata=[
            extract_metadata(
                "azure_data_factory",
                run,
                DefinitionType.DATASET,
                jsonify=True,
                flatten=True,
                json_encoder=ADFMetadataEncoder,
            )
        ],
        data_transformer_run=DataTransformerRun(
            start_time=run.start_time,
            end_time=run.end_time,
            transformer_oddrn=oddrn_generator.get_oddrn_by_path(
                "activities", run.activity_name
            ),
            status=run.status,
        ),
    )
