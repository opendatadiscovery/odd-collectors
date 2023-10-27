from odd_collector_azure.adapters.azure_data_factory.domain import ADFPipelineRun
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityType, DataTransformerRun
from oddrn_generator import AzureDataFactoryGenerator

from ..utils import ADFMetadataEncoder


def map_pipeline_run(
    oddrn_generator: AzureDataFactoryGenerator,
    run: ADFPipelineRun,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("pipelines_runs", run.id),
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
                "pipelines", run.pipeline_name
            ),
            status=run.status,
        ),
    )
