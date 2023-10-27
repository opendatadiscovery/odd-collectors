from funcy import lpluck_attr
from odd_collector_azure.adapters.azure_data_factory.domain import ADFPipeline
from odd_collector_azure.adapters.azure_data_factory.utils import ADFMetadataEncoder
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityGroup, DataEntityType, DataTransformer
from oddrn_generator import AzureDataFactoryGenerator


def map_pipeline(
    oddrn_generator: AzureDataFactoryGenerator,
    pipeline: ADFPipeline,
    activities: list[DataEntity],
) -> DataEntity:
    pipeline_entity = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("pipelines", pipeline.name),
        name=pipeline.name,
        type=DataEntityType.DAG,
        metadata=[
            extract_metadata(
                "azure_data_factory",
                pipeline,
                DefinitionType.DATASET,
                jsonify=True,
                flatten=True,
                json_encoder=ADFMetadataEncoder,
            )
        ],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", activities)
        ),
        data_transformer=DataTransformer(inputs=[], outputs=[]),
    )

    return pipeline_entity
