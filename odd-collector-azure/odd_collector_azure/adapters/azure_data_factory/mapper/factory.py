from funcy import lpluck_attr
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import AzureDataFactoryGenerator

from odd_collector_azure.adapters.azure_data_factory.domain import DataFactory
from odd_collector_azure.adapters.azure_data_factory.utils import ADFMetadataEncoder


def map_factory(
    oddrn_generator: AzureDataFactoryGenerator,
    factory: DataFactory,
    pipelines: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("factories", factory.name),
        name=factory.name,
        type=DataEntityType.DAG,
        metadata=[
            extract_metadata(
                "azure_data_factory",
                factory,
                DefinitionType.DATASET,
                jsonify=True,
                flatten=True,
                json_encoder=ADFMetadataEncoder,
            )
        ],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", pipelines)
        ),
    )
