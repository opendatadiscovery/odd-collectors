from collections import deque

from funcy import lpluck_attr
from odd_collector_azure.adapters.azure_data_factory.domain import ADFActivity
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityGroup, DataEntityType, DataTransformer
from oddrn_generator import AzureDataFactoryGenerator

from ..utils import ADFMetadataEncoder


def map_activity(
    oddrn_generator: AzureDataFactoryGenerator, activity: ADFActivity
) -> deque[DataEntity]:
    entities = deque()
    activities = deque()

    if activity.activities:
        for act in activity.activities:
            activities.extend(
                map_activity(
                    oddrn_generator,
                    ADFActivity(act, all_activities=activity.activities),
                )
            )

    inputs = [
        oddrn_generator.get_oddrn_by_path("activities", act) for act in activity.inputs
    ]
    outputs = [
        oddrn_generator.get_oddrn_by_path("activities", act) for act in activity.outputs
    ]

    entity = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("activities", activity.name),
        name=activity.name,
        type=DataEntityType.JOB,
        metadata=[
            extract_metadata(
                "azure_data_factory",
                activity,
                DefinitionType.DATASET,
                jsonify=True,
                flatten=True,
                json_encoder=ADFMetadataEncoder,
            )
        ],
        data_transformer=DataTransformer(inputs=inputs, outputs=outputs),
    )

    if activities:
        entity.data_entity_group = DataEntityGroup(
            entities_list=lpluck_attr("oddrn", activities)
        )

    entities.extend(activities)
    entities.append(entity)

    return entities
