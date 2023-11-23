from odd_collector.adapters.ckan.mappers.field import map_field
from odd_collector.adapters.ckan.mappers.models import Resource, ResourceField
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityType, DataSet
from oddrn_generator import CKANGenerator


def map_resource(
    oddrn_generator: CKANGenerator,
    organization_name: str,
    resource: Resource,
    fields: list[ResourceField],
) -> DataEntity:
    resource_name = (
        resource.name if resource.name else f"CKAN_{organization_name}_{resource.id}"
    )
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("resources", resource.id),
        name=resource_name,
        type=DataEntityType.FILE,
        metadata=[extract_metadata("ckan", resource, DefinitionType.DATASET_FIELD)],
        dataset=DataSet(
            field_list=[map_field(oddrn_generator, field) for field in fields]
            if fields
            else []
        ),
    )
