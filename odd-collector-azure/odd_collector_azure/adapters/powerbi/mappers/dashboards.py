from typing import Dict, List

from odd_models.models import DataConsumer, DataEntity, DataEntityType
from oddrn_generator.generators import PowerBiGenerator

from odd_collector_azure.adapters.powerbi.domain.dashboard import Dashboard


def map_dashboard(
    oddrn_generator: PowerBiGenerator,
    dashboard: Dashboard,
    datasets_ids: List[str],
    datasets_oddrns_map: Dict[str, str],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("dashboards", dashboard.display_name),
        name=dashboard.display_name,
        type=DataEntityType.DASHBOARD,
        metadata=[],
        data_consumer=DataConsumer(
            inputs=[
                dataset_oddrn
                for dataset_id, dataset_oddrn in datasets_oddrns_map.items()
                if dataset_id in datasets_ids
            ]
        ),
    )
