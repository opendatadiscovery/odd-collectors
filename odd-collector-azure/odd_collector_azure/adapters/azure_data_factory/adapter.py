from odd_collector_azure.domain.plugin import DataFactoryPlugin
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_collector_sdk.errors import DataSourceError, MappingDataError
from odd_models import DataEntity
from odd_models.models import DataEntityList
from oddrn_generator import AzureDataFactoryGenerator
from oddrn_generator.generators import Generator

from .client import DataFactoryClient
from .domain import ADFActivity, ADFDataFlow
from .mapper.activity import map_activity
from .mapper.activity_run import map_activity_run
from .mapper.factory import map_factory
from .mapper.pipeline import map_pipeline
from .mapper.pipeline_run import map_pipeline_run


class Adapter(BaseAdapter):
    config: DataFactoryPlugin
    generator: AzureDataFactoryGenerator

    def __init__(self, config: DataFactoryPlugin):
        self.client = DataFactoryClient(config)
        super().__init__(config)

    def create_generator(self) -> Generator:
        return AzureDataFactoryGenerator(
            azure_cloud_settings={
                "domain": self.config.resource_group,
            }
        )

    def get_data_entity_list(self) -> DataEntityList:
        pipelines_entities: list[DataEntity] = []
        pipelines_runs_entities: list[DataEntity] = []
        activities_entities: list[DataEntity] = []
        activities_runs_entities: list[DataEntity] = []
        try:
            self.generator.set_oddrn_paths(factories=self.config.factory)
            factory = self.client.get_factory()
            pipelines = self.client.get_pipelines(
                factory.name, self.config.pipeline_filter
            )
            for pipeline in pipelines:
                activities_entities_tmp = []
                self.generator.set_oddrn_paths(pipelines=pipeline.name)
                pipelines_runs = self.client.get_pipeline_runs(pipeline.name)
                pipelines_runs_entities.extend(
                    [map_pipeline_run(self.generator, run) for run in pipelines_runs]
                )

                activities = []
                for act in pipeline.activities:
                    activity = ADFActivity(act, all_activities=pipeline.activities)
                    if activity.type == "ExecuteDataFlow":
                        activity.dataflow = self.client.get_data_flow(activity.name)
                    activities.append(activity)

                activities_runs = self.client.get_activity_runs(pipeline.name)

                for activity in activities:
                    self.generator.set_oddrn_paths(activities=activity.name)
                    runs = activities_runs[activity.name]
                    activities_entities_tmp.extend(
                        map_activity(self.generator, activity)
                    )
                    activities_runs_entities.extend(
                        [map_activity_run(self.generator, run) for run in runs]
                    )
                pipelines_entities.append(
                    map_pipeline(self.generator, pipeline, activities_entities_tmp)
                )
                activities_entities.extend(activities_entities_tmp)

            factory_entity = map_factory(self.generator, factory, pipelines_entities)

        except DataSourceError:
            raise

        except Exception as e:
            raise MappingDataError(f"Error during mapping: {e}") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[
                *activities_runs_entities,
                *activities_entities,
                *pipelines_runs_entities,
                *pipelines_entities,
                factory_entity,
            ],
        )
