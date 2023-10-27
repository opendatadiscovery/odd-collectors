from collections import defaultdict
from datetime import datetime

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import PipelineResource
from odd_collector_azure.domain.plugin import DataFactoryPlugin
from odd_collector_sdk.domain.filter import Filter
from odd_collector_sdk.errors import DataSourceError

from .domain import (
    ADFActivityRun,
    ADFDataFlow,
    ADFPipeline,
    ADFPipelineRun,
    DataFactory,
)


def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise DataSourceError(
                f"Connection error during execution of {func.__name__}: {str(e)}"
            ) from e

    return wrapper


class DataFactoryClient:
    # timestamp required for runs requests, should be set to the date which allow to gather all required runs
    start_timestamp = "2010-01-01T00:00:00.0000000Z"

    def __init__(self, config: DataFactoryPlugin):
        self.client = DataFactoryManagementClient(
            credential=DefaultAzureCredential(),
            subscription_id=config.subscription,
        )
        self.resource_group = config.resource_group
        self.factory = config.factory

    @handle_errors
    def get_pipelines(self, factory: str, filter_: Filter) -> list[ADFPipeline]:
        pipeline_resources: list[
            PipelineResource
        ] = self.client.pipelines.list_by_factory(
            resource_group_name=self.resource_group, factory_name=factory
        )

        return [
            ADFPipeline(pipeline)
            for pipeline in pipeline_resources
            if filter_.is_allowed(pipeline.name)
        ]

    @handle_errors
    def get_factory(self) -> DataFactory:
        factory_resource = self.client.factories.get(
            resource_group_name=self.resource_group,
            factory_name=self.factory,
        )

        return DataFactory(factory_resource)

    @handle_errors
    def get_pipeline_runs(self, pipeline_name: str) -> list[ADFPipelineRun]:
        runs = self.client.pipeline_runs.query_by_factory(
            resource_group_name=self.resource_group,
            factory_name=self.factory,
            filter_parameters={
                "filters": [
                    {
                        "operand": "PipelineName",
                        "operator": "Equals",
                        "values": [pipeline_name],
                    }
                ],
                "lastUpdatedAfter": self.start_timestamp,
                "lastUpdatedBefore": datetime.now(),
            },
        ).value

        return [ADFPipelineRun(run) for run in runs]

    @handle_errors
    def get_activity_runs(
        self, pipeline_name: str
    ) -> defaultdict[str, list[ADFActivityRun]]:
        activity_runs = defaultdict(list)
        pipeline_runs = self.get_pipeline_runs(pipeline_name)
        for pipeline_run in pipeline_runs:
            runs = self.client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group,
                factory_name=self.factory,
                run_id=pipeline_run.id,
                filter_parameters={
                    "lastUpdatedAfter": self.start_timestamp,
                    "lastUpdatedBefore": datetime.now(),
                },
            ).value
            for run in runs:
                activity_runs[run.activity_name].append(ADFActivityRun(run))

        return activity_runs

    @handle_errors
    def get_data_flow(self, data_flow_name: str) -> ADFDataFlow:
        df = self.client.data_flows.get(
            resource_group_name=self.resource_group,
            factory_name=self.factory,
            data_flow_name=data_flow_name,
        )
        return ADFDataFlow(df)
