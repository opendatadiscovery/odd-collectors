from funcy import lmapcat
from odd_collector_aws.adapters.sagemaker.experiment_mapper import ExperimentMapper
from odd_collector_aws.adapters.sagemaker.sagemaker_client import SagemakerClient
from odd_collector_aws.domain.plugin import SagemakerPlugin
from odd_collector_aws.utils.create_generator import create_generator
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import S3Generator, SagemakerGenerator


class Adapter(AbstractAdapter):
    config: SagemakerPlugin

    def __init__(self, config: SagemakerPlugin):
        self.config = config

        self.s3_generator = create_generator(S3Generator, config)
        self.sagemaker = SagemakerClient(config)
        self.generator = create_generator(SagemakerGenerator, config)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    # Error handling:
    def get_data_entity_list(self):
        mapper = ExperimentMapper(self.generator, self.s3_generator)
        experiments = list(self.sagemaker.get_experiments(self.config.experiments))
        data_entities = lmapcat(mapper.map_experiment, experiments)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=data_entities,
        )
