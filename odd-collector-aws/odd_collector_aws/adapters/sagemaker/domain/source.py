from .base_sagemaker_entity import BaseSagemakerEntity


class Source(BaseSagemakerEntity):
    source_arn: str
    source_type: str
