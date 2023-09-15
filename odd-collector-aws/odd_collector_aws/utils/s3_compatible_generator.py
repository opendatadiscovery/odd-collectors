"""
S3Compatible generator, for cases like Minio when we can't take region/account_id
"""


from oddrn_generator import Generator
from oddrn_generator.path_models import S3PathsModel
from oddrn_generator.server_models import AbstractServerModel
from pydantic import BaseModel


class S3CompatibleServerModel(AbstractServerModel, BaseModel):
    host: str

    def __str__(self):
        return self.host


class S3CompatibleGenerator(Generator):
    source = "s3_compatible"
    paths_model = S3PathsModel
    server_model = S3CompatibleServerModel
