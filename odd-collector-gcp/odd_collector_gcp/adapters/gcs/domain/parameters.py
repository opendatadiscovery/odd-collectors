from datetime import datetime, timedelta
from typing import Mapping, Union

from pyarrow import KeyValueMetadata
from pydantic import BaseModel


class GCSAdapterParams(BaseModel):
    """
    Model for optional adapter parameters
    """

    anonymous: bool = False
    access_token: str = None
    target_service_account: str = None
    credential_token_expiration: datetime = None
    default_bucket_location: str = "US"
    scheme: str = "https"
    endpoint_override: str = None
    default_metadata: Union[Mapping, KeyValueMetadata] = None
    retry_time_limit: timedelta = None

    class Config:
        arbitrary_types_allowed = True
