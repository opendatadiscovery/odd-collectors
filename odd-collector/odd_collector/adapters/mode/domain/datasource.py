from typing import Any, Dict, Optional

from pydantic import BaseModel

from ..generator import Generator


class DataSource(BaseModel):
    id: int
    name: str
    description: str
    token: str
    adapter: str
    created_at: str
    updated_at: str
    has_expensive_schema_updates: bool
    public: bool
    asleep: bool
    queryable: bool
    display_name: str
    database: str
    host: str
    port: str
    ssl: bool
    username: str
    provider: str
    vendor: str
    ldap: bool
    bridged: bool
    adapter_version: str
    custom_attributes: dict
    links: dict

    warehouse: Optional[str] = None
    account_id: Optional[int] = None
    account_username: Optional[str] = None
    organization_token: Optional[str] = None
    default: Optional[str] = None
    default_for_organization_id: Optional[str] = None
    ssl_trusted_cert: Optional[str] = None
    default_access_level: Optional[str] = None

    @staticmethod
    def from_response(response: Dict[str, Any]):
        response["links"] = response.pop("_links")
        return DataSource.model_validate(response)

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("datasource", self.id)
        return oddrn_generator.get_oddrn_by_path("datasource")
