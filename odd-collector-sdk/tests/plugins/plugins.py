from typing_extensions import Literal

from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory


class TestGluePlugin(Plugin):
    type: Literal["glue"]


class TestS3Plugin(Plugin):
    type: Literal["s3"]


class PostgresPlugin(Plugin):
    type: Literal["postgres"]
    db_user: str


PLUGIN_FACTORY: PluginFactory = {
    "glue": TestGluePlugin,
    "s3": TestS3Plugin,
    "postgres": PostgresPlugin,
}
