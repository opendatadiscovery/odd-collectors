from odd_collector_aws.aws.aws_client import AwsClient
from odd_collector_aws.domain.plugin import AwsPlugin


class DMSClient:
    def __init__(self, config: AwsPlugin):
        self._config = config

        self.dms = AwsClient(config).get_client("dms")
        self.account_id = AwsClient(config).get_account_id()
