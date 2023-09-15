# OpenDataDiscovery Collectors.

[Generic Collector](#odd-collector) | [AWS Collector](#odd-collector-aws) | [Azure Collector](#odd-collector-azure) | [GCP Collector](#odd-collector-gcp)


### What is collector?
[Usage example](#usage-example)

Collector is a service that loads and runs [adapters](#what-is-adapter). Collectors are separated by data sources. Each collector has examples of configuration files for each adapter.
Collector works as a daemon and periodically load metadata from data sources. Data sources can be configured in plugins field of collector config.
Each plugin has own configuration to connect to data source and load metadata.

### What is adapter?
Adapter is an abstraction that allows to load metadata from different data sources.
Providing all the necessary information to connect to the data source, adapter can load metadata from it and send it to ODD Platform.
Adapters do not have any dependencies on each other and can be used separately. Adapter do not read real data from data source, only metadata.

# odd-collector
[Image](https://github.com/opendatadiscovery/odd-collector/pkgs/container/odd-collector) | [Configuration examples](odd-collector/config_examples)

Collector to the common data sources, it shares adapters to Databases, BI Tools or ML platforms as MLFlow.

Supported data sources:
- [Airbyte](odd-collector/README.md)
- [Cassandra](odd-collector/README.md)
- [CKAN](odd-collector/README.md)
- [ClickHouse](odd-collector/README.md)
- [cockroachdb](odd-collector/README.md)
- [CubeJS](odd-collector/README.md)
- [Druid](odd-collector/README.md)
- [Duckdb](odd-collector/README.md)
- [Elasticsearch](odd-collector/README.md)
- [Feast](odd-collector/README.md)
- [Fivetran](odd-collector/README.md)
- [Hive](odd-collector/README.md)
- [Kafka](odd-collector/README.md)
- [Kubeflow](odd-collector/README.md)
- [MariaDB](odd-collector/README.md)
- [Metabase](odd-collector/README.md)
- [Mlflow](odd-collector/README.md)
- [Mode](odd-collector/README.md)
- [MongoDB](odd-collector/README.md)
- [MSSql](odd-collector/README.md)
- [MySql](odd-collector/README.md)
- [Neo4j](odd-collector/README.md)
- [ODBC](odd-collector/README.md)
- [ODD Adapter](odd-collector/README.md)
- [PostgreSQL](odd-collector/README.md)
- [Presto](odd-collector/README.md)
- [Redash](odd-collector/README.md)
- [Redshift](odd-collector/README.md)
- [Scylladb](odd-collector/README.md)
- [SingleStore](odd-collector/README.md)
- [Snowflake](odd-collector/README.md)
- [Superset](odd-collector/README.md)
- [sqlite](odd-collector/README.md)
- [Tableau](odd-collector/README.md)
- [Tarantool](odd-collector/README.md)
- [Trino](odd-collector/README.md)
- [Vertica](odd-collector/README.md)

# odd-collector-aws
[Image](https://github.com/opendatadiscovery/odd-collector/pkgs/container/odd-collector-aws) | [Configuration examples](odd-collector-aws/config_examples)

Collector provides adapter for Amazon cloud services

Supported data sources:
- [Athena](odd-collector-aws/README.md)
- [DynamoDB](odd-collector-aws/README.md)
- [Glue](odd-collector-aws/README.md)
- [Quicksight](odd-collector-aws/README.md)
- [Kinesis](odd-collector-aws/README.md)
- [S3](odd-collector-aws/README.md)
- [S3 Delta](odd-collector-aws/README.md)
- [Sagemaker](odd-collector-aws/README.md)
- [SagemakerFeaturestore](odd-collector-aws/README.md)
- [SQS](odd-collector-aws/README.md)

# odd-collector-azure
[Image](https://github.com/opendatadiscovery/odd-collector/pkgs/container/odd-collector-azure) | [Configuration examples](odd-collector-azure/config_examples)

Collector provides adapter for Microsoft Azure cloud services
Supported data sources:
- [Azure SQL](odd-collector-azure/README.md)
- [Blob Storage](odd-collector-azure/README.md)
- [PowerBI](odd-collector-azure/README.md)

# odd-collector-gcp
[Image](https://github.com/opendatadiscovery/odd-collector/pkgs/container/odd-collector-gcp) | [Configuration examples](odd-collector-gcp/config_examples)

Collector provides adapter for Google Cloud services. [Detailed documentation](odd-collector-gcp/README.md).

Supported data sources:
- [BigQuery](odd-collector-gcp/README.md#bigquery)
- [BigTable](odd-collector-gcp/README.md#bigtable)
- [GoogleCloudStorage](odd-collector-gcp/README.md#googlecloudstorage)
- [GoogleCloudStoraDeltaTables](odd-collector-gcp/README.md##googlecloudstoragedeltatables)

#### Usage Example

### Collector configuration
Config file must be named `collector_config.yaml` and placed in the same directory as the collector package.
Collector config fields:
```yaml
default_pulling_interval: Optional[int] = None # Minutes to wait between runs of the job, if not set, job will be run only once
token: str # Token to access ODD Platform
plugins: list[Plugin] # List of adapters configs to be loaded
platform_host_url: str # URL of ODD Platform instance, i.e. http://localhost:8080
chunk_size: int = 250 # Number of records to be sent in one request to the platform
connection_timeout_seconds: int = 300 # Seconds to wait for connection to the platform
misfire_grace_time: Optional[int] = None  # seconds after the designated runtime that the job is still allowed to be run
max_instances: Optional[int] = 1  # maximum number of concurrently running instances allowed
verify_ssl: bool = True # For cases when self-signed certificates are used
```

### Example of collector config:
```yaml
default_pulling_interval: 10
token: '****'
platform_host_url: http://localhost:8080
chunk_size: 1000
plugins:
  - type: postgresql
    name: postgresql_adapter
    database: database
    host: localhost
    port: 5432
    user: postgres
    password: !ENV ${POSTGRES_PASSWORD}
```

### Using any collector in a docker container:
For more completed example take a look at [docker compose for demo](https://github.com/opendatadiscovery/odd-platform/blob/main/docker/README.md).
```yaml
version: "3.8"
services:
  odd-collector:
    image: ghcr.io/opendatadiscovery/odd-collector:latest
    restart: always
    volumes:
      - collector_config.yaml:/app/collector_config.yaml
    environment:
      - LOGLEVEL=DEBUG # Optional default INFO, use DEBUG for more verbose logs
      - PLATFORM_HOST_URL=${PLATFORM_HOST_URL}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
```