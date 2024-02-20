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

Collector to the common data sources, it shares adapters to Databases, Vector Stores (pgvector PostgreSQL extension), BI Tools or ML platforms as MLFlow.

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

# Ingestion Filters Configuration
This section provides a comprehensive reference for configuring Ingestion Filters that are available within several ODD Data Collectors. 
The table below outlines key information about those Collectors along with Adapters, Filter Configuration Parameters and brief Descriptions of Filter for each of them. 

| Collector           | Adapter                    | Filter Config Parameter   | Filter Description                    |
|---------------------|----------------------------|---------------------------|---------------------------------------|
| odd-collector       | PostgreSQL                 | schemas_filter            | Filter object by database schema name |
| odd-collector-aws   | S3                         | filename_filter           | Filter by file name                   |
| odd-collector-aws   | S3 Delta                   | filter                    | Filter by file name                   |
| odd-collector-gcp   | BigQuery                   | datasets_filter           | Filter by data set name               |
| odd-collector-gcp   | Google Cloud Storage       | filename_filter           | Filter by file name                   |
| odd-collector-gcp   | Google Cloud Storage Delta | filter                    | Filter by file name                   |
| odd-collector-azure | Azure Data Factory (ADF)   | pipeline_filter           | Filter by pipeline name               |
| odd-collector-azure | Azure BLOB Storage         | file_filter               | Filter by file name                   |

# Relationships
The goal of this feature is to build relationships on the top of core data entities that are logically related.
The table below represents what adapters currently support this feature and are capable of constructing the
Relationship DataEntity.
There are 2 types of relationships: ERD(Entity-Relationship Diagram) and GRAPH.
- ERD relationships represent associations between entities within a relational database. We determine 4 cardinality types of relationships:
    - ONE_TO_EXACTLY_ONE - a single instance of an entity is related to a single instance of another entity.
    - ONE_TO_ZERO_OR_ONE - a single instance of an entity is related to either zero instances or one instance of another entity.
    - ONE_TO_ONE_OR_MORE - a single instance of an entity is related to multiple instances of another entity.
    - ONE_TO_ZERO_ONE_OR_MORE - a single instance of an entity is related to zero instances or one or more instances of another entity.
- GRAPH relationships refer to connections between entities represented in a graph data structure. For example in Neo4j it will be
  relationships between nodes.


| Collector             | Adapter              | Relationship Type | Relationship Description                                                                   |
|-----------------------|----------------------|-------------------|--------------------------------------------------------------------------------------------|
| odd-collector         | PostgreSQL           | ERD               | Relationship between 2 related table entities that is determined by foreign key constraint |


# Collector configuration using alternative Secrets Backend
There is an option to store collector configuration settings via Secrets Backend (only AWS SSM Parameter Store is supported for now).
Using this approach you need to create your secrets in the chosen Secret Backend provider according to the naming and backend configuration
specified in `secrets_backend` section of `collector_config.yaml`. More detailed information with usage examples you can find below in
"Usage Example" section. Also some actual information can be found in `odd-collector` documentation and `odd-collector/collector_config.yaml` snippet.

# Usage Example

## Collector configuration
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

## Secrets Backend configuration
Secrets Backend section must be specified only in the case when you are using one of the supported
backends. In case when you use only local `collector_config.yaml` file for configuration you might
skip the `secrets_backend:` section (delete it, or left commented).
So, if you need this functionality it must be configured in the `collector_config.yaml` as well as Collector config.
As only AWSSystemsManagerParameterStore is supported for now, all the examples are attached to this case for now.
```yaml
secrets_backend:
  provider: "AWSSystemsManagerParameterStore"
  # the section below is for key-value arguments provider needs
  region_name: "eu-central-1"    # region where you store secrets
  collector_settings_parameter_name: "/odd/collector_config/collector_settings"   # parameter name for storing
                                     # collector settings, default is "/odd/collector_config/collector_settings"
  collector_plugins_prefix: "/odd/collector_config/plugins"   # prefix for parameters, that contain
                            # plugins configurations, default is "/odd/collector_config/plugins"
```
`provider` is must have to specify parameter, without default value.

`region_name` information is retreiving in the following logic:
1. The most priority has environment variable `AWS_REGION`, if it is specified - it's value will be used.
2. If no `AWS_REGION` provided, the information from `collector_config.yaml` will be used.
3. If `region_name` is not specified, we are trying to retreive AWS region information from instance metadata service (IMDS).
4. If none of the above worked, adapter will throw an error, as we can not instantiate the connection to the service.

`collector_settings_parameter_name` and `collector_plugins_prefix` have the default values, so if naming seems good for you,
this parameters can be skipped.

## Example of collector config:
```yaml
secrets_backend:
  provider: "AWSSystemsManagerParameterStore"
  # the section below is for key-value arguments provider needs
  region_name: "eu-central-1"
  collector_settings_parameter_name: "/odd/collector_config/collector_settings"
  collector_plugins_prefix: "/odd/collector_config/plugins"

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

## Using any collector in a docker container:
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
