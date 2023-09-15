# odd-collector-gcp
ODD Collector GCP is a lightweight service which gathers metadata from all your Google Cloud Platform data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Authentication
By default, collector uses the process described in https://google.aip.dev/auth/4110 to resolve credentials.  
If not running on Google Cloud Platform (GCP), this generally requires the environment variable GOOGLE_APPLICATION_CREDENTIALS to point to a JSON file containing credentials.

## Preview
 - [Implemented adapters](#implemented-adapters)
 - [How to build](#how-to-build)
 - [Config example](#config-example)
 - [Run locally](#run-locally)
 - [Docker build](#docker-build)

## Implemented adapters
 - [BigQuery](#bigquery)
 - [BigTable](#bigtable)
 - [GoogleCloudStorage](#googlecloudstorage)
 - [GoogleCloudStoraDeltaTables](#googlecloudstoragedeltatables)

### __BigQuery__
```yaml
type: bigquery_storage
name: bigquery_storage
project: <any_project_name>
```

### __BigTable__
```yaml
type: bigtable
name: bigtable
project: <any_project_name>
rows_limit: 10 # get combination of all types in table used across the first N rows.
```

### __GoogleCloudStorage__
```yaml
type: gcs
name: gcs_adapter
project: <any_project_name>
filename_filter: # Optional. Default filter allows each file to be ingested to platform.
  include: [ '.*.parquet' ]
  exclude: [ 'dev_.*' ]
parameters: # Optional set of parameters, default values presented below.
  anonymous: bool = False, # Whether to connect anonymously. If True, will not attempt to look up credentials using standard GCP configuration methods.
  access_token: str = None, # GCP access token. If provided, temporary credentials will be fetched by assuming this role; also, a credential_token_expiration must be specified as well.
  target_service_account: str = None, # An optional service account to try to impersonate when accessing GCS. This requires the specified credential user or service account to have the necessary permissions.
  credential_token_expiration: datetime = None, # Datetime in format: "2023-12-31 23:59:59". Expiration for credential generated with an access token. Must be specified if access_token is specified.
  default_bucket_location: str = "US", # GCP region to create buckets in.
  scheme: str = "https", # GCS connection transport scheme.
  endpoint_override: str = None, # Override endpoint with a connect string such as “localhost:9000”
  default_metadata: mapping or pyarrow.KeyValueMetadata = None, # Default metadata for open_output_stream. This will be ignored if non-empty metadata is passed to open_output_stream.
  retry_time_limit: timedelta = None, # Timedelta in seconds. Set the maximum amount of time the GCS client will attempt to retry transient errors. Subsecond granularity is ignored.
datasets:
  # Recursive fetch for all objects in the bucket.
  - bucket: my_bucket
  # Explicitly specify the prefix to file.
  - bucket: my_bucket
    prefix: folder/subfolder/file.csv
  # When we want to use the folder as a dataset. Very useful for partitioned datasets.
  # I.e it can be Hive partitioned dataset with structure like this:
  # gs://my_bucket/partitioned_data/year=2019/month=01/...
  - bucket: my_bucket
    prefix: partitioned_data/
    folder_as_dataset:
      file_format: parquet
      flavor: hive
  #field_names must be provided if partition flavor was not used. I.e for structure like this:
  # gs://my_bucket/partitioned_data/year/...
  - bucket: my_bucket
    prefix: partitioned_data/
    folder_as_dataset:
      file_format: csv
      field_names: ['year']
```

### __GoogleCloudStorageDeltaTables__
```yaml
type: gcs_delta
name: gcs_delta_adapter
project: <any_project_name>
parameters: # Optional set of parameters, default values presented below.
  anonymous: bool = False, # Whether to connect anonymously. If True, will not attempt to look up credentials using standard GCP configuration methods.
  access_token: str = None, # GCP access token. If provided, temporary credentials will be fetched by assuming this role; also, a credential_token_expiration must be specified as well.
  target_service_account: str = None, # An optional service account to try to impersonate when accessing GCS. This requires the specified credential user or service account to have the necessary permissions.
  credential_token_expiration: datetime = None, # Datetime in format: "2023-12-31 23:59:59". Expiration for credential generated with an access token. Must be specified if access_token is specified.
  default_bucket_location: str = "US", # GCP region to create buckets in.
  scheme: str = "https", # GCS connection transport scheme.
  endpoint_override: str = None, # Override endpoint with a connect string such as “localhost:9000”
  default_metadata: mapping or pyarrow.KeyValueMetadata = None, # Default metadata for open_output_stream. This will be ignored if non-empty metadata is passed to open_output_stream.
  retry_time_limit: timedelta = None, # Timedelta in seconds. Set the maximum amount of time the GCS client will attempt to retry transient errors. Subsecond granularity is ignored.
delta_tables: # Explicitly specify the bucket and prefix to the file.
  - bucket: str = "bucket_name"
    prefix: str = "folder/subfolder/file.csv"
    filter: # Optional, default values below
      include: list[str] = [".*"] # List of patterns to include.
      exclude: list[str] = None  # List of patterns to exclude

```

## Run locally
To run collector locally firstly we need to activate virtual environment and install dependencies:
```commandline
poetry shell
poetry install
```
If all dependencies are installed and collector config was set correctly we can run collector with:
```commandline
sh start.sh
```

## Docker build
```bash
docker build -t odd-collector-gcp .
```
