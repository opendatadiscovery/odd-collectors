[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/for-you.svg)](https://forthebadge.com)
# odd-collector-aws
ODD Collector is a lightweight service which gathers metadata from all your data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Preview
- [odd-collector-aws](#odd-collector-aws)
  - [Preview](#preview)
  - [Implemented adapters](#implemented-adapters)
  - [Building](#building)
  - [Docker compose example](#docker-compose-example)

## Implemented adapters
| Service                                                      | Config example                                        |
|--------------------------------------------------------------|-------------------------------------------------------|
| Athena                 <a name="athena"></a>                 | [config](config_examples/athena.yaml)                 |
| DynamoDB               <a name="dynamodb"></a>               | [config](config_examples/dynamodb.yaml)               |
| Glue                   <a name="glue"></a>                   | [config](config_examples/glue.yaml)                   |
| Kinesis                <a name="kinesis"></a>                | [config](config_examples/kinesis.yaml)                |
| Quicksight             <a name="quicksight"></a>             | [config](config_examples/quicksight.yaml)             |
| S3                     <a name="s3"></a>                     | [config](config_examples/s3.yaml)                     |
| S3_Delta               <a name="s3_delta"></a>               | [config](config_examples/s3_delta.yaml)               |
| Sagemaker              <a name="sagemaker"></a>              | [config](config_examples/sagemaker.yaml)              |
| SQS                    <a name="sqs"></a>                    | [config](config_examples/sqs.yaml)                    |
| SagemakerFeaturestore  <a name="sagemaker-featurestore"></a> | [config](config_examples/sagemaker_featurestore.yaml) |


## Building
```bash
docker build .
```

## Docker compose example
Due to the Plugin is inherited from `pydantic.BaseSetting`, each field missed in `collector-config.yaml` can be taken from env variables.

Custom `.env` file for docker-compose.yaml
```
AWS_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
PLATFORM_HOST_URL=http://odd-platform:8080
```

Custom `collector-config.yaml`
```yaml
platform_host_url: http://localhost:8080
default_pulling_interval: 10 # Can be omitted to run collector once
token: "" # Token that must be retrieved from the platform
plugins:
  - type: s3
    name: s3_adapter
    aws_secret_access_key: <aws_secret_access_key> # Optional.
    aws_access_key_id: <aws_access_key_id> # Optional.
    aws_session_token: <aws_session_token> # Optional.
    aws_region: <aws_region> # Optional.
    datasets:
      # Recursive fetch for all objects in the bucket.
      - bucket: my_bucket
      # Explicitly specify the prefix to file.
      - bucket: my_other_bucket
        prefix: folder/subfolder/file.csv
```

docker-compose.yaml
```yaml
version: "3.8"
services:
  # --- ODD Platform ---
  database:
    ...

  odd-platform:
    ...
  
  odd-collector-aws:
    image: 'ghcr.io/opendatadiscovery/odd-collector-aws:latest'
    restart: always
    volumes:
      - collector_config.yaml:/app/collector_config.yaml
    environment:
      - AWS_REGION=${AWS_REGION}
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - PLATFORM_HOST_URL=${PLATFORM_HOST_URL}
      - LOGLEVEL='DEBUG'
    depends_on:
      - odd-platform
```
