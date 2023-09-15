# odd-collector-aws
ODD Collector is a lightweight service which gathers metadata from all your AWS data sources.

## Preview
- [odd-collector-aws](#odd-collector-aws)
  - [Preview](#preview)
  - [Implemented adapters](#implemented-adapters)
  - [Local run](#run-locally)
  - [Docker build](#docker-build)

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
docker build -t odd-collector-aws .
```
