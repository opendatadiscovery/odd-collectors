# odd-collector-azure
ODD Collector is a lightweight service which gathers metadata from all your Azure data sources.

## Preview
- [Implemented adapters](#implemented-adapters)
- [How to build](#building)
- [Local run](#run-locally)
- [Docker build](#docker-build)

## Implemented adapters

| Service      | Config example                              |
|--------------|---------------------------------------------|
| PowerBi      | [config](config_examples/power_bi.yaml)     |
| AzureSQL     | [config](config_examples/azure_sql.yaml)    |
| Blob Storage | [config](config_examples/blob_storage.yaml) |

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
docker build -t odd-collector-azure .
```