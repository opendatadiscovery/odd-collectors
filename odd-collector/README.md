[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# odd-collector

ODD Collector is a lightweight service that gathers metadata from all your data sources.

To learn more about collector types and ODD Platform's
architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Preview:

- [odd-collector](#odd-collector)
    - [Preview:](#preview)
    - [Implemented adapters](#implemented-adapters)
    - [Run locally](#run-locally)
    - [Docker build](#docker-build)
    - [M1 building issue](#m1-building-issue)

## Implemented adapters

| Service       | Config example                                                                          |
|---------------|-----------------------------------------------------------------------------------------|
| Cassandra     | [config](config_examples/cassandra.yaml)                                                |
| ClickHouse    | [config](config_examples/clickhouse.yaml)                                               |
| Dbt           | [config](config_examples/dbt.yaml)                                                      |
| Elasticsearch | [config](config_examples/elasticsearch.yaml)                                            |
| Feast         | [config](config_examples/feast.yaml)                                                    |
| Hive          | [config](config_examples/hive.yaml)                                                     |
| Kafka         | [config](config_examples/kafka.yaml)                                                    |
| Kubeflow      | [config](config_examples/kubeflow.yaml)                                                 |
| MariaDB       | [config](config_examples/mysql.yaml), _supported via MySql adapter_                     |
| MongoDB       | [config](config_examples/mongodb.yaml)                                                  |
| MSSql         | [config](config_examples/mssql.yaml)                                                    |
| MySql         | [config](config_examples/mysql.yaml)                                                    |
| Neo4j         | [config](config_examples/neo4j.yaml)                                                    |
| PostgreSQL    | [config](config_examples/postgresql.yaml)                                               |
| Presto        | [config](config_examples/presto.yaml)                                                   |
| Redash        | [config](config_examples/redash.yaml)                                                   |
| Redshift      | [config](config_examples/redshift.yaml)                                                 |
| Snowflake     | [config](config_examples/snowflake.yaml)                                                |
| Superset      | [config](config_examples/superset.yaml)                                                 |
| Tableau       | [config](config_examples/tableau.yaml)                                                  |
| Tarantool     | [config](config_examples/tarantool.yaml)                                                |
| Trino         | [config](config_examples/trino.yaml)                                                    |
| Vertica       | [config](config_examples/vertica.yaml)                                                  |
| ODBC          | [config](config_examples/odbc.yaml), [README.md](odd_collector/adapters/odbc/README.md) |
| Cube          | [config](config_examples/cubejs.yaml)                                                   |
| ODD Adapter   | [config](config_examples/odd_adapter.yaml)                                              |
| Apache Druid  | [config](config_examples/druid.yaml)                                                    |
| Oracle        | [config](config_examples/oracle.yaml)                                                   |
| Airbyte       | [config](config_examples/airbyte.yaml)                                                  |
| SingleStore   | [config](config_examples/singlestore.yaml)                                              |
| cockroachdb   | [config](config_examples/cockroachdb.yaml)                                              |
| sqlite        | [config](config_examples/sqlite.yaml)                                                   |

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
docker build -t odd-collector .
```

## M1 building issue

**libraries** `pyodbc` , `confluent-kafka` and `grpcio`   have problem during installing and building project on Mac M1.

- https://github.com/mkleehammer/pyodbc/issues/846
- https://github.com/confluentinc/confluent-kafka-python/issues/1190
- https://github.com/grpc/grpc/issues/25082

Possible solutions

```bash
# NOTE: be aware of versions
# NOTE: easiest way is to add all export statements to your .bashrc/.zshrc file

# pyodbc dependencies
brew install unixodbc freetds openssl

export LDFLAGS="-L/opt/homebrew/lib  -L/opt/homebrew/Cellar/unixodbc/2.3.11/include -L/opt/homebrew/Cellar/freetds/1.3.17/lib -L/opt/homebrew/Cellar/openssl@1.1/1.1.1t/lib"
export CFLAGS="-I/opt/homebrew/Cellar/unixodbc/2.3.11/include -I/opt/homebrew/opt/freetds/include"
export CPPFLAGS="-I/opt/homebrew/include -I/opt/homebrew/Cellar/unixodbc/2.3.11/include -I/opt/homebrew/opt/openssl@3/include"

# confluent-kafka
brew install librdkafka

export C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/1.9.0/include
export LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/1.9.0/lib
export PATH="/opt/homebrew/opt/openssl@3/bin:$PATH"

# grpcio
export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1

# mymssql
brew install freetds
export LDFLAGS="-L/opt/homebrew/Cellar/freetds/1.3.17/lib -L/opt/homebrew/Cellar/openssl@1.1/1.1.1t/lib"
```

