# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a monorepo for OpenDataDiscovery (ODD) Collectors - a suite of services that extract metadata from various data sources and send it to the ODD Platform. The repository contains four collectors (odd-collector, odd-collector-aws, odd-collector-azure, odd-collector-gcp) and a shared SDK (odd-collector-sdk).

## Common Commands

### Development Setup

```bash
# Install dependencies for a specific collector
cd odd-collector  # or odd-collector-aws, odd-collector-azure, odd-collector-gcp
poetry install

# Activate virtual environment
poetry shell
```

### Linting

```bash
# From repository root - formats all code
make lint

# This runs:
# - black . (code formatter)
# - isort . (import sorter with black profile)
```

### Testing

```bash
# Run all tests for a collector (must be in collector directory)
cd odd-collector
poetry shell
pytest ./tests -v

# Run tests for a specific adapter
pytest ./tests/integration/test_postgres.py -v

# Run tests without activating shell (useful for CI)
poetry run pytest ./tests -v

# Integration tests are marked with @pytest.mark.integration
# Most integration tests use testcontainers
```

### Running Collectors

```bash
# Run a collector locally (requires collector_config.yaml in current directory)
cd odd-collector
poetry run python -m odd_collector

# Run with Docker
docker run -v ./collector_config.yaml:/app/collector_config.yaml ghcr.io/opendatadiscovery/odd-collector:latest

# Set log level via environment variable
export LOGLEVEL=DEBUG  # Options: DEBUG, INFO, WARNING, ERROR
```

### Versioning and Release

Version numbers are stored in two places per collector:
- `./<collector_package>/__version__.py`
- `./pyproject.toml`

Both must be updated manually before creating a release tag.

## Monorepo Architecture

### Package Structure

```
/
├── odd-collector/           # Generic collector (databases, BI tools, ML platforms)
│   ├── odd_collector/
│   │   ├── adapters/       # 40+ adapters (postgresql, mysql, snowflake, etc.)
│   │   ├── domain/
│   │   │   └── plugin.py   # PLUGIN_FACTORY registry
│   │   └── __main__.py     # Entry point
│   ├── config_examples/    # YAML configs for each adapter
│   ├── tests/
│   └── pyproject.toml
├── odd-collector-aws/       # AWS services (S3, Glue, Athena, etc.)
├── odd-collector-azure/     # Azure services (Blob Storage, PowerBI, etc.)
├── odd-collector-gcp/       # GCP services (BigQuery, GCS, etc.)
├── odd-collector-sdk/       # Shared SDK library
│   ├── odd_collector_sdk/
│   │   ├── domain/
│   │   │   ├── adapter.py  # BaseAdapter, AbstractAdapter
│   │   │   ├── plugin.py   # Plugin base class
│   │   │   └── filter.py   # Include/exclude pattern matching
│   │   ├── collector.py    # Main orchestrator
│   │   ├── job.py          # SyncJob, AsyncJob, AsyncGeneratorJob
│   │   └── api/            # ODD Platform API client
│   └── pyproject.toml
└── pyproject.toml          # Root monorepo config
```

### How Collectors Work

1. **Configuration Loading**: Collector reads `collector_config.yaml` from current directory
2. **Dynamic Adapter Loading**: Based on plugin `type`, imports `{root_package}.adapters.{type}.adapter.Adapter`
3. **Job Scheduling**: APScheduler runs jobs based on `default_pulling_interval` (one-time if not set)
4. **Metadata Extraction**: Each adapter's `get_data_entity_list()` returns `DataEntityList`
5. **Platform Ingestion**: SDK chunks and sends entities to ODD Platform via REST API

### Adapter Conventions

Every adapter MUST follow these conventions:

**Directory Structure:**
```
adapters/{adapter_name}/
└── adapter.py  # REQUIRED: Contains "Adapter" class
```

**Adapter Class:**
```python
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList

class Adapter(BaseAdapter):
    def __init__(self, config: YourPluginType) -> None:
        super().__init__(config)

    def create_generator(self) -> Generator:
        # Return ODDRN generator for this data source
        return YourOddrnGenerator(...)

    def get_data_entity_list(self) -> DataEntityList:
        # Extract metadata and return entities
        return DataEntityList(...)
```

**Plugin Registration** (`domain/plugin.py`):
```python
from typing import Literal
from odd_collector_sdk.domain.plugin import Plugin

class YourAdapterPlugin(Plugin):
    type: Literal["adapter_name"]  # MUST match adapter directory name
    # Add adapter-specific config fields
    host: str
    port: int
    # ...

# Register in factory
PLUGIN_FACTORY = {
    "adapter_name": YourAdapterPlugin,
    # ...
}
```

**Configuration Example** (`config_examples/adapter_name.yaml`):
```yaml
platform_host_url: http://localhost:8080
token: ""
plugins:
  - type: adapter_name  # Maps to PLUGIN_FACTORY key
    name: my_instance
    host: localhost
    port: 5432
```

### Common Adapter Patterns

**Repository Pattern** (for databases):
```
adapters/{adapter}/
├── adapter.py       # Orchestrates metadata extraction
├── repository.py    # Executes queries, fetches raw data
├── models.py        # Domain models (Table, Column, Schema, etc.)
└── mappers/         # Transform domain models to ODD entities
    ├── tables.py
    ├── columns.py
    └── ...
```

**Mapper Pattern** (all adapters):
- Separate transformation logic from data access
- Input: Source-specific models (e.g., PostgreSQL Table)
- Output: ODD models (`DataEntity`, `DataSet`, `DataTransformer`, etc.)

**Client Pattern** (for APIs):
```
adapters/{adapter}/
├── adapter.py
├── client.py        # HTTP/SDK client for external API
└── mapper/          # Transform API responses to ODD entities
```

### SDK Base Classes

**BaseAdapter** (most common):
- Provides `generator` attribute via `create_generator()`
- Implements `get_data_source_oddrn()` using generator
- Subclass must implement: `create_generator()`, `get_data_entity_list()`

**AsyncAbstractAdapter** (for async operations):
- Same interface but with `async def get_data_entity_list()`
- SDK automatically wraps in `AsyncJob` or `AsyncGeneratorJob`

**AbstractAdapter** (rarely used directly):
- Minimal interface when you need custom ODDRN handling

### Configuration Patterns

**Priority Order** (highest to lowest):
1. AWS SSM Parameter Store (if `secrets_backend` configured)
2. `collector_config.yaml` file
3. Environment variables (top-level config only, not plugins)
4. Default values

**Environment Variable Substitution:**
```yaml
plugins:
  - type: postgresql
    password: !ENV ${POSTGRES_PASSWORD}
```

**Filter Configuration** (reusable across adapters):
```yaml
plugins:
  - type: postgresql
    schemas_filter:
      include: ["public.*", "sales.*"]  # Regex patterns
      exclude: ["temp.*"]
      ignore_case: false
```

Adapters using filters: PostgreSQL (`schemas_filter`), Snowflake (`schemas_filter`), S3 (`filename_filter`), BigQuery (`datasets_filter`), GCS (`filename_filter`), Azure Data Factory (`pipeline_filter`), Azure Blob (`file_filter`).

## Adding a New Adapter

1. Create adapter directory: `mkdir -p {collector}/adapters/{name}`
2. Implement adapter: `{collector}/adapters/{name}/adapter.py` with `Adapter` class
3. Create plugin model in `{collector}/domain/plugin.py` with `type: Literal["{name}"]`
4. Register in `PLUGIN_FACTORY` dictionary
5. Add configuration example: `{collector}/config_examples/{name}.yaml`
6. Write tests: `{collector}/tests/integration/test_{name}.py`

## Key Dependencies

- **odd-models**: ODD metadata model (`DataEntity`, `DataSet`, etc.)
- **oddrn-generator**: Generates ODDRNs (Open Data Discovery Resource Names)
- **pydantic**: Configuration validation and parsing
- **APScheduler**: Job scheduling for periodic metadata collection
- **funcy**: Functional programming utilities (used extensively)
- **pyaml-env**: YAML parsing with environment variable support

## Docker Build

Each collector has its own Dockerfile:
- Multi-stage build (build + runtime)
- Python 3.9 base image (Debian Bullseye)
- Poetry for dependency management
- Specific system dependencies (ODBC drivers, Oracle client, etc.)
- Entry point: `/bin/bash start.sh` → `python -m {collector_package}`

## Relationships Feature

PostgreSQL and Snowflake adapters support building ERD (Entity-Relationship Diagram) relationships:
- Foreign key constraints → `ONE_TO_EXACTLY_ONE`, `ONE_TO_ZERO_OR_ONE`, etc.
- Cross-schema relationships supported
- Implementation: `adapters/{adapter}/mappers/relationships/`

## Testing with Testcontainers

Integration tests use testcontainers to spin up real databases/services:
```python
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.mark.integration
def test_postgres_adapter():
    with PostgresContainer("postgres:14") as postgres:
        # Test adapter against real Postgres
```

Run integration tests: `pytest ./tests -v -m integration`