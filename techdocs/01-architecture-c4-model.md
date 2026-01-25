# ODD Collectors - Architecture Documentation (C4 Model)

> This document describes the architecture of the ODD Collectors system using the C4 Model methodology.
> C4 Model provides four levels of abstraction: Context, Container, Component, and Code.

---

## Table of Contents

1. [Level 1: System Context](#level-1-system-context)
2. [Level 2: Container Diagram](#level-2-container-diagram)
3. [Level 3: Component Diagram](#level-3-component-diagram)
4. [Level 4: Code Level](#level-4-code-level)
5. [Data Flow Architecture](#data-flow-architecture)
6. [Architecture Review & Improvement Suggestions](#architecture-review--improvement-suggestions)

---

## Level 1: System Context

### Overview

The ODD Collectors system extracts metadata from various data sources and sends it to the ODD Platform for data discovery and governance.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              SYSTEM CONTEXT                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────┐          ┌─────────────────────────┐          ┌─────────────┐
    │             │          │                         │          │             │
    │  Data       │◄────────►│    ODD Collectors       │─────────►│ ODD Platform│
    │  Sources    │ metadata │    (This System)        │ ingest   │   (API)     │
    │             │          │                         │          │             │
    └─────────────┘          └─────────────────────────┘          └─────────────┘
          │                            │                                 │
          │                            │                                 │
    ┌─────┴─────┐              ┌───────┴───────┐                        │
    │           │              │               │                        │
    ▼           ▼              ▼               ▼                        ▼
┌───────┐  ┌───────┐    ┌───────────┐  ┌────────────┐           ┌─────────────┐
│ DBs   │  │ Cloud │    │ Config    │  │  Secrets   │           │ Data Catalog│
│ APIs  │  │ Svcs  │    │ (YAML)    │  │  Backend   │           │ Lineage     │
└───────┘  └───────┘    └───────────┘  └────────────┘           │ Discovery   │
                                                                 └─────────────┘

```

### Actors & External Systems

| Actor/System | Description | Interaction |
|--------------|-------------|-------------|
| **Data Sources** | Databases, cloud services, BI tools, ML platforms | Collectors query these for metadata |
| **ODD Platform** | Central data discovery platform | Receives ingested metadata via REST API |
| **Secrets Backend** | AWS SSM, etc. | Provides secure configuration/credentials |
| **DevOps/Data Engineers** | Human operators | Configure and deploy collectors |

### Key Responsibilities

1. **Connect** to diverse data sources (60+ supported)
2. **Extract** metadata (schemas, tables, columns, relationships)
3. **Transform** to ODD standard format (DataEntity model)
4. **Ingest** to ODD Platform via REST API
5. **Schedule** periodic collection (daemon mode)

---

## Level 2: Container Diagram

### Container Overview

The system is organized as a monorepo with 5 deployable containers (packages):

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              CONTAINER DIAGRAM                                   │
└─────────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────────────────────┐
                    │          ODD Platform API           │
                    │        (External System)            │
                    └──────────────────┬──────────────────┘
                                       │
                                       │ REST API
                                       │ POST /ingestion/entities
                                       │ POST /ingestion/datasources
                                       ▼
    ┌──────────────────────────────────────────────────────────────────────────┐
    │                         ODD COLLECTORS MONOREPO                          │
    │                                                                          │
    │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐ ┌────────────┐│
    │  │ odd-collector  │ │odd-collector-  │ │odd-collector-  │ │odd-        ││
    │  │                │ │     aws        │ │    azure       │ │collector-  ││
    │  │  42 adapters   │ │  11 adapters   │ │   4 adapters   │ │   gcp      ││
    │  │                │ │                │ │                │ │ 4 adapters ││
    │  │ - PostgreSQL   │ │ - S3           │ │ - PowerBI      │ │ - BigQuery ││
    │  │ - MySQL        │ │ - Glue         │ │ - Azure SQL    │ │ - GCS      ││
    │  │ - Snowflake    │ │ - Athena       │ │ - Blob Storage │ │ - BigTable ││
    │  │ - MongoDB      │ │ - DynamoDB     │ │ - Data Factory │ │ - GCS Delta││
    │  │ - Kafka        │ │ - Kinesis      │ │                │ │            ││
    │  │ - Tableau      │ │ - SageMaker    │ │                │ │            ││
    │  │ - etc...       │ │ - etc...       │ │                │ │            ││
    │  └───────┬────────┘ └───────┬────────┘ └───────┬────────┘ └─────┬──────┘│
    │          │                  │                  │                │       │
    │          └──────────────────┴─────────┬────────┴────────────────┘       │
    │                                       │                                  │
    │                                       ▼                                  │
    │                    ┌──────────────────────────────────┐                 │
    │                    │       odd-collector-sdk          │                 │
    │                    │                                  │                 │
    │                    │  - Collector (orchestrator)      │                 │
    │                    │  - BaseAdapter (interface)       │                 │
    │                    │  - Plugin (config model)         │                 │
    │                    │  - Job (sync/async execution)    │                 │
    │                    │  - PlatformApi (REST client)     │                 │
    │                    │  - Config Loader                 │                 │
    │                    │  - Secrets Backend               │                 │
    │                    └──────────────────────────────────┘                 │
    │                                                                          │
    └──────────────────────────────────────────────────────────────────────────┘
                    │                                       │
                    │                                       │
         ┌──────────┴──────────┐               ┌───────────┴───────────┐
         ▼                     ▼               ▼                       ▼
    ┌─────────┐          ┌─────────┐     ┌──────────┐           ┌──────────┐
    │ Database│          │  Cloud  │     │ collector│           │  Secrets │
    │ Servers │          │ Services│     │_config.  │           │  Backend │
    │         │          │         │     │  yaml    │           │ (AWS SSM)│
    └─────────┘          └─────────┘     └──────────┘           └──────────┘
```

### Container Descriptions

| Container | Technology | Description |
|-----------|------------|-------------|
| **odd-collector-sdk** | Python library | Shared framework: adapters, jobs, API client, config |
| **odd-collector** | Python app + Docker | Generic collector: databases, BI tools, ML platforms |
| **odd-collector-aws** | Python app + Docker | AWS services: S3, Glue, Athena, DynamoDB, etc. |
| **odd-collector-azure** | Python app + Docker | Azure services: Blob, PowerBI, Data Factory |
| **odd-collector-gcp** | Python app + Docker | GCP services: BigQuery, GCS, BigTable |

### Communication Patterns

| From | To | Protocol | Purpose |
|------|----|----------|---------|
| Collectors | ODD Platform | HTTPS REST | Metadata ingestion |
| Collectors | Data Sources | Various (JDBC, SDK, REST) | Metadata extraction |
| Collectors | Secrets Backend | AWS SDK | Secure config retrieval |

---

## Level 3: Component Diagram

### SDK Components (odd-collector-sdk)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         odd-collector-sdk COMPONENTS                             │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 COLLECTOR                                        │
│                            (Main Orchestrator)                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  - Loads configuration (CollectorConfigLoader)                          │    │
│  │  - Dynamically loads adapters (load_adapters)                           │    │
│  │  - Registers data sources with platform                                 │    │
│  │  - Schedules jobs (APScheduler)                                         │    │
│  │  - Manages lifecycle (signals, graceful shutdown)                       │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │
           ┌────────────────────────────┼────────────────────────────┐
           │                            │                            │
           ▼                            ▼                            ▼
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   CONFIG LOADER     │    │    ADAPTER LOADER   │    │     PLATFORM API    │
├─────────────────────┤    ├─────────────────────┤    ├─────────────────────┤
│ CollectorConfigLoader│    │ load_adapters()     │    │ PlatformApi         │
│ - YAML parsing      │    │ load_package()      │    │ - register_datasource│
│ - Env var subst.    │    │ import_submodules() │    │ - ingest_data       │
│ - Secrets merge     │    │ @cache decoration   │    │ - aiohttp client    │
│ - Pydantic valid.   │    │ Dynamic import      │    │ - Async HTTP        │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
           │                            │                            │
           ▼                            ▼                            │
┌─────────────────────┐    ┌─────────────────────┐                   │
│   SECRETS BACKEND   │    │    JOB SYSTEM       │                   │
├─────────────────────┤    ├─────────────────────┤                   │
│ BaseSecretsBackend  │    │ AbstractJob         │                   │
│ AWSSSMBackend       │    │ ├─ SyncJob          │◄──────────────────┘
│ SecretsBackendFactory│    │ ├─ AsyncJob         │    send_metadata()
│                     │    │ └─ AsyncGeneratorJob │
└─────────────────────┘    │ create_job() factory │
                           │ _split() chunking    │
                           └─────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DOMAIN MODELS                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐    ┌───────────────┐    ┌───────────────┐    ┌────────────┐ │
│  │ AbstractAdapter│    │    Plugin     │    │CollectorConfig│    │   Filter   │ │
│  │ BaseAdapter   │    │ (pydantic)    │    │ (pydantic)    │    │ include[]  │ │
│  │ AsyncAdapter  │    │ type, name    │    │ plugins[]     │    │ exclude[]  │ │
│  │               │    │ extra=allow   │    │ token, url    │    │ is_allowed()│ │
│  └───────────────┘    └───────────────┘    └───────────────┘    └────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Collector Components (odd-collector example)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         odd-collector COMPONENTS                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              __main__.py                                         │
│                            (Entry Point)                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Collector(                                                                      │
│      config_path=CONFIG_PATH,                                                   │
│      root_package="odd_collector",                                              │
│      plugin_factory=PLUGIN_FACTORY                                              │
│  ).run()                                                                         │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
┌─────────────────────────────────┐         ┌─────────────────────────────────┐
│         domain/plugin.py        │         │      adapters/ (42 adapters)    │
├─────────────────────────────────┤         ├─────────────────────────────────┤
│ PLUGIN_FACTORY = {              │         │ postgresql/                     │
│   "postgresql": PostgreSQLPlugin│         │ ├── adapter.py     (Adapter)    │
│   "mysql": MySQLPlugin,         │         │ ├── repository.py  (queries)    │
│   "snowflake": SnowflakePlugin, │         │ ├── models.py      (domain)     │
│   ...42 plugins                 │         │ └── mapper/        (transform)  │
│ }                               │         │     ├── tables.py               │
│                                 │         │     ├── columns.py              │
│ class PostgreSQLPlugin(Plugin): │         │     └── relationships/          │
│   type: Literal["postgresql"]   │         │                                 │
│   host: str                     │         │ mysql/                          │
│   port: int = 5432              │         │ snowflake/                      │
│   database: str                 │         │ mongodb/                        │
│   schemas_filter: Filter        │         │ kafka/                          │
│                                 │         │ ... (42 total)                  │
└─────────────────────────────────┘         └─────────────────────────────────┘
```

### Adapter Internal Structure

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    ADAPTER COMPONENT STRUCTURE                                   │
│                    (Repository Pattern Example)                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────────┐
                              │   Data Source       │
                              │   (PostgreSQL)      │
                              └──────────┬──────────┘
                                         │
                                         │ SQL queries
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              adapter.py                                          │
│                           class Adapter(BaseAdapter)                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                                            │
│  │ create_generator│ ──► PostgresqlGenerator (ODDRN)                           │
│  └─────────────────┘                                                            │
│                                                                                  │
│  ┌─────────────────────┐                                                        │
│  │ get_data_entity_list│                                                        │
│  └──────────┬──────────┘                                                        │
│             │                                                                    │
│             │  1. Fetch metadata                                                │
│             ▼                                                                    │
│  ┌─────────────────────┐     ┌─────────────────────┐                           │
│  │    Repository       │────►│     Models          │                           │
│  │  - get_schemas()    │     │  - Schema           │                           │
│  │  - get_tables()     │     │  - Table            │                           │
│  │  - get_columns()    │     │  - Column           │                           │
│  │  - get_fk_constraints│     │  - ForeignKey       │                           │
│  └─────────────────────┘     └──────────┬──────────┘                           │
│                                         │                                        │
│             │  2. Transform to ODD                                              │
│             ▼                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                           Mappers                                        │   │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                │   │
│  │  │ map_tables()  │  │ map_columns() │  │ map_relations │                │   │
│  │  │ Table→DataSet │  │ Col→DataSetFld│  │ FK→ERDRelation│                │   │
│  │  └───────────────┘  └───────────────┘  └───────────────┘                │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                        │
│             │  3. Return ODD entities                                           │
│             ▼                                                                    │
│  ┌─────────────────────┐                                                        │
│  │   DataEntityList    │                                                        │
│  │   - data_source_oddrn                                                        │
│  │   - items: [DataEntity...]                                                   │
│  └─────────────────────┘                                                        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Level 4: Code Level

### Key Class Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              CLASS HIERARCHY                                     │
└─────────────────────────────────────────────────────────────────────────────────┘

Adapter Hierarchy:
──────────────────
AbstractAdapter (ABC)                      # Minimal interface
├── get_data_source_oddrn() -> str         # Abstract
└── get_data_entity_list() -> DataEntityList  # Abstract
         │
         ├──► BaseAdapter                   # Most common base class
         │    ├── config: Plugin            # Injected config
         │    ├── generator: Generator      # ODDRN generator
         │    ├── create_generator()        # Abstract, must implement
         │    └── get_data_source_oddrn()   # Implemented via generator
         │              │
         │              └──► PostgreSQLAdapter
         │              └──► MySQLAdapter
         │              └──► SnowflakeAdapter
         │              └──► ... (most adapters)
         │
         └──► AsyncAbstractAdapter          # Async variant
              └── async get_data_entity_list()


Job Hierarchy:
──────────────
AbstractJob (ABC)
├── adapter: Adapter
├── api: PlatformApi
├── chunk_size: int
├── start()                    # Abstract
├── send_metadata()            # Sends chunked data
└── _split()                   # Generator for chunking
         │
         ├──► SyncJob          # For sync adapters
         ├──► AsyncJob         # For async adapters
         └──► AsyncGeneratorJob # For async generator adapters


Plugin Hierarchy:
─────────────────
pydantic.BaseSettings
└── Plugin
    ├── type: str              # Discriminator
    ├── name: str              # Unique identifier
    ├── description: Optional[str]
    └── namespace: Optional[str]
              │
              ├──► DatabasePlugin (WithHost, WithPort)
              │    ├──► PostgreSQLPlugin
              │    ├──► MySQLPlugin
              │    └──► ...
              │
              ├──► AwsPlugin
              │    ├──► S3Plugin
              │    ├──► GluePlugin
              │    └──► ...
              │
              └──► GcpPlugin
                   ├──► BigQueryStoragePlugin
                   └──► ...
```

### Core Interfaces (Contracts)

```python
# File: odd_collector_sdk/domain/adapter.py

class AbstractAdapter(ABC):
    """Minimal adapter interface - implement for full control"""

    @abstractmethod
    def get_data_source_oddrn(self) -> str:
        """Return unique ODDRN for this data source"""
        pass

    @abstractmethod
    def get_data_entity_list(self) -> DataEntityList:
        """Extract and return metadata as ODD entities"""
        pass


class BaseAdapter(AbstractAdapter):
    """Standard adapter base - handles ODDRN generation"""

    def __init__(self, config: Plugin) -> None:
        self.config = config
        self._generator = None

    @property
    def generator(self) -> Generator:
        if not self._generator:
            self._generator = self.create_generator()
        return self._generator

    @abstractmethod
    def create_generator(self) -> Generator:
        """Create ODDRN generator for this data source"""
        pass

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()


class AsyncAbstractAdapter(ABC):
    """Async adapter interface for IO-bound operations"""

    @abstractmethod
    def get_data_source_oddrn(self) -> str:
        pass

    @abstractmethod
    async def get_data_entity_list(self) -> DataEntityList:
        pass
```

---

## Data Flow Architecture

### End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                           │
└─────────────────────────────────────────────────────────────────────────────────┘

Phase 1: STARTUP
────────────────
collector_config.yaml ──► CollectorConfigLoader ──► CollectorConfig
                              │
                              ├── Parse YAML with env substitution
                              ├── Merge secrets backend (if configured)
                              └── Validate with Pydantic


Phase 2: ADAPTER INITIALIZATION
───────────────────────────────
CollectorConfig.plugins ──► load_adapters() ──► [Adapter, Adapter, ...]
                              │
                              ├── For each plugin:
                              │   ├── import_module("{root}.adapters.{type}")
                              │   ├── Get {type}/adapter.py::Adapter class
                              │   └── Instantiate with plugin config
                              └── Return list of adapters


Phase 3: DATA SOURCE REGISTRATION
─────────────────────────────────
[Adapters] ──► Collector.register_data_sources() ──► ODD Platform
                              │
                              ├── For each adapter:
                              │   └── Create DataSource(oddrn, name, description)
                              └── POST /ingestion/datasources


Phase 4: METADATA COLLECTION (per adapter)
──────────────────────────────────────────

┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Data Source  │────►│   Adapter    │────►│  Mapper(s)   │────►│DataEntityList│
│ (PostgreSQL) │     │              │     │              │     │              │
│              │     │ Repository   │     │ Domain→ODD   │     │ items: [     │
│ - Tables     │     │ - SQL query  │     │ - Table→     │     │   DataEntity │
│ - Columns    │     │ - Parse rows │     │   DataSet    │     │   DataEntity │
│ - FKs        │     │ - Build      │     │ - Column→    │     │   ...        │
│              │     │   models     │     │   DataSetFld │     │ ]            │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘


Phase 5: CHUNKING & INGESTION
─────────────────────────────
DataEntityList ──► Job._split() ──► [Chunk1, Chunk2, ...] ──► PlatformApi
                         │                                        │
                         │ funcy.chunks(250)                      │
                         │                                        ▼
                         │                              POST /ingestion/entities
                         │                              (for each chunk)
                         └────────────────────────────────────────┘


Phase 6: SCHEDULING (Daemon Mode)
─────────────────────────────────
┌─────────────────────────────────────────────────────────────────────────────┐
│                          APScheduler                                         │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                   │
│  │ Job: postgres │  │ Job: mysql    │  │ Job: snowflake│  ...              │
│  │ interval: 60m │  │ interval: 60m │  │ interval: 60m │                   │
│  │ next: now     │  │ next: now     │  │ next: now     │                   │
│  └───────────────┘  └───────────────┘  └───────────────┘                   │
│                                                                              │
│  - AsyncIOScheduler with timezone                                           │
│  - max_instances: 1 (no overlap)                                            │
│  - coalesce: true (skip missed)                                             │
│  - misfire_grace_time: configurable                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Architecture Review & Improvement Suggestions

### Strengths

| Aspect | Description |
|--------|-------------|
| **Modularity** | Clean separation: SDK framework vs specific collectors vs adapters |
| **Extensibility** | Adding new adapters follows clear conventions |
| **Dynamic Loading** | Plugin-based architecture with runtime adapter discovery |
| **Async-First** | Proper async/await support with automatic job type selection |
| **Configuration Flexibility** | YAML + env vars + secrets backend priority chain |
| **Type Safety** | Pydantic validation throughout configuration layer |

### Areas for Improvement

#### 1. Error Handling & Resilience

**Current State:**
- Exceptions in adapters are logged but may not be properly surfaced
- No retry mechanism for transient failures
- No circuit breaker pattern for failing data sources

**Suggestions:**
```python
# Add retry decorator for transient failures
from tenacity import retry, stop_after_attempt, wait_exponential

class BaseAdapter:
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_data_entity_list(self) -> DataEntityList:
        # Implementation
        pass

# Add circuit breaker for repeated failures
from circuitbreaker import circuit

@circuit(failure_threshold=5, recovery_timeout=300)
def connect_to_source(self):
    pass
```

**Priority:** HIGH

---

#### 2. Observability & Monitoring

**Current State:**
- Logging via loguru (structured)
- No metrics export (Prometheus, StatsD)
- No distributed tracing

**Suggestions:**
```python
# Add OpenTelemetry integration
from opentelemetry import trace, metrics

class Collector:
    def __init__(self):
        self.tracer = trace.get_tracer(__name__)
        self.meter = metrics.get_meter(__name__)

        # Metrics
        self.entities_collected = self.meter.create_counter(
            "odd.collector.entities_collected",
            description="Number of entities collected"
        )
        self.collection_duration = self.meter.create_histogram(
            "odd.collector.collection_duration_seconds",
            description="Time to collect metadata"
        )
```

**Priority:** MEDIUM

---

#### 3. Testing Infrastructure

**Current State:**
- Integration tests use testcontainers
- No mock/stub framework for unit testing adapters
- Limited test coverage for SDK core

**Suggestions:**
```python
# Add adapter test fixtures
@pytest.fixture
def mock_adapter_config():
    return PostgreSQLPlugin(
        type="postgresql",
        name="test",
        host="localhost",
        port=5432,
        database="test",
        user="test",
        password="test"
    )

# Add adapter contract tests
class AdapterContractTest:
    """Base class ensuring all adapters follow contract"""

    def test_returns_data_entity_list(self, adapter):
        result = adapter.get_data_entity_list()
        assert isinstance(result, DataEntityList)

    def test_has_valid_oddrn(self, adapter):
        oddrn = adapter.get_data_source_oddrn()
        assert oddrn.startswith("//")
```

**Priority:** HIGH

---

#### 4. Configuration Validation

**Current State:**
- Runtime validation only
- No schema documentation auto-generation
- Missing config validation CLI command

**Suggestions:**
```bash
# Add config validation command
python -m odd_collector validate --config collector_config.yaml

# Generate JSON Schema for IDE support
python -m odd_collector schema > collector_config.schema.json
```

```python
# Add pre-flight checks
class Collector:
    def validate_config(self) -> list[str]:
        """Validate config before running"""
        errors = []
        for adapter in self.adapters:
            try:
                adapter.test_connection()
            except Exception as e:
                errors.append(f"{adapter.config.name}: {e}")
        return errors
```

**Priority:** MEDIUM

---

#### 5. Dependency Injection

**Current State:**
- Hard-coded dependencies in Collector class
- Difficult to mock for testing
- Tight coupling between components

**Suggestions:**
```python
# Use dependency injection container
from dependency_injector import containers, providers

class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    platform_api = providers.Singleton(
        PlatformApi,
        token=config.token,
        platform_url=config.platform_host_url
    )

    collector = providers.Singleton(
        Collector,
        api=platform_api,
        config=config
    )
```

**Priority:** LOW (significant refactor)

---

#### 6. Plugin Discovery Enhancement

**Current State:**
- Hardcoded PLUGIN_FACTORY dictionary
- Must manually register each plugin

**Suggestions:**
```python
# Auto-discovery via decorators
@register_adapter("postgresql")
class PostgreSQLAdapter(BaseAdapter):
    pass

# Or entry points (pyproject.toml)
[project.entry-points."odd_collector.adapters"]
postgresql = "odd_collector.adapters.postgresql:Adapter"
```

**Priority:** LOW

---

#### 7. Health Check Endpoint

**Current State:**
- No HTTP health endpoint
- Difficult to integrate with Kubernetes probes

**Suggestions:**
```python
# Add lightweight health server
from aiohttp import web

class HealthServer:
    def __init__(self, collector: Collector):
        self.collector = collector

    async def health(self, request):
        return web.json_response({
            "status": "healthy",
            "adapters": len(self.collector.adapters),
            "last_run": self.collector.last_run_time
        })

    async def ready(self, request):
        # Check all adapters can connect
        pass
```

**Priority:** MEDIUM (for production deployments)

---

#### 8. Rate Limiting & Backpressure

**Current State:**
- No rate limiting for API calls
- No backpressure mechanism when platform is slow

**Suggestions:**
```python
# Add rate limiter
from aiolimiter import AsyncLimiter

class PlatformApi:
    def __init__(self):
        # Max 10 requests per second
        self.rate_limiter = AsyncLimiter(10, 1)

    async def ingest_data(self, data: DataEntityList):
        async with self.rate_limiter:
            await self._do_ingest(data)
```

**Priority:** MEDIUM

---

### Summary: Improvement Roadmap

| Priority | Improvement | Effort | Impact |
|----------|-------------|--------|--------|
| HIGH | Error handling & retries | Medium | High |
| HIGH | Testing infrastructure | Medium | High |
| MEDIUM | Observability (metrics/tracing) | Medium | High |
| MEDIUM | Config validation CLI | Low | Medium |
| MEDIUM | Health check endpoint | Low | Medium |
| MEDIUM | Rate limiting | Low | Medium |
| LOW | Dependency injection | High | Medium |
| LOW | Plugin auto-discovery | Medium | Low |

---

## Appendix: Key File Locations

```
SDK Core:
  odd-collector-sdk/odd_collector_sdk/
  ├── collector.py              # Main orchestrator
  ├── job.py                    # Job execution (sync/async)
  ├── load_adapter.py           # Dynamic adapter loading
  ├── domain/
  │   ├── adapter.py            # Adapter interfaces
  │   ├── plugin.py             # Plugin base model
  │   ├── filter.py             # Include/exclude filtering
  │   ├── collector_config.py   # Config model
  │   └── collector_config_loader.py  # Config loading
  ├── api/
  │   └── datasource_api.py     # Platform REST client
  └── secrets/
      ├── secrets_backend.py    # Backend interface
      └── aws/                  # AWS SSM implementation

Generic Collector:
  odd-collector/odd_collector/
  ├── __main__.py               # Entry point
  ├── domain/plugin.py          # 42 plugin definitions
  └── adapters/                 # 42 adapter implementations

Cloud Collectors:
  odd-collector-{aws,azure,gcp}/
  └── (similar structure)
```

---

*Document generated: 2026-01-25*
*C4 Model Level: Context, Container, Component, Code*
