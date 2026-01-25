# ODD Collectors - Developer Manual

> This manual is intended for developers who need to understand, maintain, or refactor the ODD Collectors codebase.
> It assumes familiarity with Python, async programming, and basic software architecture concepts.

---

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Codebase Navigation](#codebase-navigation)
3. [Core Concepts & How They Work](#core-concepts--how-they-work)
4. [Code Walkthrough: Request Lifecycle](#code-walkthrough-request-lifecycle)
5. [Key Patterns & Conventions](#key-patterns--conventions)
6. [Adding a New Adapter](#adding-a-new-adapter)
7. [Common Refactoring Tasks](#common-refactoring-tasks)
8. [Testing Guide](#testing-guide)
9. [Debugging Tips](#debugging-tips)
10. [Dependency Analysis](#dependency-analysis)

---

## Development Environment Setup

### Prerequisites

- Python 3.9+
- Poetry (dependency management)
- Docker (for integration tests)
- Git

### Initial Setup

```bash
# Clone repository
git clone <repository-url>
cd odd-collectors

# Setup a specific collector (example: odd-collector)
cd odd-collector
poetry install

# Activate virtual environment
poetry shell

# Verify installation
python -m odd_collector --help
```

### IDE Configuration

**VS Code recommended extensions:**
- Python
- Pylance (type checking)
- Black Formatter
- isort

**PyCharm:**
- Mark `odd-collector-sdk/odd_collector_sdk` as Sources Root
- Configure Black as external tool

### Running Locally

```bash
# Create config file
cp config_examples/postgresql.yaml collector_config.yaml
# Edit collector_config.yaml with your settings

# Run collector
python -m odd_collector

# With debug logging
LOGLEVEL=DEBUG python -m odd_collector
```

---

## Codebase Navigation

### Directory Structure

```
odd-collectors/
├── odd-collector-sdk/          # SHARED SDK (read this first)
│   ├── odd_collector_sdk/
│   │   ├── collector.py        # Main orchestrator class
│   │   ├── job.py              # Job execution (SyncJob, AsyncJob, etc.)
│   │   ├── load_adapter.py     # Dynamic adapter loading
│   │   ├── logger.py           # Loguru configuration
│   │   ├── domain/
│   │   │   ├── adapter.py      # Adapter interfaces (AbstractAdapter, BaseAdapter)
│   │   │   ├── plugin.py       # Plugin base class
│   │   │   ├── filter.py       # Include/exclude filtering
│   │   │   ├── collector_config.py      # Config model
│   │   │   └── collector_config_loader.py  # Config loading logic
│   │   ├── api/
│   │   │   └── datasource_api.py   # ODD Platform REST client
│   │   ├── secrets/
│   │   │   ├── secrets_backend.py      # Base interface
│   │   │   ├── secrets_backend_factory.py
│   │   │   └── aws/                    # AWS SSM implementation
│   │   ├── grammar_parser/
│   │   │   └── build_dataset_field.py  # Complex type parsing
│   │   └── utils/
│   │       └── yaml_parser.py          # YAML with env substitution
│   └── pyproject.toml
│
├── odd-collector/              # GENERIC COLLECTOR
│   ├── odd_collector/
│   │   ├── __main__.py         # Entry point
│   │   ├── __version__.py      # Version string
│   │   ├── domain/
│   │   │   └── plugin.py       # 42 plugin definitions + PLUGIN_FACTORY
│   │   └── adapters/           # 42 adapters
│   │       ├── postgresql/
│   │       │   ├── adapter.py      # Main adapter class
│   │       │   ├── repository.py   # SQL queries
│   │       │   ├── models.py       # Domain models
│   │       │   ├── mapper/         # ODD entity mappers
│   │       │   └── ...
│   │       ├── mysql/
│   │       ├── snowflake/
│   │       └── ... (42 total)
│   ├── config_examples/        # Sample YAML configs
│   ├── tests/
│   └── pyproject.toml
│
├── odd-collector-aws/          # AWS COLLECTOR
├── odd-collector-azure/        # AZURE COLLECTOR
├── odd-collector-gcp/          # GCP COLLECTOR
└── pyproject.toml              # Root monorepo config
```

### Key Files to Understand First

| Priority | File | Purpose |
|----------|------|---------|
| 1 | `sdk/domain/adapter.py` | Core adapter interfaces |
| 2 | `sdk/collector.py` | Main orchestration logic |
| 3 | `sdk/job.py` | Job execution patterns |
| 4 | `sdk/load_adapter.py` | Dynamic loading mechanism |
| 5 | `sdk/domain/plugin.py` | Plugin configuration base |
| 6 | `collector/domain/plugin.py` | All plugin definitions |
| 7 | Any adapter `adapter.py` | Concrete implementation example |

---

## Core Concepts & How They Work

### 1. Plugin System

**What it does:** Defines configuration schema for each adapter type.

**How it works:**

```python
# File: odd_collector/domain/plugin.py

class PostgreSQLPlugin(Plugin):
    """Configuration for PostgreSQL adapter"""
    type: Literal["postgresql"]  # MUST match adapter directory name
    host: str
    port: int = 5432
    database: str
    user: str
    password: SecretStr
    schemas_filter: Filter = Filter()

# Registry maps type string to Plugin class
PLUGIN_FACTORY: PluginFactory = {
    "postgresql": PostgreSQLPlugin,
    "mysql": MySQLPlugin,
    # ... 42 entries
}
```

**Key points:**
- `type` field is the discriminator - it MUST match the adapter directory name
- Pydantic validates all fields at startup
- `extra = "allow"` permits forward compatibility

### 2. Dynamic Adapter Loading

**What it does:** Loads adapter classes at runtime based on plugin configuration.

**How it works:**

```python
# File: sdk/load_adapter.py

def load_adapters(root_package: str, plugins: list[Plugin]) -> list[Adapter]:
    adapters = []
    for plugin in plugins:
        # Import path: odd_collector.adapters.postgresql
        package = load_package(f"{root_package}.{plugin.type}")

        # Get Adapter class from {type}/adapter.py
        adapter = package.adapter.Adapter(plugin)
        adapters.append(adapter)
    return adapters

@cache  # Results cached for performance
def load_package(package_path: str) -> ModuleType:
    package = import_module(package_path)
    import_submodules(package)  # Recursively import all .py files
    return package
```

**Key points:**
- Convention: `adapters/{type}/adapter.py` must have class named `Adapter`
- Uses `@cache` to avoid re-importing modules
- `import_submodules` loads all Python files in the adapter directory

### 3. Adapter Interface

**What it does:** Defines contract that all adapters must implement.

**How it works:**

```python
# File: sdk/domain/adapter.py

class AbstractAdapter(ABC):
    """Minimal interface - use when you need full control"""

    @abstractmethod
    def get_data_source_oddrn(self) -> str:
        """Return unique ODDRN for this data source"""
        pass

    @abstractmethod
    def get_data_entity_list(self) -> DataEntityList:
        """Extract metadata and return as ODD entities"""
        pass


class BaseAdapter(AbstractAdapter):
    """Standard base - handles ODDRN generation automatically"""

    def __init__(self, config: Plugin) -> None:
        self.config = config
        self._generator = None

    @property
    def generator(self) -> Generator:
        """Lazy-load ODDRN generator"""
        if not self._generator:
            self._generator = self.create_generator()
        return self._generator

    @abstractmethod
    def create_generator(self) -> Generator:
        """Subclass must return appropriate ODDRN generator"""
        pass

    def get_data_source_oddrn(self) -> str:
        """Implemented: uses generator to create ODDRN"""
        return self.generator.get_data_source_oddrn()
```

**Key points:**
- Most adapters extend `BaseAdapter`
- `create_generator()` returns ODDRN generator from `oddrn-generator` library
- `get_data_entity_list()` is the main method - fetch and transform metadata

### 4. Job System

**What it does:** Executes adapter's `get_data_entity_list()` and sends results to platform.

**How it works:**

```python
# File: sdk/job.py

def create_job(adapter: Adapter, api: PlatformApi, chunk_size: int) -> AbstractJob:
    """Factory: selects job type based on adapter method signature"""
    method = adapter.get_data_entity_list

    if inspect.isasyncgenfunction(method):
        return AsyncGeneratorJob(adapter, api, chunk_size)
    elif inspect.iscoroutinefunction(method):
        return AsyncJob(adapter, api, chunk_size)
    else:
        return SyncJob(adapter, api, chunk_size)


class SyncJob(AbstractJob):
    async def start(self):
        # Call sync method
        data_entity_list = self.adapter.get_data_entity_list()

        # Chunk and send
        for chunk in self._split(data_entity_list):
            await self.send_metadata(chunk)

    def _split(self, data_entity_list: DataEntityList) -> Generator:
        """Split into chunks of chunk_size (default 250)"""
        for items_chunk in funcy.chunks(self.chunk_size, data_entity_list.items):
            yield DataEntityList(
                data_source_oddrn=data_entity_list.data_source_oddrn,
                items=list(items_chunk)
            )
```

**Key points:**
- Job type is automatically selected based on method signature
- Data is chunked to avoid large payloads (default: 250 entities per chunk)
- Uses `funcy.chunks` for memory-efficient iteration

### 5. Collector Orchestrator

**What it does:** Main entry point that ties everything together.

**How it works:**

```python
# File: sdk/collector.py

class Collector:
    def __init__(self, config_path, root_package, plugin_factory):
        # Load and validate configuration
        self.config = CollectorConfigLoader(config_path, plugin_factory).load()

        # Initialize API client
        self.api = PlatformApi(
            token=self.config.token,
            platform_url=self.config.platform_host_url
        )

        # Load adapter instances
        self.adapters = load_adapters(
            f"{root_package}.adapters",
            self.config.plugins
        )

    def run(self):
        """Main entry point"""
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

        # Register data sources with platform
        asyncio.run(self.register_data_sources())

        if self.config.default_pulling_interval:
            # Daemon mode: schedule periodic jobs
            self.start_polling()
        else:
            # One-time mode: run all adapters once
            asyncio.run(self.one_time_run())

    def start_polling(self):
        """Schedule jobs with APScheduler"""
        scheduler = AsyncIOScheduler(timezone=tzlocal())

        for adapter in self.adapters:
            job = create_job(adapter, self.api, self.config.chunk_size)
            scheduler.add_job(
                job.start,
                trigger="interval",
                minutes=self.config.default_pulling_interval,
                next_run_time=datetime.now(tz=tzlocal()),  # Run immediately
                max_instances=self.config.max_instances,
                coalesce=True  # Skip missed runs
            )

        scheduler.start()
        asyncio.get_event_loop().run_forever()
```

**Key points:**
- Entry point is `Collector.run()`
- Two modes: one-time (`default_pulling_interval=None`) or daemon
- APScheduler handles job scheduling with coalescing

### 6. Configuration Loading

**What it does:** Loads and validates configuration from multiple sources.

**How it works:**

```python
# File: sdk/domain/collector_config_loader.py

class CollectorConfigLoader:
    def load(self) -> CollectorConfig:
        # 1. Parse YAML file
        conf_dict = self._parse_config()

        # 2. Extract sections
        plugins = conf_dict.pop("plugins", [])
        secrets_backend = conf_dict.pop("secrets_backend", None)
        collector_settings = conf_dict

        # 3. Merge with secrets backend (if configured)
        if secrets_backend is not None:
            sb_provider = SecretsBackendFactory(...).get_provider()
            collector_settings = self._merge_collector_settings(
                sb_provider.get_collector_settings(),  # Priority
                collector_settings                      # Fallback
            )
            plugins = self._merge_plugins(
                sb_provider.get_plugins(),  # Priority
                plugins                      # Fallback
            )

        # 4. Validate plugins using factory
        plugins = [
            self.plugin_factory[plugin["type"]].model_validate(plugin)
            for plugin in plugins
        ]

        # 5. Return validated config
        return CollectorConfig.model_validate({
            **collector_settings,
            "plugins": plugins
        })
```

**Priority order:**
1. Secrets Backend (AWS SSM, etc.)
2. YAML configuration file
3. Environment variables (Pydantic BaseSettings)
4. Default values

---

## Code Walkthrough: Request Lifecycle

### Startup Sequence

```
1. __main__.py
   │
   ├── Create Collector instance
   │   ├── CollectorConfigLoader.load()
   │   │   ├── Parse YAML (with env substitution)
   │   │   ├── Merge secrets backend
   │   │   └── Validate with Pydantic
   │   │
   │   ├── Create PlatformApi client
   │   │
   │   └── load_adapters()
   │       ├── For each plugin in config:
   │       │   ├── import_module("{root}.adapters.{type}")
   │       │   ├── Load {type}/adapter.py::Adapter
   │       │   └── Instantiate Adapter(plugin)
   │       └── Return [adapter1, adapter2, ...]
   │
   └── collector.run()
       ├── Setup signal handlers (SIGTERM, SIGINT)
       ├── register_data_sources()
       │   └── POST /ingestion/datasources
       │
       └── start_polling() OR one_time_run()
```

### Metadata Collection Sequence

```
2. Job Execution (per adapter)
   │
   ├── APScheduler triggers job.start()
   │
   ├── adapter.get_data_entity_list()
   │   │
   │   ├── [Connect to data source]
   │   │   Example: psycopg2.connect(...)
   │   │
   │   ├── [Fetch metadata via Repository]
   │   │   Example: SELECT * FROM information_schema.tables
   │   │
   │   ├── [Build domain models]
   │   │   Example: Table(name="users", columns=[...])
   │   │
   │   ├── [Map to ODD entities]
   │   │   Example: map_table(table, generator) -> DataEntity
   │   │
   │   └── Return DataEntityList(items=[...])
   │
   ├── Job._split(data_entity_list)
   │   └── Yield chunks of 250 items each
   │
   └── For each chunk:
       └── Job.send_metadata(chunk)
           └── PlatformApi.ingest_data(chunk)
               └── POST /ingestion/entities
```

### Adapter Internal Flow (PostgreSQL Example)

```
3. PostgreSQL Adapter
   │
   ├── adapter.py: Adapter.get_data_entity_list()
   │   │
   │   ├── repository.py: PostgreSQLRepository
   │   │   ├── get_schemas() -> [Schema]
   │   │   ├── get_tables() -> [Table]
   │   │   ├── get_columns() -> [Column]
   │   │   └── get_foreign_keys() -> [ForeignKey]
   │   │
   │   ├── models.py: Domain objects
   │   │   class Table:
   │   │       name: str
   │   │       schema: str
   │   │       columns: list[Column]
   │   │
   │   └── mapper/: Transform to ODD
   │       ├── tables.py: map_tables(tables, generator)
   │       │   └── Table -> DataEntity(type=DATA_SET)
   │       │
   │       ├── columns.py: map_columns(columns, generator)
   │       │   └── Column -> DataSetField
   │       │
   │       └── relationships.py: map_relationships(fks, generator)
   │           └── ForeignKey -> ERDRelation
   │
   └── Return DataEntityList
```

---

## Key Patterns & Conventions

### 1. Repository Pattern

Used by database adapters to separate data access from business logic.

```python
# repository.py
class PostgreSQLRepository:
    def __init__(self, connection: Connection):
        self.connection = connection

    def get_tables(self) -> list[Table]:
        query = """
            SELECT table_name, table_schema, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """
        rows = self.connection.execute(query)
        return [Table(**row) for row in rows]
```

**Benefits:**
- SQL is isolated in one place
- Easy to test with mock connections
- Separation of concerns

### 2. Mapper Pattern

Transforms domain models to ODD entities.

```python
# mapper/tables.py
def map_table(table: Table, generator: PostgresqlGenerator) -> DataEntity:
    generator.set_oddrn_paths(schemas=table.schema, tables=table.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.name,
        type=DataEntityType.TABLE,
        dataset=DataSet(
            field_list=[map_column(col, generator) for col in table.columns]
        )
    )
```

**Benefits:**
- Clear transformation logic
- Reusable across adapters with similar models
- Easy to extend for new fields

### 3. Filter Pattern

Configurable include/exclude filtering.

```python
# Using Filter in adapter
class Adapter(BaseAdapter):
    def get_data_entity_list(self) -> DataEntityList:
        all_schemas = self.repository.get_schemas()

        # Apply filter from config
        filtered_schemas = [
            s for s in all_schemas
            if self.config.schemas_filter.is_allowed(s.name)
        ]
        # ...
```

```yaml
# collector_config.yaml
plugins:
  - type: postgresql
    schemas_filter:
      include: ["public.*", "sales.*"]
      exclude: ["temp_.*"]
```

### 4. ODDRN Generation

Every entity needs a unique ODDRN (Open Data Discovery Resource Name).

```python
# Using oddrn-generator library
from oddrn_generator import PostgresqlGenerator

generator = PostgresqlGenerator(
    host_settings="db.example.com",
    databases="mydb"
)

# Set path components
generator.set_oddrn_paths(schemas="public", tables="users")

# Get ODDRN
oddrn = generator.get_oddrn_by_path("tables")
# Result: //postgresql/host/db.example.com/databases/mydb/schemas/public/tables/users
```

### 5. Async Adapter Pattern

For IO-bound operations (API calls, cloud services).

```python
class Adapter(AsyncAbstractAdapter):
    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        # Async operations
        async with aiohttp.ClientSession() as session:
            data = await session.get(self.api_url)
            # ...
        return DataEntityList(...)
```

### 6. TTL Cache Pattern

Avoid redundant queries within a collection run.

```python
from functools import cached_property
from cachetools import TTLCache

class Adapter(BaseAdapter):
    CACHE_TTL = 60  # seconds

    @cached_property
    def _metadata(self):
        """Cached metadata - fetched once per run"""
        return self.repository.get_all_metadata()

    def get_data_entity_list(self) -> DataEntityList:
        metadata = self._metadata  # Uses cache
        # ...
```

---

## Adding a New Adapter

### Step-by-Step Guide

#### Step 1: Create Adapter Directory

```bash
cd odd-collector/odd_collector/adapters
mkdir my_adapter
touch my_adapter/__init__.py
touch my_adapter/adapter.py
```

#### Step 2: Define Plugin Configuration

```python
# File: odd_collector/domain/plugin.py

class MyAdapterPlugin(Plugin):
    """Configuration for My Adapter"""
    type: Literal["my_adapter"]  # MUST match directory name

    # Connection settings
    host: str
    port: int = 8080
    api_key: SecretStr

    # Optional settings
    timeout: int = 30
    verify_ssl: bool = True

# Add to factory
PLUGIN_FACTORY: PluginFactory = {
    # ... existing entries
    "my_adapter": MyAdapterPlugin,
}
```

#### Step 3: Implement Adapter

```python
# File: odd_collector/adapters/my_adapter/adapter.py

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList, DataEntity
from oddrn_generator import MyAdapterGenerator  # or create custom

from odd_collector.domain.plugin import MyAdapterPlugin


class Adapter(BaseAdapter):
    config: MyAdapterPlugin

    def __init__(self, config: MyAdapterPlugin) -> None:
        super().__init__(config)
        # Initialize client/connection
        self.client = MyClient(
            host=config.host,
            port=config.port,
            api_key=config.api_key.get_secret_value()
        )

    def create_generator(self) -> MyAdapterGenerator:
        return MyAdapterGenerator(
            host_settings=self.config.host
        )

    def get_data_entity_list(self) -> DataEntityList:
        # 1. Fetch metadata
        items = self.client.list_items()

        # 2. Map to ODD entities
        entities = [self._map_item(item) for item in items]

        # 3. Return list
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=entities
        )

    def _map_item(self, item) -> DataEntity:
        self.generator.set_oddrn_paths(items=item.name)
        return DataEntity(
            oddrn=self.generator.get_oddrn_by_path("items"),
            name=item.name,
            type=DataEntityType.TABLE,
            # ... other fields
        )
```

#### Step 4: Add Configuration Example

```yaml
# File: odd_collector/config_examples/my_adapter.yaml
platform_host_url: http://localhost:8080
token: ""

plugins:
  - type: my_adapter
    name: my_instance
    host: api.example.com
    port: 443
    api_key: "${MY_ADAPTER_API_KEY}"
    timeout: 60
    verify_ssl: true
```

#### Step 5: Write Tests

```python
# File: odd_collector/tests/integration/test_my_adapter.py
import pytest
from odd_collector.adapters.my_adapter.adapter import Adapter
from odd_collector.domain.plugin import MyAdapterPlugin


@pytest.fixture
def config():
    return MyAdapterPlugin(
        type="my_adapter",
        name="test",
        host="localhost",
        port=8080,
        api_key="test-key"
    )


def test_adapter_returns_data_entity_list(config, mock_client):
    adapter = Adapter(config)
    result = adapter.get_data_entity_list()

    assert isinstance(result, DataEntityList)
    assert len(result.items) > 0


def test_adapter_generates_valid_oddrn(config):
    adapter = Adapter(config)
    oddrn = adapter.get_data_source_oddrn()

    assert oddrn.startswith("//")
    assert "localhost" in oddrn
```

#### Step 6: Test Locally

```bash
# Create config
cp config_examples/my_adapter.yaml collector_config.yaml
# Edit with real credentials

# Run
python -m odd_collector
```

---

## Common Refactoring Tasks

### 1. Updating Pydantic from v1 to v2

**Files affected:** All `plugin.py` files, config models

**Changes needed:**

```python
# Before (Pydantic v1)
class MyPlugin(Plugin):
    class Config:
        extra = "allow"

    @validator("host")
    def validate_host(cls, v):
        return v.lower()

# After (Pydantic v2)
class MyPlugin(Plugin):
    model_config = ConfigDict(extra="allow")

    @field_validator("host")
    @classmethod
    def validate_host(cls, v: str) -> str:
        return v.lower()
```

**Search patterns:**
```bash
grep -r "class Config:" --include="*.py"
grep -r "@validator" --include="*.py"
grep -r "\.dict()" --include="*.py"  # Now .model_dump()
grep -r "\.parse_obj" --include="*.py"  # Now .model_validate()
```

### 2. Adding a New Field to Existing Adapter

**Example:** Add `connection_pool_size` to PostgreSQL adapter

**Step 1:** Update plugin

```python
# odd_collector/domain/plugin.py
class PostgreSQLPlugin(DatabasePlugin):
    # ... existing fields
    connection_pool_size: int = 5  # New field with default
```

**Step 2:** Use in adapter

```python
# odd_collector/adapters/postgresql/adapter.py
def __init__(self, config: PostgreSQLPlugin):
    super().__init__(config)
    self.pool = ConnectionPool(
        max_size=config.connection_pool_size  # Use new field
    )
```

**Step 3:** Update config example

```yaml
# config_examples/postgresql.yaml
plugins:
  - type: postgresql
    # ... existing fields
    connection_pool_size: 10
```

### 3. Extracting Common Logic to SDK

**When:** Multiple adapters share similar code

**Example:** Common database repository base

**Step 1:** Create base class in SDK

```python
# odd_collector_sdk/domain/database_repository.py
class BaseDatabaseRepository(ABC):
    def __init__(self, connection):
        self.connection = connection

    def execute_query(self, query: str) -> list[dict]:
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @abstractmethod
    def get_tables(self) -> list:
        pass
```

**Step 2:** Update adapters to use base

```python
# odd_collector/adapters/postgresql/repository.py
from odd_collector_sdk.domain.database_repository import BaseDatabaseRepository

class PostgreSQLRepository(BaseDatabaseRepository):
    def get_tables(self) -> list[Table]:
        rows = self.execute_query("SELECT * FROM information_schema.tables")
        return [Table(**row) for row in rows]
```

### 4. Migrating Adapter to Async

**When:** Adapter does IO-bound work that can be parallelized

**Before:**

```python
class Adapter(BaseAdapter):
    def get_data_entity_list(self) -> DataEntityList:
        data = self.client.fetch()  # Blocking
        return DataEntityList(...)
```

**After:**

```python
from odd_collector_sdk.domain.adapter import AsyncAbstractAdapter

class Adapter(AsyncAbstractAdapter):
    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        async with aiohttp.ClientSession() as session:
            data = await session.get(self.url)  # Non-blocking
        return DataEntityList(...)
```

**Note:** The SDK automatically detects async methods and uses `AsyncJob`.

### 5. Adding Filtering Support

**Step 1:** Add filter field to plugin

```python
class MyPlugin(Plugin):
    items_filter: Filter = Filter()  # Default: allow all
```

**Step 2:** Apply filter in adapter

```python
def get_data_entity_list(self) -> DataEntityList:
    all_items = self.client.list_items()

    filtered = [
        item for item in all_items
        if self.config.items_filter.is_allowed(item.name)
    ]

    return DataEntityList(items=[self._map(i) for i in filtered])
```

---

## Testing Guide

### Test Structure

```
tests/
├── unit/                    # Fast, no external dependencies
│   ├── test_plugin.py       # Plugin validation
│   └── test_mapper.py       # Mapping logic
├── integration/             # Requires external services
│   ├── test_postgresql.py   # Uses testcontainers
│   └── test_s3.py           # Uses moto
└── conftest.py              # Shared fixtures
```

### Running Tests

```bash
# All tests
poetry run pytest ./tests -v

# Unit tests only
poetry run pytest ./tests/unit -v

# Integration tests only
poetry run pytest ./tests -v -m integration

# Specific adapter
poetry run pytest ./tests/integration/test_postgresql.py -v

# With coverage
poetry run pytest --cov=odd_collector --cov-report=html
```

### Writing Unit Tests

```python
# tests/unit/test_postgresql_mapper.py
import pytest
from odd_collector.adapters.postgresql.mapper.tables import map_table
from odd_collector.adapters.postgresql.models import Table, Column


def test_map_table_creates_data_entity():
    table = Table(
        name="users",
        schema="public",
        columns=[Column(name="id", type="integer")]
    )
    generator = MockGenerator()

    entity = map_table(table, generator)

    assert entity.name == "users"
    assert entity.type == DataEntityType.TABLE
    assert len(entity.dataset.field_list) == 1
```

### Writing Integration Tests

```python
# tests/integration/test_postgresql.py
import pytest
from testcontainers.postgres import PostgresContainer

from odd_collector.adapters.postgresql.adapter import Adapter
from odd_collector.domain.plugin import PostgreSQLPlugin


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:14") as container:
        yield container


@pytest.fixture
def adapter(postgres_container):
    config = PostgreSQLPlugin(
        type="postgresql",
        name="test",
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        database="test",
        user="test",
        password="test"
    )
    return Adapter(config)


@pytest.mark.integration
def test_adapter_extracts_tables(adapter, postgres_container):
    # Setup: create test table
    connection = postgres_container.get_connection_url()
    # ... create table ...

    result = adapter.get_data_entity_list()

    assert len(result.items) > 0
    assert any(e.name == "test_table" for e in result.items)
```

---

## Debugging Tips

### Enable Debug Logging

```bash
# Environment variable
export LOGLEVEL=DEBUG
python -m odd_collector

# Or in code
from odd_collector_sdk.logger import logger
logger.level("DEBUG")
```

### Common Issues

#### 1. Adapter Not Loading

**Symptom:** `KeyError: 'my_adapter'`

**Causes:**
- Plugin not added to `PLUGIN_FACTORY`
- `type` in config doesn't match plugin `type` literal
- Adapter directory name doesn't match plugin type

**Debug:**
```python
# Check PLUGIN_FACTORY
from odd_collector.domain.plugin import PLUGIN_FACTORY
print(PLUGIN_FACTORY.keys())
```

#### 2. ODDRN Generation Errors

**Symptom:** `ValueError: Path 'tables' not found`

**Causes:**
- Calling `get_oddrn_by_path()` before `set_oddrn_paths()`
- Wrong path name

**Debug:**
```python
generator.set_oddrn_paths(schemas="public", tables="users")
print(generator.available_paths())  # Check available paths
```

#### 3. Connection Errors

**Symptom:** `ConnectionRefusedError`

**Debug steps:**
1. Verify config values are correct
2. Test connection manually
3. Check network/firewall

```python
# Test connection manually
import psycopg2
conn = psycopg2.connect(
    host="...",
    port=5432,
    database="...",
    user="...",
    password="..."
)
```

#### 4. Pydantic Validation Errors

**Symptom:** `ValidationError: 1 validation error for PostgreSQLPlugin`

**Debug:**
```python
# Check what's being passed
import yaml
with open("collector_config.yaml") as f:
    config = yaml.safe_load(f)
print(config["plugins"][0])

# Validate manually
from odd_collector.domain.plugin import PostgreSQLPlugin
PostgreSQLPlugin.model_validate(config["plugins"][0])
```

### Useful Debug Commands

```bash
# Check module can be imported
python -c "from odd_collector.adapters.postgresql.adapter import Adapter; print('OK')"

# Validate config
python -c "
from odd_collector_sdk.domain.collector_config_loader import CollectorConfigLoader
from odd_collector.domain.plugin import PLUGIN_FACTORY
config = CollectorConfigLoader('./collector_config.yaml', PLUGIN_FACTORY).load()
print(f'Loaded {len(config.plugins)} plugins')
"

# Test single adapter
python -c "
from odd_collector.adapters.postgresql.adapter import Adapter
from odd_collector.domain.plugin import PostgreSQLPlugin

config = PostgreSQLPlugin(
    type='postgresql',
    name='test',
    host='localhost',
    port=5432,
    database='testdb',
    user='postgres',
    password='postgres'
)
adapter = Adapter(config)
result = adapter.get_data_entity_list()
print(f'Found {len(result.items)} entities')
"
```

---

## Dependency Analysis

### Core Dependencies

| Package | Version | Purpose | Used By |
|---------|---------|---------|---------|
| `pydantic` | ^2.7.1 | Config validation | All plugins, config |
| `pydantic-settings` | ^2.2.1 | Env var parsing | CollectorConfig |
| `APScheduler` | ^3.8.1 | Job scheduling | Collector daemon mode |
| `aiohttp` | ^3.8.1 | Async HTTP | PlatformApi |
| `odd-models` | ^2.0.47 | ODD data models | All adapters (output) |
| `oddrn-generator` | ^0.1.103 | ODDRN creation | All adapters |
| `funcy` | ^2.0 | Functional utils | Job chunking |
| `loguru` | ^0.7.2 | Logging | Everywhere |
| `pyaml-env` | ^1.1.5 | YAML + env vars | Config loading |

### Adapter-Specific Dependencies

**Databases:**
| Adapter | Package | Notes |
|---------|---------|-------|
| PostgreSQL | `psycopg2-binary` | C extension |
| MySQL | `mysql-connector-python` | Pure Python |
| MSSQL | `pymssql` | Requires FreeTDS |
| Oracle | `oracledb` | Requires Oracle client |
| MongoDB | `pymongo` | |
| Cassandra | `cassandra-driver` | |
| Snowflake | `snowflake-connector-python` | Large dependency |

**Cloud:**
| Adapter | Package | Notes |
|---------|---------|-------|
| AWS (all) | `boto3`, `botocore` | |
| Azure (all) | `azure-*` packages | Multiple packages |
| GCP (all) | `google-cloud-*` | Multiple packages |

### Dependency Graph (Simplified)

```
odd-collector
├── odd-collector-sdk (local)
│   ├── pydantic
│   ├── APScheduler
│   ├── aiohttp
│   ├── odd-models
│   │   └── pydantic
│   ├── oddrn-generator
│   └── funcy
├── psycopg2-binary (postgresql)
├── mysql-connector-python (mysql)
├── snowflake-connector-python (snowflake)
└── ... (adapter-specific)
```

### Updating Dependencies

```bash
# Update all
cd odd-collector
poetry update

# Update specific
poetry update pydantic

# Check outdated
poetry show --outdated

# Lock without updating
poetry lock --no-update
```

---

## Quick Reference

### Entry Points

| Collector | Entry Point | Package |
|-----------|-------------|---------|
| Generic | `python -m odd_collector` | `odd_collector` |
| AWS | `python -m odd_collector_aws` | `odd_collector_aws` |
| Azure | `python -m odd_collector_azure` | `odd_collector_azure` |
| GCP | `python -m odd_collector_gcp` | `odd_collector_gcp` |

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `LOGLEVEL` | Log verbosity | `INFO` |
| `CONFIG_PATH` | Config file path | `./collector_config.yaml` |

### File Naming Conventions

| Type | Pattern | Example |
|------|---------|---------|
| Adapter | `adapters/{type}/adapter.py` | `adapters/postgresql/adapter.py` |
| Plugin | `domain/plugin.py` | Class: `PostgreSQLPlugin` |
| Config example | `config_examples/{type}.yaml` | `config_examples/postgresql.yaml` |
| Tests | `tests/integration/test_{type}.py` | `tests/integration/test_postgresql.py` |

### Class Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Adapter | `Adapter` (always) | `class Adapter(BaseAdapter)` |
| Plugin | `{Type}Plugin` | `class PostgreSQLPlugin(Plugin)` |
| Generator | `{Type}Generator` | `PostgresqlGenerator` |
| Repository | `{Type}Repository` | `PostgreSQLRepository` |

---

*Document generated: 2026-01-25*
*For developers performing maintenance, feature additions, or refactoring*
