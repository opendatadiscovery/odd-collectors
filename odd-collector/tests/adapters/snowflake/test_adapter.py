import datetime
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from funcy import filter, first, lfilter
from odd_collector.adapters.snowflake.adapter import Adapter
from odd_collector.adapters.snowflake.domain import Column, Connection, Table, View
from odd_collector.adapters.snowflake.domain.fk_constraint import ForeignKeyConstraint
from odd_collector.adapters.snowflake.domain.pipe import RawPipe, RawStage
from odd_collector.adapters.snowflake.domain.unique_constraint import UniqueConstraint
from odd_collector.domain.plugin import SnowflakePlugin
from odd_models.models import DataEntity, DataEntityType
from pydantic import SecretStr

DATABASE_NAME = "TEST_DB"
SCHEMA = "PUBLIC"
TABLE_NAME = "TEST_TABLE"
FIRST_VIEW = "FIRST_VIEW"
SECOND_VIEW = "SECOND_VIEW"
FIRST_PIPE = "MY_PIPE_1"
SECOND_PIPE = "MY_PIPE_2"
STAGE_NAME = "MY_INTERNAL_STAGE"


@pytest.fixture()
def config() -> SnowflakePlugin:
    return SnowflakePlugin(
        name="snowflake_adapter",
        description="snowflake_adapter",
        host="localhost",
        database=DATABASE_NAME,
        user="omit",
        type="snowflake",
        password=SecretStr("omit"),
        account="account_name",
        warehouse="warehouse",
    )


@pytest.fixture()
def raw_pipes() -> list[RawPipe]:
    return [
        RawPipe(
            pipe_catalog=DATABASE_NAME,
            pipe_schema=SCHEMA,
            pipe_name=FIRST_PIPE,
            definition=f"COPY INTO {TABLE_NAME}\nFROM @my_internal_stage\nFILE_FORMAT = (TYPE = 'CSV')",
        ),
        RawPipe(
            pipe_catalog=DATABASE_NAME,
            pipe_schema=SCHEMA,
            pipe_name=SECOND_PIPE,
            definition=f"COPY INTO {TABLE_NAME}\nFROM @my_internal_stage\nFILE_FORMAT = (TYPE = 'JSON')\nMATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        ),
    ]


@pytest.fixture()
def raw_stages() -> list[RawStage]:
    return [
        RawStage(
            stage_name=STAGE_NAME,
            stage_catalog=DATABASE_NAME,
            stage_schema=SCHEMA,
            stage_url=None,
            stage_type="Internal Named",
        )
    ]


@pytest.fixture()
def fk_constraints() -> list[ForeignKeyConstraint]:
    return []


@pytest.fixture()
def unique_constraints() -> list[UniqueConstraint]:
    return []


@pytest.fixture()
def tables() -> list[Table]:
    return [
        Table(
            upstream=[],
            downstream=[
                Connection(
                    table_catalog=DATABASE_NAME,
                    table_name=FIRST_VIEW,
                    table_schema=SCHEMA,
                    domain="VIEW",
                )
            ],
            table_catalog=DATABASE_NAME,
            table_schema=SCHEMA,
            table_name=TABLE_NAME,
            table_owner="ACCOUNTADMIN",
            table_type="BASE TABLE",
            is_transient="NO",
            clustering_key=None,
            row_count=0,
            retention_time=1,
            created=datetime.datetime.now(),
            last_altered=datetime.datetime.now(),
            table_comment=None,
            self_referencing_column_name=None,
            reference_generation=None,
            user_defined_type_catalog=None,
            user_defined_type_schema=None,
            user_defined_type_name=None,
            is_insertable_into="YES",
            is_typed="YES",
            columns=[
                Column(
                    table_catalog=DATABASE_NAME,
                    table_schema=SCHEMA,
                    table_name=TABLE_NAME,
                    column_name="NAME",
                    ordinal_position=1,
                    column_default=None,
                    is_nullable="YES",
                    data_type="TEXT",
                    character_maximum_length=16777216,
                    character_octet_length=16777216,
                    numeric_precision=None,
                    numeric_precision_radix=None,
                    numeric_scale=None,
                    collation_name=None,
                    is_identity="NO",
                    identity_generation=None,
                    identity_start=None,
                    identity_increment=None,
                    identity_cycle=None,
                    comment=None,
                )
            ],
        ),
        View(
            upstream=[
                Connection(
                    table_catalog=DATABASE_NAME,
                    table_name=TABLE_NAME,
                    table_schema=SCHEMA,
                    domain="TABLE",
                )
            ],
            downstream=[
                Connection(
                    table_catalog=DATABASE_NAME,
                    table_name=SECOND_VIEW,
                    table_schema=SCHEMA,
                    domain="VIEW",
                )
            ],
            table_catalog=DATABASE_NAME,
            table_schema=SCHEMA,
            table_name=FIRST_VIEW,
            table_owner="ACCOUNTADMIN",
            table_type="VIEW",
            is_transient=None,
            clustering_key=None,
            row_count=None,
            retention_time=None,
            created=datetime.datetime.now(),
            last_altered=datetime.datetime.now(),
            table_comment=None,
            self_referencing_column_name=None,
            reference_generation=None,
            user_defined_type_catalog=None,
            user_defined_type_schema=None,
            user_defined_type_name=None,
            is_insertable_into="YES",
            is_typed="YES",
            columns=[
                Column(
                    table_catalog=DATABASE_NAME,
                    table_schema=SCHEMA,
                    table_name=FIRST_VIEW,
                    column_name="NAME",
                    ordinal_position=1,
                    column_default=None,
                    is_nullable="YES",
                    data_type="TEXT",
                    character_maximum_length=16777216,
                    character_octet_length=16777216,
                    numeric_precision=None,
                    numeric_precision_radix=None,
                    numeric_scale=None,
                    collation_name=None,
                    is_identity="NO",
                    identity_generation=None,
                    identity_start=None,
                    identity_increment=None,
                    identity_cycle=None,
                    comment=None,
                )
            ],
            view_definition="create view test_view as\n    -- comment = '<comment>'\n    select * from test;",
            is_updatable="NO",
            is_secure="NO",
            view_comment=None,
        ),
        View(
            upstream=[
                Connection(
                    table_catalog=DATABASE_NAME,
                    table_name=FIRST_VIEW,
                    table_schema=SCHEMA,
                    domain="VIEW",
                )
            ],
            downstream=[],
            table_catalog=DATABASE_NAME,
            table_schema=SCHEMA,
            table_name=SECOND_VIEW,
            table_owner="ACCOUNTADMIN",
            table_type="VIEW",
            is_transient=None,
            clustering_key=None,
            row_count=None,
            retention_time=None,
            created=datetime.datetime.now(),
            last_altered=datetime.datetime.now(),
            table_comment=None,
            self_referencing_column_name=None,
            reference_generation=None,
            user_defined_type_catalog=None,
            user_defined_type_schema=None,
            user_defined_type_name=None,
            is_insertable_into="YES",
            is_typed="YES",
            columns=[
                Column(
                    table_catalog=DATABASE_NAME,
                    table_schema=SCHEMA,
                    table_name=SECOND_VIEW,
                    column_name="NAME",
                    ordinal_position=1,
                    column_default=None,
                    is_nullable="YES",
                    data_type="TEXT",
                    character_maximum_length=16777216,
                    character_octet_length=16777216,
                    numeric_precision=None,
                    numeric_precision_radix=None,
                    numeric_scale=None,
                    collation_name=None,
                    is_identity="NO",
                    identity_generation=None,
                    identity_start=None,
                    identity_increment=None,
                    identity_cycle=None,
                    comment=None,
                )
            ],
            view_definition="create or replace view TEST_1.PUBLIC.TEST_VIEW_2(\n\tNAME\n) as\n    -- comment = '<comment>'\n    select * from TEST_VIEW;",
            is_updatable="NO",
            is_secure="NO",
            view_comment=None,
        ),
    ]


def _find_database(seq: list[DataEntity]) -> Optional[DataEntity]:
    return first(filter(lambda entity: entity.name == DATABASE_NAME, seq))


def _find_schema(seq: list[DataEntity]) -> Optional[DataEntity]:
    return first(filter(lambda entity: entity.name == SCHEMA, seq))


def _find_tables(seq: list[DataEntity]) -> list[DataEntity]:
    return lfilter(
        lambda entity: entity.type in {DataEntityType.TABLE, DataEntityType.VIEW}, seq
    )


def _find_pipes(seq: list[DataEntity]) -> list[DataEntity]:
    return lfilter(lambda entity: entity.name in (FIRST_PIPE, SECOND_PIPE), seq)


@patch("odd_collector.adapters.snowflake.adapter.SnowflakeClient")
def test_adapter(
    mock_snowflake_client,
    config: SnowflakePlugin,
    tables: list[Table],
    raw_pipes: list[RawPipe],
    raw_stages: list[RawStage],
    fk_constraints: list[ForeignKeyConstraint],
    unique_constraints: list[UniqueConstraint],
):
    # Create a mock instance of the SnowflakeClient
    mock_client_instance = MagicMock()

    # Mock the return values for the client methods
    mock_client_instance.get_tables.return_value = tables
    mock_client_instance.get_raw_pipes.return_value = raw_pipes
    mock_client_instance.get_raw_stages.return_value = raw_stages
    mock_client_instance.get_fk_constraints.return_value = fk_constraints
    mock_client_instance.get_unique_constraints.return_value = unique_constraints

    # Set the mock client instance as the return value of the context manager
    mock_snowflake_client.return_value.__enter__.return_value = mock_client_instance

    # Create the adapter with the mocked client
    adapter = Adapter(config)

    data_entity_list = adapter.get_data_entity_list()
    data_entities = data_entity_list.items

    assert len(data_entities) == 7  # 1 Database; 1 Schema; 3 Tables + Views; 2 Pipes

    database_entity: DataEntity = _find_database(data_entities)
    schema_entity: DataEntity = _find_schema(data_entities)
    table_entities: list[DataEntity] = _find_tables(data_entities)
    pipe_entities: list[DataEntity] = _find_pipes(data_entities)

    assert database_entity is not None
    assert schema_entity is not None
    assert table_entities is not None
    assert pipe_entities is not None

    assert len(table_entities) == 3
    assert len(pipe_entities) == 2

    assert schema_entity.oddrn in database_entity.data_entity_group.entities_list
    for table_entity in table_entities:
        assert table_entity.oddrn in schema_entity.data_entity_group.entities_list
