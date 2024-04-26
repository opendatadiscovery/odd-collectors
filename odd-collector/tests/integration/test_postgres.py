import odd_models
import pytest
import sqlalchemy
from odd_collector.adapters.postgresql.adapter import Adapter
from odd_collector.domain.plugin import PostgreSQLPlugin
from pydantic import SecretStr
from testcontainers.postgres import PostgresContainer
from tests.integration.helpers import (
    find_by_name,
    find_by_type,
    find_dataset_field_by_name,
)


def create_primary_schema(connection: sqlalchemy.engine.Connection):
    create_enum = """CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');"""
    create_tables = """CREATE TABLE IF NOT EXISTS TABLE_ONE (
        code        char(5) CONSTRAINT firstkey PRIMARY KEY,
        title       varchar(40) NOT NULL,
        did         integer NOT NULL,
        date_prod   date,
        kind        varchar(10),
        len         interval hour to minute,
        status      mood
    )"""

    create_view = """
    CREATE VIEW VIEW_ONE AS
    SELECT *
    FROM TABLE_ONE
    """

    create_second_view = """
    CREATE VIEW VIEW_TWO AS
    SELECT t.code, v.title
    FROM TABLE_ONE t, VIEW_ONE v
    """

    with connection.begin():
        connection.exec_driver_sql(create_enum)
        connection.exec_driver_sql(create_tables)
        connection.exec_driver_sql(create_view)
        connection.exec_driver_sql(create_second_view)


def create_other_schema(connection: sqlalchemy.engine.Connection):
    create_schema = """
    CREATE SCHEMA IF NOT EXISTS other_schema;
    """

    create_view_three = """
    CREATE VIEW other_schema.VIEW_THREE AS
    SELECT *
    FROM TABLE_ONE
    """

    create_view_four = """
    CREATE VIEW other_schema.VIEW_FOUR AS
    SELECT v1.code, v3.title
    FROM VIEW_ONE v1, other_schema.VIEW_THREE v3
    """

    create_materialized_view = """
    CREATE MATERIALIZED VIEW other_schema.materialized_view AS
    SELECT *
    FROM TABLE_ONE
    """

    with connection.begin():
        connection.exec_driver_sql(create_schema)
        connection.exec_driver_sql(create_view_three)
        connection.exec_driver_sql(create_view_four)
        connection.exec_driver_sql(create_materialized_view)


def create_vector_store_schema(connection: sqlalchemy.engine.Connection):
    init_pgvector_extension = """
        CREATE EXTENSION vector;
    """

    create_schema = """
        CREATE SCHEMA IF NOT EXISTS vector_store_schema;
    """

    create_vector_store_table = """
        CREATE TABLE vector_store_schema.vector_store_table (
            id serial PRIMARY KEY,
            name varchar(255),
            age integer,
            embedding vector(3)
        );
    """

    create_vector_store_view = """
        CREATE VIEW vector_store_schema.vector_store_view
        AS
            SELECT vst.name, vst.embedding
            FROM vector_store_schema.vector_store_table vst;
    """

    create_vector_store_materialized_view = """
        CREATE MATERIALIZED VIEW vector_store_schema.vector_store_materialized_view
        AS
            SELECT vst.name, vst.embedding
            FROM vector_store_schema.vector_store_table vst;
    """

    with connection.begin():
        for q in (
            init_pgvector_extension,
            create_schema,
            create_vector_store_table,
            create_vector_store_view,
            create_vector_store_materialized_view,
        ):
            connection.exec_driver_sql(q)


@pytest.fixture(scope="module")
def data_entity_list() -> odd_models.DataEntityList:
    with PostgresContainer(
        image="ankane/pgvector", password="postgres", username="postgres"
    ) as container:
        engine = sqlalchemy.create_engine(container.get_connection_url())

        with engine.connect() as connection:
            create_primary_schema(connection)
            create_other_schema(connection)
            create_vector_store_schema(connection)

        config = PostgreSQLPlugin(
            type="postgresql",
            name="test",
            database="test",
            password=SecretStr("postgres"),
            user="postgres",
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(5432)),
        )

        return Adapter(config).get_data_entity_list()


def test_decoding_to_json(data_entity_list: odd_models.DataEntityList):
    assert data_entity_list.model_dump_json()


def test_data_base_service(data_entity_list: odd_models.DataEntityList):
    database_services: list[odd_models.DataEntity] = find_by_type(
        data_entity_list, odd_models.DataEntityType.DATABASE_SERVICE
    )
    assert len(database_services) == 4

    database = find_by_name(data_entity_list, "test")
    public_schema = find_by_name(data_entity_list, "public")
    other_schema = find_by_name(data_entity_list, "other_schema")
    vector_store_schema = find_by_name(data_entity_list, "vector_store_schema")

    all_db_services = [
        database,
        public_schema,
        other_schema,
        vector_store_schema,
    ]
    only_schemas = [
        public_schema,
        other_schema,
        vector_store_schema,
    ]

    for db_service in all_db_services:
        assert db_service is not None

    assert isinstance(database, odd_models.DataEntity)
    assert database.type == odd_models.DataEntityType.DATABASE_SERVICE

    assert database.data_entity_group is not None
    assert len(database.data_entity_group.entities_list) == 3

    for schema in only_schemas:
        assert schema.oddrn in database.data_entity_group.entities_list


def test_public_schema(data_entity_list: odd_models.DataEntityList):
    public_schema = find_by_name(data_entity_list, "public")
    assert public_schema is not None
    assert isinstance(public_schema, odd_models.DataEntity)
    assert public_schema.type == odd_models.DataEntityType.DATABASE_SERVICE
    assert public_schema.data_entity_group is not None

    table_one = find_by_name(data_entity_list, "table_one")
    view_one = find_by_name(data_entity_list, "view_one")
    view_two = find_by_name(data_entity_list, "view_two")

    assert len(public_schema.data_entity_group.entities_list) == 3

    for data_entity in [table_one, view_one, view_two]:
        assert data_entity is not None
        assert data_entity.oddrn in public_schema.data_entity_group.entities_list


def test_other_schema(data_entity_list: odd_models.DataEntityList):
    other_schema = find_by_name(data_entity_list, "other_schema")
    assert other_schema is not None
    assert isinstance(other_schema, odd_models.DataEntity)
    assert other_schema.type == odd_models.DataEntityType.DATABASE_SERVICE
    assert other_schema.data_entity_group is not None

    view_three = find_by_name(data_entity_list, "view_three")
    view_four = find_by_name(data_entity_list, "view_four")
    materialized_view = find_by_name(data_entity_list, "materialized_view")

    assert len(other_schema.data_entity_group.entities_list) == 3

    for data_entity in [view_three, view_four, materialized_view]:
        assert data_entity is not None
        assert data_entity.oddrn in other_schema.data_entity_group.entities_list


def test_table_one(data_entity_list: odd_models.DataEntityList):
    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one is not None
    assert isinstance(table_one, odd_models.DataEntity)
    assert table_one.type == odd_models.DataEntityType.TABLE
    assert table_one.dataset is not None
    assert table_one.dataset.field_list is not None
    assert len(table_one.dataset.field_list) == 7


def test_view_one(data_entity_list: odd_models.DataEntityList):
    view_one = find_by_name(data_entity_list, "view_one")
    assert view_one is not None
    assert isinstance(view_one, odd_models.DataEntity)
    assert view_one.type == odd_models.DataEntityType.VIEW
    assert view_one.dataset is not None
    assert view_one.dataset.field_list is not None
    assert len(view_one.dataset.field_list) == 7

    assert view_one.data_transformer is not None
    assert view_one.data_transformer.inputs is not None

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in view_one.data_transformer.inputs


def test_view_two(data_entity_list: odd_models.DataEntityList):
    view_two = find_by_name(data_entity_list, "view_two")
    assert view_two is not None
    assert isinstance(view_two, odd_models.DataEntity)
    assert view_two.type == odd_models.DataEntityType.VIEW
    assert view_two.dataset is not None
    assert view_two.dataset.field_list is not None
    assert len(view_two.dataset.field_list) == 2

    assert view_two.data_transformer is not None
    assert view_two.data_transformer.inputs is not None
    assert len(view_two.data_transformer.inputs) == 2

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in view_two.data_transformer.inputs
    view_one = find_by_name(data_entity_list, "view_one")
    assert view_one.oddrn in view_two.data_transformer.inputs


def test_view_three(data_entity_list: odd_models.DataEntityList):
    view_three = find_by_name(data_entity_list, "view_three")
    assert view_three is not None
    assert isinstance(view_three, odd_models.DataEntity)
    assert view_three.type == odd_models.DataEntityType.VIEW
    assert view_three.dataset is not None
    assert view_three.dataset.field_list is not None
    assert len(view_three.dataset.field_list) == 7

    assert view_three.data_transformer is not None
    assert view_three.data_transformer.inputs is not None
    assert len(view_three.data_transformer.inputs) == 1

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in view_three.data_transformer.inputs


def test_view_four(data_entity_list: odd_models.DataEntityList):
    view_four = find_by_name(data_entity_list, "view_four")
    assert view_four is not None
    assert isinstance(view_four, odd_models.DataEntity)
    assert view_four.type == odd_models.DataEntityType.VIEW
    assert view_four.dataset is not None
    assert view_four.dataset.field_list is not None
    assert len(view_four.dataset.field_list) == 2

    assert view_four.data_transformer is not None
    assert view_four.data_transformer.inputs is not None
    assert len(view_four.data_transformer.inputs) == 2

    view_one = find_by_name(data_entity_list, "view_one")
    assert view_one.oddrn in view_four.data_transformer.inputs

    view_three = find_by_name(data_entity_list, "view_three")
    assert view_three.oddrn in view_four.data_transformer.inputs


def test_materialized_view(data_entity_list: odd_models.DataEntityList):
    materialized_view = find_by_name(data_entity_list, "materialized_view")
    assert materialized_view is not None
    assert isinstance(materialized_view, odd_models.DataEntity)
    assert materialized_view.type == odd_models.DataEntityType.VIEW
    assert materialized_view.dataset is not None
    assert materialized_view.dataset.field_list is not None
    assert len(materialized_view.dataset.field_list) == 7

    assert materialized_view.data_transformer is not None
    assert materialized_view.data_transformer.inputs is not None
    assert len(materialized_view.data_transformer.inputs) == 1

    table_one = find_by_name(data_entity_list, "table_one")
    assert table_one.oddrn in materialized_view.data_transformer.inputs


#   Block for testing new logic attached to pgvector extension
def test_vector_store_schema(data_entity_list: odd_models.DataEntityList):
    vector_store_schema = find_by_name(data_entity_list, "vector_store_schema")
    assert vector_store_schema is not None
    assert isinstance(vector_store_schema, odd_models.DataEntity)
    assert vector_store_schema.type == odd_models.DataEntityType.DATABASE_SERVICE
    assert vector_store_schema.data_entity_group is not None

    vector_store_table = find_by_name(data_entity_list, "vector_store_table")
    vector_store_view = find_by_name(data_entity_list, "vector_store_view")
    vector_store_materialized_view = find_by_name(
        data_entity_list, "vector_store_materialized_view"
    )

    assert len(vector_store_schema.data_entity_group.entities_list) == 3

    for data_entity in [
        vector_store_table,
        vector_store_view,
        vector_store_materialized_view,
    ]:
        assert data_entity is not None
        assert data_entity.oddrn in vector_store_schema.data_entity_group.entities_list


def test_vector_store_table(data_entity_list: odd_models.DataEntityList):
    vector_store_table = find_by_name(data_entity_list, "vector_store_table")
    assert vector_store_table is not None
    assert isinstance(vector_store_table, odd_models.DataEntity)
    assert vector_store_table.type == odd_models.DataEntityType.VECTOR_STORE
    assert vector_store_table.dataset is not None
    assert vector_store_table.dataset.field_list is not None
    assert len(vector_store_table.dataset.field_list) == 4

    # TODO: checked only column with new data type, maybe worth to be populated on others
    vector_column = find_dataset_field_by_name(
        vector_store_table.dataset.field_list, "embedding"
    )
    assert vector_column is not None
    assert vector_column.type.type == odd_models.Type.TYPE_VECTOR


def test_vector_store_view(data_entity_list: odd_models.DataEntityList):
    vector_store_view = find_by_name(data_entity_list, "vector_store_view")
    assert vector_store_view is not None
    assert isinstance(vector_store_view, odd_models.DataEntity)
    assert vector_store_view.type == odd_models.DataEntityType.VECTOR_STORE
    assert vector_store_view.dataset is not None
    assert vector_store_view.dataset.field_list is not None
    assert len(vector_store_view.dataset.field_list) == 2

    assert vector_store_view.data_transformer is not None
    assert vector_store_view.data_transformer.inputs is not None
    assert len(vector_store_view.data_transformer.inputs) == 1

    vector_store_table = find_by_name(data_entity_list, "vector_store_table")
    assert vector_store_table.oddrn in vector_store_view.data_transformer.inputs

    # TODO: checked only column with new data type, maybe worth to be populated on others
    vector_column = find_dataset_field_by_name(
        vector_store_view.dataset.field_list, "embedding"
    )
    assert vector_column is not None
    assert vector_column.type.type == odd_models.Type.TYPE_VECTOR


def test_vector_store_materialized_view(data_entity_list: odd_models.DataEntityList):
    vector_store_materialized_view = find_by_name(
        data_entity_list, "vector_store_materialized_view"
    )
    assert vector_store_materialized_view is not None
    assert isinstance(vector_store_materialized_view, odd_models.DataEntity)
    assert vector_store_materialized_view.type == odd_models.DataEntityType.VECTOR_STORE
    assert vector_store_materialized_view.dataset is not None
    assert vector_store_materialized_view.dataset.field_list is not None
    assert len(vector_store_materialized_view.dataset.field_list) == 2

    assert vector_store_materialized_view.data_transformer is not None
    assert vector_store_materialized_view.data_transformer.inputs is not None
    assert len(vector_store_materialized_view.data_transformer.inputs) == 1

    vector_store_table = find_by_name(data_entity_list, "vector_store_table")
    assert (
        vector_store_table.oddrn
        in vector_store_materialized_view.data_transformer.inputs
    )

    # TODO: checked only column with new data type, maybe worth to be populated on others
    vector_column = find_dataset_field_by_name(
        vector_store_materialized_view.dataset.field_list, "embedding"
    )
    assert vector_column is not None
    assert vector_column.type.type == odd_models.Type.TYPE_VECTOR
