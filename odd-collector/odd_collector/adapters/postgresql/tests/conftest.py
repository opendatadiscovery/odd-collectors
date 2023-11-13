import pytest
from oddrn_generator import PostgresqlGenerator

from odd_collector.adapters.postgresql.mappers.tables import Table
from odd_collector.adapters.postgresql.mappers.columns import Column


@pytest.fixture(scope="module")
def postgresql_generator():
    config = {
        "host_settings": "test",
        "databases": "test"
    }
    generator = PostgresqlGenerator(**config)
    generator.set_oddrn_paths(**{"databases": config["databases"]})
    return generator


@pytest.fixture(scope="module")
def int4_id_column():
    return Column(
        attrelid=16520, table_catalog='test', table_schema='test',
        table_name='test', column_name='id', ordinal_position=1,
        column_default="nextval('test.test_id_seq'::regclass)", is_nullable='NO',
        data_type='int4', character_maximum_length=None, character_octet_length=None, numeric_precision=32,
        numeric_precision_radix=2, numeric_scale=0, datetime_precision=None, interval_type=None,
        interval_precision=None, character_set_catalog=None, character_set_schema=None,
        character_set_name=None,
        collation_catalog=None, collation_schema=None, collation_name=None, domain_catalog=None,
        domain_schema=None, domain_name=None, udt_catalog='test', udt_schema='pg_catalog', udt_name='int4',
        scope_catalog=None, scope_schema=None, scope_name=None, maximum_cardinality=None, dtd_identifier='1',
        is_self_referencing='NO', is_identity='NO', identity_generation=None, identity_start=None,
        identity_increment=None, identity_maximum=None, identity_minimum=None, identity_cycle='NO',
        is_generated='NEVER', generation_expression=None, is_updatable='YES', description=None, type_oid=23,
        enums=[], is_primary=True
    )


@pytest.fixture(scope="module")
def varchar_column():
    return Column(
        attrelid=16520, table_catalog='test', table_schema='test',
        table_name='test', column_name='test', ordinal_position=2, column_default=None,
        is_nullable='YES', data_type='varchar', character_maximum_length=255, character_octet_length=1020,
        numeric_precision=None, numeric_precision_radix=None, numeric_scale=None, datetime_precision=None,
        interval_type=None, interval_precision=None, character_set_catalog=None, character_set_schema=None,
        character_set_name=None, collation_catalog=None, collation_schema=None, collation_name=None,
        domain_catalog=None, domain_schema=None, domain_name=None, udt_catalog='test', udt_schema='pg_catalog',
        udt_name='varchar', scope_catalog=None, scope_schema=None, scope_name=None, maximum_cardinality=None,
        dtd_identifier='2', is_self_referencing='NO', is_identity='NO', identity_generation=None,
        identity_start=None, identity_increment=None, identity_maximum=None, identity_minimum=None,
        identity_cycle='NO', is_generated='NEVER', generation_expression=None, is_updatable='YES',
        description=None, type_oid=1043, enums=[], is_primary=False
    )


@pytest.fixture(scope="module")
def vector_column():
    return Column(
        attrelid=16520, table_catalog='test', table_schema='test',
        table_name='test', column_name='test', ordinal_position=3, column_default=None,
        is_nullable='YES', data_type='vector', character_maximum_length=None, character_octet_length=None,
        numeric_precision=None, numeric_precision_radix=None, numeric_scale=None, datetime_precision=None,
        interval_type=None, interval_precision=None, character_set_catalog=None, character_set_schema=None,
        character_set_name=None, collation_catalog=None, collation_schema=None, collation_name=None,
        domain_catalog=None, domain_schema=None, domain_name=None, udt_catalog='test', udt_schema='public',
        udt_name='vector', scope_catalog=None, scope_schema=None, scope_name=None, maximum_cardinality=None,
        dtd_identifier='4', is_self_referencing='NO', is_identity='NO', identity_generation=None,
        identity_start=None, identity_increment=None, identity_maximum=None, identity_minimum=None,
        identity_cycle='NO', is_generated='NEVER', generation_expression=None, is_updatable='YES',
        description=None, type_oid=16420, enums=[], is_primary=False
    )


@pytest.fixture(scope="module")
def table_without_vector_column(int4_id_column, varchar_column):
    return Table(
        oid=16520,
        table_catalog='test',
        table_schema='test',
        table_name='test',
        table_type='r',
        self_referencing_column_name=None,
        reference_generation=None,
        user_defined_type_catalog=None,
        user_defined_type_schema=None,
        user_defined_type_name=None,
        is_insertable_into='YES',
        is_typed='NO',
        commit_action=None,
        view_definition=None,
        view_check_option=None,
        view_is_updatable=None,
        view_is_insertable_into=None,
        view_is_trigger_updatable=None,
        view_is_trigger_deletable=None,
        view_is_trigger_insertable_into=None,
        table_owner='postgres',
        table_rows=100,
        description=None,
        columns=[int4_id_column, varchar_column, ],
        primary_keys=[]
    )


@pytest.fixture(scope="module")
def table_with_vector_column(int4_id_column, varchar_column, vector_column):
    return Table(
        oid=16520,
        table_catalog='test',
        table_schema='test',
        table_name='test',
        table_type='r',
        self_referencing_column_name=None,
        reference_generation=None,
        user_defined_type_catalog=None,
        user_defined_type_schema=None,
        user_defined_type_name=None,
        is_insertable_into='YES',
        is_typed='NO',
        commit_action=None,
        view_definition=None,
        view_check_option=None,
        view_is_updatable=None,
        view_is_insertable_into=None,
        view_is_trigger_updatable=None,
        view_is_trigger_deletable=None,
        view_is_trigger_insertable_into=None,
        table_owner='postgres',
        table_rows=100,
        description=None,
        columns=[int4_id_column, varchar_column, vector_column, ],
        primary_keys=[]
    )


@pytest.fixture(scope="module")
def view_without_vector_column(int4_id_column, varchar_column):
    return Table(
        oid=16528,
        table_catalog='test',
        table_schema='test',
        table_name='test',
        table_type='v',
        self_referencing_column_name=None,
        reference_generation=None,
        user_defined_type_catalog=None,
        user_defined_type_schema=None,
        user_defined_type_name=None,
        is_insertable_into='YES',
        is_typed='NO',
        commit_action=None,
        view_definition='SELECT * FROM test.test;',
        view_check_option='NONE',
        view_is_updatable='YES',
        view_is_insertable_into='YES',
        view_is_trigger_updatable='NO',
        view_is_trigger_deletable='NO',
        view_is_trigger_insertable_into='NO',
        table_owner='postgres',
        table_rows=100,
        description=None,
        columns=[int4_id_column, varchar_column, ],
        primary_keys=[]
    )


@pytest.fixture(scope="module")
def view_with_vector_column(int4_id_column, varchar_column, vector_column):
    return Table(
        oid=16528,
        table_catalog='test',
        table_schema='test',
        table_name='test',
        table_type='v',
        self_referencing_column_name=None,
        reference_generation=None,
        user_defined_type_catalog=None,
        user_defined_type_schema=None,
        user_defined_type_name=None,
        is_insertable_into='YES',
        is_typed='NO',
        commit_action=None,
        view_definition='SELECT * FROM test.test;',
        view_check_option='NONE',
        view_is_updatable='YES',
        view_is_insertable_into='YES',
        view_is_trigger_updatable='NO',
        view_is_trigger_deletable='NO',
        view_is_trigger_insertable_into='NO',
        table_owner='postgres',
        table_rows=100,
        description=None,
        columns=[int4_id_column, varchar_column, vector_column, ],
        primary_keys=[]
    )


@pytest.fixture(scope="module")
def materialized_view_without_vector_column(int4_id_column, varchar_column):
    return Table(
        oid=16532,
        table_catalog=None,
        table_schema='test',
        table_name='test',
        table_type='m',
        self_referencing_column_name=None,
        reference_generation=None,
        user_defined_type_catalog=None,
        user_defined_type_schema=None,
        user_defined_type_name=None,
        is_insertable_into=None,
        is_typed=None,
        commit_action=None,
        view_definition='SELECT * FROM test.test;',
        view_check_option=None,
        view_is_updatable=None,
        view_is_insertable_into=None,
        view_is_trigger_updatable=None,
        view_is_trigger_deletable=None,
        view_is_trigger_insertable_into=None,
        table_owner='postgres',
        table_rows=100,
        description=None,
        columns=[int4_id_column, varchar_column, ],
        primary_keys=[]
    )


@pytest.fixture(scope="module")
def materialized_view_with_vector_column(int4_id_column, varchar_column, vector_column):
    return Table(
        oid=16532,
        table_catalog=None,
        table_schema='test',
        table_name='test',
        table_type='m',
        self_referencing_column_name=None,
        reference_generation=None,
        user_defined_type_catalog=None,
        user_defined_type_schema=None,
        user_defined_type_name=None,
        is_insertable_into=None,
        is_typed=None,
        commit_action=None,
        view_definition='SELECT * FROM test.test;',
        view_check_option=None,
        view_is_updatable=None,
        view_is_insertable_into=None,
        view_is_trigger_updatable=None,
        view_is_trigger_deletable=None,
        view_is_trigger_insertable_into=None,
        table_owner='postgres',
        table_rows=100,
        description=None,
        columns=[int4_id_column, varchar_column, vector_column, ],
        primary_keys=[]
    )
