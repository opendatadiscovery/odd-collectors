import re
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable, Union

from funcy import lsplit
from odd_collector.domain.plugin import SnowflakePlugin
from odd_collector.helpers import LowerKeyDict
from odd_collector_sdk.errors import DataSourceError
from snowflake import connector
from snowflake.connector.cursor import DictCursor

from .domain import Column, ForeignKeyConstraint, RawPipe, RawStage, Table, View
from .logger import logger

TABLES_VIEWS_QUERY = """
    with recursive cte as (
        select
            referenced_database,
            referenced_schema,
            referenced_object_name,
            referenced_object_id,
            referenced_object_domain,
            referencing_database,
            referencing_schema,
            referencing_object_name,
            referencing_object_id,
            referencing_object_domain
        from snowflake.account_usage.object_dependencies
        union all
        select
            deps.referenced_database,
            deps.referenced_schema,
            deps.referenced_object_name,
            deps.referenced_object_id,
            deps.referenced_object_domain,
            deps.referencing_database,
            deps.referencing_schema,
            deps.referencing_object_name,
            deps.referencing_object_id,
            deps.referencing_object_domain
        from snowflake.account_usage.object_dependencies deps
        join cte
            on deps.referencing_object_id = cte.referenced_object_id
            and deps.referencing_object_domain = cte.referenced_object_domain
    ),
    upstream as (
        select
            referencing_database as node_database,
            referencing_schema as node_schema,
            referencing_object_name as node_name,
            referencing_object_domain as node_domain,
            array_agg(
                distinct concat_ws(
                    '.',
                    referenced_database,
                    referenced_schema,
                    referenced_object_name,
                    referenced_object_domain
                )
            ) as nodes
        from cte
        group by
            referencing_database,
            referencing_schema,
            referencing_object_name,
            referencing_object_id,
            referencing_object_domain
    ),
    downstream as (
        select
            referenced_database as node_database,
            referenced_schema as node_schema,
            referenced_object_name as node_name,
            referenced_object_domain as node_domain,
            array_agg(
                distinct concat_ws(
                    '.',
                    referencing_database,
                    referencing_schema,
                    referencing_object_name,
                    referencing_object_domain
                )
            ) as nodes
        from cte
        group by referenced_database,
            referenced_schema,
            referenced_object_name,
            referenced_object_id,
            referenced_object_domain
    )
    select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.table_owner,
        t.table_type,
        t.is_transient,
        t.clustering_key,
        t.row_count,
        t.bytes,
        t.retention_time,
        t.self_referencing_column_name,
        t.reference_generation,
        t.user_defined_type_catalog,
        t.user_defined_type_schema,
        t.user_defined_type_name,
        t.is_insertable_into,
        t.is_typed,
        t.created,
        t.last_altered,
        t.auto_clustering_on,
        t.comment as table_comment,
        v.comment as view_comment,
        v.view_definition,
        v.is_secure,
        v.is_updatable,
        array_to_string(u.nodes, ',') as upstream,
        array_to_string(d.nodes, ',') as downstream
    from information_schema.tables t
    left join information_schema.views as v
        on v.table_catalog = t.table_catalog
        and v.table_schema = t.table_schema
        and v.table_name = t.table_name
    left join upstream u
        on u.node_database = t.table_catalog
        and u.node_schema = t.table_schema
        and u.node_name = t.table_name
    left join downstream d
        on d.node_database = t.table_catalog
        and d.node_schema = t.table_schema
        and d.node_name = t.table_name
    where t.table_schema != 'INFORMATION_SCHEMA'
    order by table_catalog, table_schema, table_name;
"""

COLUMNS_QUERY = """
    select
       c.table_catalog,
       c.table_schema,
       c.table_name,
       c.column_name,
       c.ordinal_position,
       c.column_default,
       c.is_nullable,
       c.data_type,
       c.character_maximum_length,
       c.character_octet_length,
       c.numeric_precision,
       c.numeric_precision_radix,
       c.numeric_scale,
       c.collation_name,
       c.is_identity,
       c.identity_generation,
       c.identity_start,
       c.identity_increment,
       c.identity_cycle,
       c.comment
    from information_schema.columns as c
    join information_schema.tables as t
        on c.table_catalog = t.table_catalog
        and c.table_schema = t.table_schema
        and c.table_name = t.table_name
    where c.table_schema != 'INFORMATION_SCHEMA'
    order by
        c.table_catalog,
        c.table_schema,
        c.table_name,
        c.ordinal_position;
"""

RAW_PIPES_QUERY = """
    SELECT PIPE_CATALOG, PIPE_SCHEMA, PIPE_NAME, DEFINITION
    FROM INFORMATION_SCHEMA.PIPES;
"""

RAW_STAGES_QUERY = """
    SELECT STAGE_NAME, STAGE_CATALOG, STAGE_SCHEMA, STAGE_URL, STAGE_TYPE
    FROM INFORMATION_SCHEMA.STAGES;
"""

PRIMARY_KEYS_QUERY = """
    SHOW PRIMARY KEYS IN DATABASE;
"""

FOREIGN_KEY_CONSTRAINTS_QUERIES = (
    """SHOW IMPORTED KEYS IN DATABASE;""",
    """
        SELECT
            "created_on",
            "fk_name" as constraint_name,
            "fk_database_name" as database_name,
            "fk_schema_name" as schema_name,
            "fk_table_name" as table_name,
            ARRAY_AGG("fk_column_name") WITHIN GROUP (ORDER BY "key_sequence") AS "foreign_key",
            "pk_name" as referenced_constraint_name,
            "pk_database_name" as referenced_database_name,
            "pk_schema_name" as referenced_schema_name,
            "pk_table_name" as referenced_table_name,
            ARRAY_AGG("pk_column_name") WITHIN GROUP (ORDER BY "key_sequence") AS "referenced_foreign_key"
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        GROUP BY
            "created_on",
            "pk_database_name", "pk_schema_name", "pk_table_name", "pk_name",
            "fk_database_name", "fk_schema_name", "fk_table_name", "fk_name";
    """,
)


class SnowflakeClientBase(ABC):
    def __init__(self, config: SnowflakePlugin):
        self._config = config

    @abstractmethod
    def get_tables(self) -> list[Table]:
        raise NotImplementedError

    @abstractmethod
    def get_raw_pipes(self) -> list[RawPipe]:
        raise NotImplementedError

    @abstractmethod
    def get_raw_stages(self) -> list[RawStage]:
        raise NotImplementedError

    @abstractmethod
    def get_fk_constraints(self) -> list[ForeignKeyConstraint]:
        raise NotImplementedError


class SnowflakeClient(SnowflakeClientBase):
    def __init__(self, config):
        super().__init__(config)
        self._conn = None

    def __enter__(self):
        try:
            logger.info("Setting up Snowflake connection...")
            self._conn = connector.connect(
                user=self._config.user,
                password=self._config.password.get_secret_value(),
                account=self._config.account,
                database=self._config.database,
                warehouse=self._config.warehouse,
            )
        except Exception as e:
            raise DataSourceError(
                f"Error during getting information from Snowflake. {e}"
            ) from e
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            logger.info("Snowflake connection has been closed.")
            self._conn.close()

    def get_tables(self) -> list[Union[Table, View]]:
        logger.info("Getting tables and views from Snowflake")

        def is_belongs(table: Table) -> Callable[[Column], bool]:
            def _(column: Column) -> bool:
                return (
                    table.table_catalog == column.table_catalog
                    and table.table_schema == column.table_schema
                    and table.table_name == column.table_name
                )

            return _

        with DictCursor(self._conn) as cursor:
            tables: list[Table] = self._fetch_tables(cursor)
            columns: list[Column] = self._fetch_columns(cursor)
            primary_keys: dict[str, list] = self._fetch_primary_keys(cursor)
            clustering_keys: dict[str, list] = self._get_clustering_keys(tables)

            for column in columns:
                if column.column_name in primary_keys.get(column.table_name, []):
                    column.is_primary_key = True
                if column.column_name.lower() in clustering_keys.get(
                    column.table_name, []
                ):
                    column.is_clustering_key = True

            for table in tables:
                belongs, not_belongs = lsplit(is_belongs(table), columns)
                table.columns.extend(belongs)
                columns = not_belongs
            return tables

    def get_raw_pipes(self) -> list[RawPipe]:
        logger.info("Getting pipes from Snowflake")
        with DictCursor(self._conn) as cursor:
            return self._base_fetch_entity_list(RAW_PIPES_QUERY, cursor, RawPipe)

    def get_raw_stages(self) -> list[RawStage]:
        logger.info("Getting stages from Snowflake")
        with DictCursor(self._conn) as cursor:
            return self._base_fetch_entity_list(RAW_STAGES_QUERY, cursor, RawStage)

    def get_fk_constraints(self) -> list[ForeignKeyConstraint]:
        logger.info("Getting foreign key constraints from Snowflake")
        with DictCursor(self._conn) as cursor:
            return self._fetch_fk_constraints(cursor)

    @staticmethod
    def _get_clustering_keys(tables: list[Table]) -> dict[str, list]:
        res: dict[str, list] = {}

        # Snowflake clustering keys could look like: "LINEAR(to_date(post_timestamp))", "LINEAR(column2, column3)"
        # cl_keys matches any parentheses and everything inside them that do not contain any parentheses.
        regex = r"\((?P<cl_keys>[^()]+)\)"

        for table in tables:
            if table.clustering_key:
                matches = re.search(regex, table.clustering_key)
                if matches:
                    res[table.table_name] = matches.group("cl_keys").split(", ")
        return res

    @staticmethod
    def _fetch_tables(cursor: DictCursor) -> list[Table]:
        result: list[Table] = []

        cursor.execute(TABLES_VIEWS_QUERY)
        for raw_object in cursor.fetchall():
            if raw_object.get("TABLE_TYPE") == "BASE TABLE":
                result.append(Table.parse_obj(LowerKeyDict(raw_object)))
            elif raw_object.get("TABLE_TYPE") == "VIEW":
                result.append(View.parse_obj(LowerKeyDict(raw_object)))
        return result

    def _fetch_columns(self, cursor: DictCursor) -> list[Column]:
        return self._base_fetch_entity_list(COLUMNS_QUERY, cursor, Column)

    @staticmethod
    def _fetch_primary_keys(cursor: DictCursor) -> dict[str, list]:
        res = defaultdict(list)

        cursor.execute(PRIMARY_KEYS_QUERY)
        for pk in cursor.fetchall():
            res[pk["table_name"]].append(pk["column_name"])
        return res

    @staticmethod
    def _base_fetch_entity_list(
        query: str,
        cursor: DictCursor,
        entity_type: Any,
    ) -> list[Any]:
        cursor.execute(query)
        return [
            entity_type.parse_obj(LowerKeyDict(raw_object))
            for raw_object in cursor.fetchall()
        ]

    @staticmethod
    def _fetch_fk_constraints(cursor: DictCursor) -> list[ForeignKeyConstraint]:
        result: list[ForeignKeyConstraint] = []

        for query in FOREIGN_KEY_CONSTRAINTS_QUERIES:
            cursor.execute(query)

        # for clearing string representation of list, in order to further transformation into tuple
        translation_table = str.maketrans(
            {"[": None, "]": None, "\n": None, " ": None, '"': None}
        )

        for raw_object in cursor.fetchall():
            for col in ("foreign_key", "referenced_foreign_key"):
                raw_object[col] = tuple(
                    raw_object[col].translate(translation_table).split(",")
                )
            result.append(ForeignKeyConstraint.parse_obj(LowerKeyDict(raw_object)))
        return result
