from dataclasses import asdict, dataclass
from operator import attrgetter
from typing import Union

import psycopg2
from funcy.seqs import group_by
from odd_collector.adapters.postgresql.models import (
    Column,
    EnumTypeLabel,
    ForeignKeyConstraint,
    PrimaryKey,
    Schema,
    Table,
    UniqueConstraint,
)
from odd_collector.domain.plugin import PostgreSQLPlugin
from odd_collector_sdk.domain.filter import Filter
from psycopg2 import sql


@dataclass(frozen=True)
class ConnectionParams:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_config(cls, config: PostgreSQLPlugin):
        return cls(
            dbname=config.database,
            user=config.user,
            password=config.password.get_secret_value(),
            host=config.host,
            port=config.port,
        )


class PostgreSQLRepository:
    def __init__(self, conn_params: ConnectionParams, schemas_filter: Filter):
        self.conn_params = conn_params
        self.schemas_filter = schemas_filter

    def __enter__(self):
        self.conn = psycopg2.connect(**asdict(self.conn_params))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def get_schemas(self) -> list[Schema]:
        with self.conn.cursor() as cur:
            schemas = [
                Schema(*raw)
                for raw in self.execute(self.schemas_query, cur)
                if self.schemas_filter.is_allowed(raw[0])
                and raw[0]
                not in (
                    "pg_toast",
                    "pg_internal",
                    "catalog_history",
                    "pg_catalog",
                    "information_schema",
                )
            ]
        return schemas

    @property
    def get_filtered_schema_names_str(self):
        schemas = [schema.schema_name for schema in self.get_schemas()]
        return ", ".join([f"'{schema}'" for schema in schemas])

    def get_tables(self) -> list[Table]:
        query = self.tables_query(self.get_filtered_schema_names_str)

        with self.conn.cursor() as cur:
            tables = [Table(*raw) for raw in self.execute(query, cur)]
            grouped_columns = group_by(attrgetter("attrelid"), self.get_columns())

            for table in tables:
                if columns := grouped_columns[table.oid]:
                    table.columns.extend(columns)

            return tables

    def get_columns(self):
        with self.conn.cursor() as cur:
            enums = self.get_enums()
            primary_keys = self.get_primary_keys()

            grouped_enums = group_by(attrgetter("type_oid"), enums)
            grouped_pks = group_by(attrgetter("attrelid", "column_name"), primary_keys)

            raw_data = self.execute(
                self.columns_query(self.get_filtered_schema_names_str), cur
            )
            columns = [Column(*raw) for raw in raw_data]

            for column in columns:
                if enums := grouped_enums[column.type_oid]:
                    column.enums.extend(enums)
                if (column.attrelid, column.column_name) in grouped_pks:
                    column.is_primary = True

            return columns

    def get_enums(self):
        with self.conn.cursor() as cur:
            raw_data = self.execute(
                self.enums_query(self.get_filtered_schema_names_str), cur
            )
            return [EnumTypeLabel(*raw) for raw in raw_data]

    def get_primary_keys(self):
        with self.conn.cursor() as cur:
            raw_data = self.execute(
                self.pks_query(self.get_filtered_schema_names_str), cur
            )
            return [PrimaryKey(*raw) for raw in raw_data]

    def get_foreign_key_constraints(self):
        with self.conn.cursor() as cur:
            fk_constraints = [
                (
                    oid,
                    cn,
                    nsp_oid,
                    nsp,
                    tn,
                    tc,
                    rnsp_oid,
                    rnsp,
                    rtn,
                    rtc,
                    tuple(fk),
                    tuple(fka),
                    tuple(rfk),
                    tuple(rfka),
                )
                for oid, cn, nsp_oid, nsp, tn, tc, rnsp_oid, rnsp, rtn, rtc, fk, fka, rfk, rfka in self.execute(
                    self.foreign_key_constraints_query(
                        self.get_filtered_schema_names_str
                    ),
                    cur,
                )
            ]
            return [ForeignKeyConstraint(*fkc) for fkc in fk_constraints]

    def get_unique_constraints(self):
        with self.conn.cursor() as cur:
            raw_data = self.execute(
                self.unique_constraints_query(self.get_filtered_schema_names_str), cur
            )
            return [UniqueConstraint(*raw) for raw in raw_data]

    @staticmethod
    def pks_query(schemas: str):
        return f"""
            select c.relname, a.attname, a.attrelid
            from pg_index i
                join pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                join pg_catalog.pg_class c on c.oid = a.attrelid
                join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where i.indisprimary
                and c.relkind in ('r', 'v', 'm', 'p')
                and c.relispartition = false -- exclude partiotions
                and a.attnum > 0
                and n.nspname in ({schemas}) 
        """

    @property
    def schemas_query(self):
        return """
            select 
                n.nspname as schema_name, 
                pg_catalog.pg_get_userbyid(n.nspowner) as schema_owner,
                n.oid as oid,
                pg_catalog.obj_description(n.oid, 'pg_namespace') as description,
                pg_total_relation_size(n.oid) as total_size_bytes
            from pg_catalog.pg_namespace n
            where n.nspname not like all(array['pg_temp_%', 'pg_toast_%'])
            and n.nspname not in ('pg_toast', 'pg_internal', 'catalog_history', 'pg_catalog', 'information_schema');
        """

    @staticmethod
    def tables_query(schemas: str):
        return f"""
            select
                c.oid
                , it.table_catalog
                , n.nspname as table_schema
                , c.relname as table_name
                , c.relkind as table_type
                , it.self_referencing_column_name
                , it.reference_generation
                , it.user_defined_type_catalog
                , it.user_defined_type_schema
                , it.user_defined_type_name
                , it.is_insertable_into
                , it.is_typed
                , it.commit_action
                , pg_get_viewdef(c.oid, true)            as view_definition
                , iw.check_option                        as view_check_option
                , iw.is_updatable                        as view_is_updatable
                , iw.is_insertable_into                  as view_is_insertable_into
                , iw.is_trigger_updatable                as view_is_trigger_updatable
                , iw.is_trigger_deletable                as view_is_trigger_deletable
                , iw.is_trigger_insertable_into          as view_is_trigger_insertable_into
                , pg_catalog.pg_get_userbyid(c.relowner) as table_owner
                , c.reltuples                            as table_rows
                , pg_catalog.obj_description(c.oid)      as description
            from pg_catalog.pg_class c
                    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                    left join information_schema.tables it on it.table_schema = n.nspname and it.table_name = c.relname
                    left join information_schema.views iw on iw.table_schema = n.nspname and iw.table_name = c.relname
            where c.relkind in ('r', 'v', 'm', 'p')
                and c.relispartition = false -- exclude partiotions
                and n.nspname not like 'pg_temp_%'
                and n.nspname in ({schemas}) 
            order by n.nspname, c.relname
        """

    @staticmethod
    def columns_query(schemas: str):
        return f"""
            select
                a.attrelid
                , ic.table_catalog
                , nspname as table_schema
                , c.relname as table_name
                , a.attname as column_name
                , ic.ordinal_position
                , ic.column_default
                , ic.is_nullable
                , t.typname as data_type
                , ic.character_maximum_length
                , ic.character_octet_length
                , ic.numeric_precision
                , ic.numeric_precision_radix
                , ic.numeric_scale
                , ic.datetime_precision
                , ic.interval_type
                , ic.interval_precision
                , ic.character_set_catalog
                , ic.character_set_schema
                , ic.character_set_name
                , ic.collation_catalog
                , ic.collation_schema
                , ic.collation_name
                , ic.domain_catalog
                , ic.domain_schema
                , ic.domain_name
                , ic.udt_catalog
                , ic.udt_schema
                , ic.udt_name
                , ic.scope_catalog
                , ic.scope_schema
                , ic.scope_name
                , ic.maximum_cardinality
                , ic.dtd_identifier
                , ic.is_self_referencing
                , ic.is_identity
                , ic.identity_generation
                , ic.identity_start
                , ic.identity_increment
                , ic.identity_maximum
                , ic.identity_minimum
                , ic.identity_cycle
                , ic.is_generated
                , ic.generation_expression
                , ic.is_updatable
                , pg_catalog.col_description(c.oid, a.attnum) as description
                , a.atttypid as type_oid
            from pg_catalog.pg_attribute a
                    join pg_catalog.pg_class c on c.oid = a.attrelid
                    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                    join pg_catalog.pg_type t on t.oid = a.atttypid
                    left join information_schema.columns ic on ic.table_schema = n.nspname
                        and ic.table_name = c.relname
                        and ic.ordinal_position = a.attnum
            where c.relkind in ('r', 'v', 'm', 'p')
                and c.relispartition = false -- exclude partiotions
                and a.attnum > 0
                and n.nspname in ({schemas}) 
                and a.attisdropped is false
            order by n.nspname, c.relname, a.attnum
        """

    @staticmethod
    def enums_query(schemas: str):
        return f"""
            select
                pe.enumtypid as type_oid
                , pt.typname as type_name
                , pe.enumlabel as label
            from pg_enum pe
            join pg_type pt on pt.oid = pe.enumtypid
            join pg_catalog.pg_namespace n on n.oid = pt.typnamespace
            where n.nspname in ({schemas})
            order by pe.enumsortorder
        """

    @staticmethod
    def foreign_key_constraints_query(schemas: str) -> str:
        return f"""
            SELECT
                subq.oid
                , conname AS constraint_name
                , ns.oid AS schema_oid
                , ns.nspname AS schema_name
                , c.relname AS table_name
                , conrelid AS table_conrelid
                , rc.relnamespace AS referenced_schema_oid
                , rns.nspname AS referenced_schema_name
                , rc.relname AS referenced_table_name
                , confrelid AS referenced_table_confrelid
                , array_agg(ta.attname ORDER BY unnested_ordinality) AS fkey
                , array_agg(unnested_conkey ORDER BY unnested_ordinality) AS fkey_attnum
                , array_agg(rta.attname ORDER BY unnested_ordinality) AS referenced_fkey
                , array_agg(unnested_confkey ORDER BY unnested_ordinality) AS referenced_fkey_attnum
            FROM (
                SELECT
                    oid
                    , conname
                    , connamespace
                    , conrelid
                    , confrelid
                    , unnested_conkey
                    , unnested_confkey
                    , unnested_ordinality
                FROM
                    pg_catalog.pg_constraint,
                    unnest(conkey, confkey) WITH ORDINALITY AS t(
                        unnested_conkey,
                        unnested_confkey,
                        unnested_ordinality
                    )
                WHERE
                    contype = 'f'       -- only foreign keys
                    AND conparentid = 0 -- exclude constraints on partitions
            ) subq
                JOIN pg_catalog.pg_class AS c
                    ON c.oid = conrelid
                JOIN pg_catalog.pg_class AS rc
                    ON rc.oid = confrelid
                JOIN pg_catalog.pg_namespace AS ns  -- table namespace
                    ON ns.oid = connamespace
                JOIN pg_catalog.pg_namespace AS rns -- referenced table namespace
                    ON rns.oid = rc.relnamespace
                JOIN pg_catalog.pg_attribute AS ta  -- table attribute
                    ON ta.attrelid = conrelid AND ta.attnum = unnested_conkey
                JOIN pg_catalog.pg_attribute AS rta -- referenced table attribute
                    ON rta.attrelid = confrelid AND rta.attnum = unnested_confkey
            WHERE ns.nspname IN ({schemas}) AND rns.nspname in ({schemas})
            GROUP BY
                subq.oid, conname, conrelid, confrelid, ns.oid, ns.nspname, c.relname, rc.relnamespace, rns.nspname, rc.relname;
        """

    @staticmethod
    def unique_constraints_query(schemas: str):
        return f"""
            SELECT
                con.oid AS oid
                , con.conname AS constraint_name
                , ns.nspname AS schema_name
                , con.conrelid AS table_conrelid
                , c.relname AS table_name
                , ARRAY_AGG(att.attname) AS column_names
            FROM pg_catalog.pg_constraint AS con
                JOIN pg_catalog.pg_class AS c
                    ON c.oid = con.conrelid
                JOIN pg_catalog.pg_attribute AS att
                    ON con.conrelid = att.attrelid AND att.attnum = ANY(con.conkey)
                JOIN pg_catalog.pg_namespace AS ns
                    ON ns.oid = con.connamespace
            WHERE
                con.contype = 'u' AND ns.nspname IN ({schemas})
            GROUP BY
                con.oid, con.conname, con.conrelid, c.relname, ns.nspname;
        """

    @staticmethod
    def execute(query: Union[str, sql.Composed], cursor) -> list[tuple]:
        cursor.execute(query)
        return cursor.fetchall()
