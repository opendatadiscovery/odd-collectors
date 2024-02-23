## Logic that has been used to determine relationships' Cardinality type and is_identifying status

Relationship:       `Parent table    ----fk_constraint----<    Child table`

We also use termes source or referencing table according `child table`, and
target or referenced table according `parent table`.


**Lets consider 2 tables:**
```
parent_table (
    CONSTRAINT parent_table_pk PRIMARY KEY (..., ...)                   -- alias: target_pk_columns
)
```
and
```
child_table (
    CONSTRAINT child_table_pk PRIMARY KEY (..., ...)                    -- alias: source_pk_columns

    CONSTRAINT child_parent_constraint_fk FOREIGN KEY (..., ...)        -- alias: source_fk_columns
        REFERENCES parent_table (..., ...)                              -- alias: target_fk_columns
)
```

### is_identifying determination
If `(target_fk_columns == target_pk_columns)` and `(source_fk_columns is a subset of source_pk_columns)`
then relationships **is identigying**, else - **not identigying**.

### Cardinality determination
If `any of source_fk_columns is UNIQUE or PRIMARY KEY` then relationship's cardinality is
**ONE_TO_ZERO_OR_ONE**, else - **ONE_TO_ZERO_ONE_OR_MORE**.
