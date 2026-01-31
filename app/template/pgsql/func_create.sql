-- Create a custom function to get the table creation DDL of a specific table in a schema.
CREATE OR REPLACE FUNCTION show_create_table_schema(
    in_schema_name varchar,
    in_table_name varchar
)
    RETURNS text
    LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
    -- the ddl we're building
v_table_ddl text;

    -- data about the target table
    v_table_oid int;

    v_table_type char;
    v_partition_key varchar;
    v_table_comment varchar;

    -- records for looping
    v_column_record record;
    v_constraint_record record;
    v_index_record record;
    v_column_comment_record record;
    v_index_comment_record record;
    v_constraint_comment_record record;
BEGIN
    -- grab the oid of the table; https://www.postgresql.org/docs/8.3/catalog-pg-class.html
SELECT c.oid, c.relkind INTO v_table_oid, v_table_type
FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind in ('r', 'p')
  AND c.relname = in_table_name -- the table name
  AND n.nspname = in_schema_name; -- the schema

-- throw an error if table was not found
IF (v_table_oid IS NULL) THEN
        RAISE EXCEPTION 'table does not exist';
END IF;

    -- start the create definition
    v_table_ddl := 'CREATE TABLE "' || in_table_name || '" (' || E'\n';

    -- define all of the columns in the table; https://stackoverflow.com/a/8153081/3068233
FOR v_column_record IN
SELECT
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    c.is_nullable,
    c.column_default
FROM information_schema.columns c
WHERE (table_schema, table_name) = (in_schema_name, in_table_name)
ORDER BY ordinal_position
    LOOP
            v_table_ddl := v_table_ddl || '  ' -- note: two char spacer to start, to indent the column
                               || '"' || v_column_record.column_name || '" '
                               || v_column_record.data_type || CASE WHEN v_column_record.character_maximum_length IS NOT NULL THEN ('(' || v_column_record.character_maximum_length || ')') ELSE '' END || ' '
                               || CASE WHEN v_column_record.is_nullable = 'NO' THEN 'NOT NULL' ELSE 'NULL' END
                               || CASE WHEN v_column_record.column_default IS NOT null THEN (' DEFAULT ' || replace(v_column_record.column_default, '"', '') ) ELSE '' END
                               || ',' || E'\n';
END LOOP;

    -- define all the constraints in the; https://www.postgresql.org/docs/9.1/catalog-pg-constraint.html && https://dba.stackexchange.com/a/214877/75296
FOR v_constraint_record IN
SELECT
    con.conname as constraint_name,
    con.contype as constraint_type,
    CASE
        WHEN con.contype = 'p' THEN 1 -- primary key constraint
        WHEN con.contype = 'u' THEN 2 -- unique constraint
        WHEN con.contype = 'f' THEN 3 -- foreign key constraint
        WHEN con.contype = 'c' THEN 4
        ELSE 5
        END as type_rank,
    pg_get_constraintdef(con.oid) as constraint_definition
FROM pg_catalog.pg_constraint con
         JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
         JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
WHERE nsp.nspname = in_schema_name
  AND rel.relname = in_table_name
ORDER BY type_rank
    LOOP
            IF v_constraint_record.constraint_type = 'p' THEN
                v_table_ddl := v_table_ddl || '  '
                                   || v_constraint_record.constraint_definition
                                   || ',' || E'\n';
ELSE
                v_table_ddl := v_table_ddl || '  ' -- note: two char spacer to start, to indent the column
                                   || 'CONSTRAINT' || ' '
                                   || '"' || v_constraint_record.constraint_name || '" '
                                   || v_constraint_record.constraint_definition
                                   || ',' || E'\n';
END IF;
END LOOP;

    -- drop the last comma before ending the create statement
    v_table_ddl = substr(v_table_ddl, 0, length(v_table_ddl) - 1) || E'\n';

    -- end the create definition
    v_table_ddl := v_table_ddl || ')';

    IF v_table_type = 'p' THEN
SELECT pg_get_partkeydef(v_table_oid) INTO v_partition_key;
IF v_partition_key IS NOT NULL THEN
            v_table_ddl := v_table_ddl || ' PARTITION BY ' || v_partition_key;
END IF;
END IF;

    v_table_ddl := v_table_ddl || ';' || E'\n';

    -- suffix create statement with all of the indexes on the table
FOR v_index_record IN
SELECT regexp_replace(indexdef, ' "?' || schemaname || '"?\.', ' ') AS indexdef
FROM pg_catalog.pg_indexes
WHERE (schemaname, tablename) = (in_schema_name, in_table_name)
  AND indexname NOT IN (
    select con.conname
    FROM pg_catalog.pg_constraint con
             JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
             JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
    WHERE nsp.nspname = in_schema_name
      AND rel.relname = in_table_name
)
    LOOP
            v_table_ddl := v_table_ddl
                               || v_index_record.indexdef
                               || ';' || E'\n';
END LOOP;

    -- comment on table
SELECT description INTO v_table_comment
FROM pg_catalog.pg_description
WHERE objoid = v_table_oid AND objsubid = 0;

IF v_table_comment IS NOT NULL THEN
        v_table_ddl := v_table_ddl || 'COMMENT ON TABLE "' || in_table_name || '" IS ''' || replace(v_table_comment, '''', '''''') || ''';' || E'\n';
END IF;

    -- comment on column
FOR v_column_comment_record IN
SELECT col.column_name, d.description
FROM information_schema.columns col
         JOIN pg_catalog.pg_class c ON c.relname = col.table_name
         JOIN pg_catalog.pg_namespace nsp ON nsp.oid = c.relnamespace AND col.table_schema = nsp.nspname
         JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = col.ordinal_position
WHERE c.oid = v_table_oid
ORDER BY col.ordinal_position
    LOOP
            v_table_ddl := v_table_ddl || 'COMMENT ON COLUMN "' || in_table_name || '"."'
                               || v_column_comment_record.column_name || '" IS '''
                               || replace(v_column_comment_record.description, '''', '''''') || ''';' || E'\n';
END LOOP;

    -- comment on index
FOR v_index_comment_record IN
SELECT c.relname, d.description
FROM pg_catalog.pg_index idx
         JOIN pg_catalog.pg_class c ON idx.indexrelid = c.oid
         JOIN pg_catalog.pg_description d ON idx.indexrelid = d.objoid
WHERE idx.indrelid = v_table_oid
    LOOP
            v_table_ddl := v_table_ddl || 'COMMENT ON INDEX "'
                               || v_index_comment_record.relname || '" IS '''
                               || replace(v_index_comment_record.description, '''', '''''') || ''';' || E'\n';
END LOOP;

    -- comment on constraint
FOR v_constraint_comment_record IN
SELECT
    con.conname,
    pg_description.description
FROM pg_catalog.pg_constraint con
         JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
         JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
         JOIN pg_catalog.pg_description ON pg_description.objoid = con.oid
WHERE rel.oid = v_table_oid
    LOOP
            v_table_ddl := v_table_ddl || 'COMMENT ON CONSTRAINT "'
                               || v_constraint_comment_record.conname || '" ON "' || in_table_name || '" IS '''
                               || replace(v_constraint_comment_record.description, '''', '''''') || ''';' || E'\n';
END LOOP;

    -- return the ddl
RETURN v_table_ddl;
END
$$;

-- Call function.
-- SELECT show_create_table_schema('public', 'student');



-- Create a custom function to get the DDL of a specific table in the current schema.
CREATE OR REPLACE FUNCTION show_create_table(
    in_table_name varchar
)
    RETURNS text
    LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
    -- the ddl we're building
v_table_ddl text;

    -- data about the target table
    v_table_oid int;

    v_table_type char;
    v_partition_key varchar;
    v_namespace varchar;
    v_table_comment varchar;

    -- records for looping
    v_column_record record;
    v_constraint_record record;
    v_index_record record;
    v_column_comment_record record;
    v_index_comment_record record;
    v_constraint_comment_record record;
BEGIN
    -- grab the oid of the table; https://www.postgresql.org/docs/8.3/catalog-pg-class.html
SELECT c.oid, c.relkind, n.nspname INTO v_table_oid, v_table_type, v_namespace
FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind in ('r', 'p')
  AND c.relname = in_table_name -- the table name
  AND pg_catalog.pg_table_is_visible(c.oid); -- the schema

-- throw an error if table was not found
IF (v_table_oid IS NULL) THEN
        RAISE EXCEPTION 'table does not exist';
END IF;

    -- start the create definition
    v_table_ddl := 'CREATE TABLE "' || in_table_name || '" (' || E'\n';

    -- define all of the columns in the table; https://stackoverflow.com/a/8153081/3068233
FOR v_column_record IN
SELECT
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    c.is_nullable,
    c.column_default
FROM information_schema.columns c
WHERE table_name = in_table_name and table_schema = v_namespace
ORDER BY ordinal_position
    LOOP
            v_table_ddl := v_table_ddl || '  ' -- note: two char spacer to start, to indent the column
                               || '"' || v_column_record.column_name || '" '
                               || v_column_record.data_type || CASE WHEN v_column_record.character_maximum_length IS NOT NULL THEN ('(' || v_column_record.character_maximum_length || ')') ELSE '' END || ' '
                               || CASE WHEN v_column_record.is_nullable = 'NO' THEN 'NOT NULL' ELSE 'NULL' END
                               || CASE WHEN v_column_record.column_default IS NOT null THEN (' DEFAULT ' || replace(v_column_record.column_default, '"', '') ) ELSE '' END
                               || ',' || E'\n';
END LOOP;

    -- define all the constraints in the; https://www.postgresql.org/docs/9.1/catalog-pg-constraint.html && https://dba.stackexchange.com/a/214877/75296
FOR v_constraint_record IN
SELECT
    con.conname as constraint_name,
    con.contype as constraint_type,
    CASE
        WHEN con.contype = 'p' THEN 1 -- primary key constraint
        WHEN con.contype = 'u' THEN 2 -- unique constraint
        WHEN con.contype = 'f' THEN 3 -- foreign key constraint
        WHEN con.contype = 'c' THEN 4
        ELSE 5
        END as type_rank,
    pg_get_constraintdef(con.oid) as constraint_definition
FROM pg_catalog.pg_constraint con
         JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
         JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
WHERE rel.relname = in_table_name
  AND pg_catalog.pg_table_is_visible(rel.oid)
ORDER BY type_rank
    LOOP
            IF v_constraint_record.constraint_type = 'p' THEN
                v_table_ddl := v_table_ddl || '  '
                                   || v_constraint_record.constraint_definition
                                   || ',' || E'\n';
ELSE
                v_table_ddl := v_table_ddl || '  ' -- note: two char spacer to start, to indent the column
                                   || 'CONSTRAINT' || ' '
                                   || '"' || v_constraint_record.constraint_name || '" '
                                   || v_constraint_record.constraint_definition
                                   || ',' || E'\n';
END IF;
END LOOP;

    -- drop the last comma before ending the create statement
    v_table_ddl = substr(v_table_ddl, 0, length(v_table_ddl) - 1) || E'\n';

    -- end the create definition
    v_table_ddl := v_table_ddl || ')';

    IF v_table_type = 'p' THEN
SELECT pg_get_partkeydef(v_table_oid) INTO v_partition_key;
IF v_partition_key IS NOT NULL THEN
            v_table_ddl := v_table_ddl || ' PARTITION BY ' || v_partition_key;
END IF;
END IF;

    v_table_ddl := v_table_ddl || ';' || E'\n';

    -- suffix create statement with all of the indexes on the table
FOR v_index_record IN
SELECT regexp_replace(idx.indexdef, ' "?' || idx.schemaname || '"?\.', ' ') AS indexdef
FROM pg_indexes idx
         JOIN (
    SELECT ns.nspname, cls.relname
    FROM pg_catalog.pg_class cls
             LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = cls.relnamespace
    WHERE pg_catalog.pg_table_is_visible(cls.oid)
) t ON idx.schemaname = t.nspname AND idx.tablename = t.relname
WHERE idx.tablename = in_table_name
  AND idx.indexname NOT IN (
    select con.conname
    FROM pg_catalog.pg_constraint con
             JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
             JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
    WHERE rel.relname = in_table_name
      AND pg_catalog.pg_table_is_visible(rel.oid)
)
    LOOP
            v_table_ddl := v_table_ddl
                               || v_index_record.indexdef
                               || ';' || E'\n';
END LOOP;

    -- comment on table
SELECT description INTO v_table_comment
FROM pg_catalog.pg_description
WHERE objoid = v_table_oid AND objsubid = 0;

IF v_table_comment IS NOT NULL THEN
        v_table_ddl := v_table_ddl || 'COMMENT ON TABLE "' || in_table_name || '" IS ''' || replace(v_table_comment, '''', '''''') || ''';' || E'\n';
END IF;

    -- comment on column
FOR v_column_comment_record IN
SELECT col.column_name, d.description
FROM information_schema.columns col
         JOIN pg_catalog.pg_class c ON c.relname = col.table_name
         JOIN pg_catalog.pg_namespace nsp ON nsp.oid = c.relnamespace AND col.table_schema = nsp.nspname
         JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = col.ordinal_position
WHERE c.oid = v_table_oid
ORDER BY col.ordinal_position
    LOOP
            v_table_ddl := v_table_ddl || 'COMMENT ON COLUMN "' || in_table_name || '"."'
                               || v_column_comment_record.column_name || '" IS '''
                               || replace(v_column_comment_record.description, '''', '''''') || ''';' || E'\n';
END LOOP;

    -- comment on index
FOR v_index_comment_record IN
SELECT c.relname, d.description
FROM pg_catalog.pg_index idx
         JOIN pg_catalog.pg_class c ON idx.indexrelid = c.oid
         JOIN pg_catalog.pg_description d ON idx.indexrelid = d.objoid
WHERE idx.indrelid = v_table_oid
    LOOP
            v_table_ddl := v_table_ddl || 'COMMENT ON INDEX "'
                               || v_index_comment_record.relname || '" IS '''
                               || replace(v_index_comment_record.description, '''', '''''') || ''';' || E'\n';
END LOOP;

    -- comment on constraint
FOR v_constraint_comment_record IN
SELECT
    con.conname,
    pg_description.description
FROM pg_catalog.pg_constraint con
         JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
         JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
         JOIN pg_catalog.pg_description ON pg_description.objoid = con.oid
WHERE rel.oid = v_table_oid
    LOOP
            v_table_ddl := v_table_ddl || 'COMMENT ON CONSTRAINT "'
                               || v_constraint_comment_record.conname || '" ON "' || in_table_name || '" IS '''
                               || replace(v_constraint_comment_record.description, '''', '''''') || ''';' || E'\n';
END LOOP;

    -- return the ddl
RETURN v_table_ddl;
END
$$;

-- SELECT show_create_table('student');
