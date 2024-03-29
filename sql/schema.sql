/* 
This file is auto generated by pgrx.

The ordering of items is not stable, it is driven by a dependency graph.
*/

-- src/lib.rs:31
CREATE SCHEMA IF NOT EXISTS quartz; /* quartz::quartz */

-- src/lib.rs:47
-- quartz::quartz::quartz_timers_before_update
CREATE FUNCTION quartz."quartz_timers_before_update"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'quartz_timers_before_update_wrapper';

-- src/lib.rs:47
-- quartz::quartz::quartz_timers_before_insert
CREATE FUNCTION quartz."quartz_timers_before_insert"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'quartz_timers_before_insert_wrapper';

-- src/lib.rs:47
-- quartz::quartz::quartz_timers_before_delete
CREATE FUNCTION quartz."quartz_timers_before_delete"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'quartz_timers_before_delete_wrapper';

-- src/lib.rs:47
-- quartz::quartz::quartz_timers_after_update
CREATE FUNCTION quartz."quartz_timers_after_update"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'quartz_timers_after_update_wrapper';

-- src/lib.rs:47
-- quartz::quartz::quartz_timers_after_insert
CREATE FUNCTION quartz."quartz_timers_after_insert"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'quartz_timers_after_insert_wrapper';

-- src/lib.rs:47
-- quartz::quartz::quartz_timers_after_delete
CREATE FUNCTION quartz."quartz_timers_after_delete"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'quartz_timers_after_delete_wrapper';

-- src/lib.rs:76
-- quartz::quartz::deactivate_timers
CREATE  FUNCTION quartz."deactivate_timers"(
	"rel" TEXT /* &str */
) RETURNS void
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'deactivate_timers_wrapper';

-- src/lib.rs:88
-- quartz::quartz::create_timers_table
CREATE  FUNCTION quartz."create_timers_table"(
	"rel" TEXT /* &str */
) RETURNS void
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'create_timers_table_wrapper';

-- src/lib.rs:64
-- quartz::quartz::activate_timers
CREATE  FUNCTION quartz."activate_timers"(
	"rel" TEXT /* &str */
) RETURNS void
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'activate_timers_wrapper';

-- src/lib.rs:15
---
--- src/init.sql
---

create table quartz.timer_relations (
    relid oid primary key
);

create function quartz.check_relation_is_table()
returns trigger
as $$
begin
    if (TG_TABLE_SCHEMA != 'quartz' or TG_TABLE_NAME != 'timer_relations') then
        raise exception 'quartz.ensure_relid_is_table(): must be used only on the "quartz.timer_tables" table';
    end if;

    if not exists (
        select 1 from pg_class where oid = new.relid and relkind = 'r'
    ) then
        raise exception 'relid % is not a table', new.relid;
    end if;
    return new;
end;
$$ language plpgsql;

create trigger check_relation_is_table
    before insert or update on quartz.timer_relations
    for each row execute function quartz.check_relation_is_table();

