/*
This file is auto generated by pgrx.

The ordering of items is not stable, it is driven by a dependency graph.
*/

-- src/lib.rs:29
CREATE SCHEMA IF NOT EXISTS horloge; /* horloge::horloge */

-- src/lib.rs:45
-- horloge::horloge::horloge_timers_before_update
CREATE FUNCTION horloge."horloge_timers_before_update"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'horloge_timers_before_update_wrapper';

-- src/lib.rs:45
-- horloge::horloge::horloge_timers_before_insert
CREATE FUNCTION horloge."horloge_timers_before_insert"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'horloge_timers_before_insert_wrapper';

-- src/lib.rs:45
-- horloge::horloge::horloge_timers_before_delete
CREATE FUNCTION horloge."horloge_timers_before_delete"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'horloge_timers_before_delete_wrapper';

-- src/lib.rs:45
-- horloge::horloge::horloge_timers_after_update
CREATE FUNCTION horloge."horloge_timers_after_update"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'horloge_timers_after_update_wrapper';

-- src/lib.rs:45
-- horloge::horloge::horloge_timers_after_insert
CREATE FUNCTION horloge."horloge_timers_after_insert"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'horloge_timers_after_insert_wrapper';

-- src/lib.rs:45
-- horloge::horloge::horloge_timers_after_delete
CREATE FUNCTION horloge."horloge_timers_after_delete"()
	RETURNS TRIGGER
	LANGUAGE c
	AS 'MODULE_PATHNAME', 'horloge_timers_after_delete_wrapper';

-- src/lib.rs:74
-- horloge::horloge::deactivate_timers
CREATE  FUNCTION horloge."deactivate_timers"(
	"rel" TEXT /* &str */
) RETURNS void
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'deactivate_timers_wrapper';

-- src/lib.rs:62
-- horloge::horloge::activate_timers
CREATE  FUNCTION horloge."activate_timers"(
	"rel" TEXT /* &str */
) RETURNS void
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'activate_timers_wrapper';

-- src/lib.rs:13
---
--- src/init.sql
---

create schema if not exists horloge;

create table if not exists horloge.timer_tables (
    relid oid primary key references pg_class
);

create or replace function horloge.ensure_relid_is_table()
returns trigger
as $$
begin
    if not exists (
        select 1 from pg_class where oid = new.relid and relkind = 'r'
    ) then
        raise exception 'relid % is not a table', new.relid;
    end if;
    return new;
end;
$$ language plpgsql;

create trigger ensure_relid_is_table
    before insert or update on horloge.timer_tables
    for each row execute function horloge.ensure_relid_is_table();
