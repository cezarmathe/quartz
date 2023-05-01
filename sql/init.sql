---
--- src/init.sql
---

create table if not exists horloge.timer_tables (
    relid oid primary key references pg_class
);

create or replace function horloge.ensure_relid_is_table()
returns trigger
as $$
begin
    if (TG_TABLE_SCHEMA != 'horloge' or TG_TABLE_NAME != 'timer_tables') then
        raise exception 'horloge.ensure_relid_is_table(): must be used only on the "horloge.timer_tables" table';
    end if;

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
