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
