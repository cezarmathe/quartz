select
    tr.relid,
    pn.nspname::text as schema_name,
    pc.relname::text as table_name
from quartz.timer_relations tr
join pg_class pc on tr.relid = pc.oid
join pg_namespace pn on pc.relnamespace = pn.oid;
