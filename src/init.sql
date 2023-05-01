---
--- src/init.sql
---

create schema if not exists horloge;

create table if not exists horloge."timer_tables" (
    relid oid primary key references pg_class,
);
