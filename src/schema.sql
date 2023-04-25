--
-- src/schema.sql
--

create schema if not exists horloge;

create table if not exists horloge.timers (
    id bigint primary key generated always as identity,
    ts timestamp without time zone not null
);

create index if not exists timers_ts_idx on horloge.timers (ts);

create or replace trigger on_horloge_timers_event
    after insert or update or delete on horloge.timers
    for each row execute procedure on_horloge_timers_event();
