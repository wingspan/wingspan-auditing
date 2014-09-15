-- The following example queries show some things you ca do with audit data
set search_path to movie_audit_demo;

-- Retrieve audit history for each entity
select * from movies$a;
select * from licenses$a;

-- Find out who changed something last
select * from blame('movies', 'title');
select * from blame('licenses', 'title');

create or replace function test_undo() returns void
language plpgsql AS $$
declare
  txid int;
begin
  select audit_txid from movies$a order by 1 offset 1 limit 1 into txid;
  perform undo(txid, null);
end;
$$;

select test_undo();

select * from movies where id = 1;

create or replace function test_redo() returns void
language plpgsql AS $$
declare
  txid int;
begin
  select audit_txid from movies$a order by 1 offset 1 limit 1 into txid;
  perform redo(txid, null);
end;
$$;

select test_redo();
select * from movies where id = 1;

drop function test_undo();
drop function test_redo();

