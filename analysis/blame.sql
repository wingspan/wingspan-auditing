create type blame_type as (
  id integer, 
  output varchar,
  audit_action varchar,
  audit_request varchar,  
  audit_txid bigint,
  audit_user varchar,
  audit_date timestamp);

create or replace function blame(table_name varchar, column_name varchar) returns 
  setof blame_type
language plpgsql AS $fn$
declare
  r record;
begin
  perform format('create type ()');
  
  for r in
  execute format ('
  select 
    id,
    column$c::varchar output,
    audit_action,
    audit_request,  
    audit_txid,
    audit_user,
    audit_date
  from 
  (
    select 
      audit_action,
      audit_request,  
      audit_txid,
      audit_user,
      audit_date,
      id,
      column$p,
      column$c,
      max(audit_txid) over (partition by id) audit_txid_max
    from (
      select 
        id,
        %s column$c,
        lead(%s) over w column$p,
        audit_action,
        audit_request,  
        audit_txid,
        audit_user,
        audit_date
      from %s$a
      where audit_action in (''I'', ''U'')
      window w AS (partition by id order by audit_date desc)
    ) a
    where (column$p <> column$c or audit_action = ''I'')
  ) b
  where audit_txid_max = audit_txid',
  column_name,
  column_name,
  table_name
  )
  loop
    return next r;
  end loop;  
end;
$fn$;
