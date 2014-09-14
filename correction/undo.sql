create or replace function undo(
  audit_txid bigint, audit_request varchar, audit_action varchar, audit_user varchar, audit_interval interval, audit_tables varchar[]
) returns void
language plpgsql AS $$
declare
  tables record;
  from_data record;
  to_data record;

  where_clause text;
  table_name text;
  from_sql text;
  to_sql text;
  undo_sql text;

  columns_list text;
  columns_type text;
  columns_insert text;

  reserved_columns varchar[];
begin
  reserved_columns := array['audit_action', 'audit_date', 'audit_request', 'audit_txid', 'audit_user', 'id'];

  where_clause := 'where';
  if (audit_txid is not null) then
    where_clause := where_clause || ' and audit_data.audit_txid = ' || audit_txid;
  end if;

  if (audit_request is not null) then
    where_clause := where_clause || ' and audit_data.audit_request = ''' || format('%I', audit_request) || '''';
  end if;

  if (audit_action is not null) then
    where_clause := where_clause || ' and audit_data.audit_action = ''' || format('%I', audit_action) || '''';
  end if;

  if (audit_user is not null) then
    where_clause := where_clause || ' and audit_data.audit_user = ''' || format('%I', audit_user) || '''';
  end if;

  if (audit_interval is not null) then
    where_clause := where_clause || ' and audit_data.audit_interval <% interval ''' || format('%I', audit_interval) || '''';
  end if;

  where_clause := regexp_replace(where_clause, 'where and', 'where');

  -- todo sorting
  for tables in 
    select * 
    from information_schema.tables t
    where t.table_name not like '%$a' 
      and t.table_schema = current_schema
      and (audit_tables is null or t.table_name = any (audit_tables))
  loop  
    table_name = current_schema || '.' || format('%I', tables.table_name) || '$a';

    -- find out which columns changed, and the prior values.
    -- sort these in order they happened so we can undo them in
    -- reverse
    from_sql := 
      format(
'-- undo query
with prior as (
  select rank() over w,
         audit_data.*         
  from %s audit_data
  %s
  window w as (partition by id order by audit_date desc)
  order by 1 
  offset 1 
  limit 1
)
update %s audit_data
set %s
from prior
%s
',
      table_name,
      where_clause,
      table_name,
      per_column('
  ${column} = 
    (case when 
      (audit_data.${column} <> prior.${column}) or
      (audit_data.${column} is null and prior.${column} is not null) or 
      (audit_data.${column} is not null and audit_data.${column} is null)
    then prior.${column} 
    else audit_data.${column} 
    end)', ', ', current_schema::varchar, tables.table_name, reserved_columns),
      where_clause
    );

    raise notice '%', from_sql;
    -- problem here is you can't enumerate record columns

    execute from_sql;
  end loop;
end;
$$;

