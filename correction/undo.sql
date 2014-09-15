create or replace function change(
  audit_txid bigint, audit_tables varchar[], idx int
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
  reserved_columns := get_audit_columns();

	-- what 'undo' means for insert isn't well defined, so don't let it happen
  where_clause := 'audit_data.audit_txid <= ' || audit_txid;

  -- todo sorting
  for tables in 
    select * 
    from information_schema.tables t
    where t.table_name not like '%$a' 
      and t.table_schema = current_schema
      and (audit_tables is null or t.table_name = any (audit_tables))
  loop  
    table_name = current_schema || '.' || format('%I', tables.table_name);

    -- find out which columns changed, and the prior values.
    -- sort these in order they happened so we can undo them in
    -- reverse
    from_sql := 
      format(
'-- undo query
with change as (
  select %s, %s
  from (
    select %s, %s 
    from (
      select *
      from %s$a audit_data
      where %s
      order by audit_txid desc
    ) a
  ) b
  group by %s
)
update %s audit_data
set %s
from change
where change.%s = audit_data.%s
',
      per_column('array_agg(${column}) ${column}', ', ', current_schema, tables.table_name, reserved_columns),
			cfg_get_id(current_schema, tables.table_name),
      per_column('${column}', ', ', current_schema, tables.table_name, reserved_columns),
			cfg_get_id(current_schema, tables.table_name),		
      table_name,
      where_clause,
      cfg_get_id(current_schema, tables.table_name),
      table_name,
      per_column('
  ${column} = 
    (case when 
      ne(change.${column}[1], change.${column}[2])
    then change.${column}[' || idx || ']
    else audit_data.${column} 
    end)', ', ', current_schema, tables.table_name, reserved_columns),
			cfg_get_id(current_schema, tables.table_name),
			cfg_get_id(current_schema, tables.table_name)
    );

    raise notice '%', from_sql;
    -- problem here is you can't enumerate record columns

    execute from_sql;
  end loop;
end;
$$;

create or replace function undo(
  audit_txid bigint, audit_tables varchar[]
) returns void
language plpgsql AS $$
begin
  execute change(audit_txid, audit_tables, 2);
end;
$$;

create or replace function redo(
  audit_txid bigint, audit_tables varchar[]
) returns void
language plpgsql AS $$
begin
  execute change(audit_txid, audit_tables, 1);
end;
$$;
