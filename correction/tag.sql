create table tags (tag text unique, txid bigint);

create or replace function create_tag(tag_name text) returns void
language plpgsql AS $fn$
begin
  execute 
    'insert into tags (tag, txid) values ($1, $2)'
    using tag_name, txid_current();
end;
$fn$;

-- provide the tag name you want and the schema to copy into
create or replace function checkout_tag(tag_name text, from_schema text, to_schema text) returns void
language plpgsql AS $fn$
declare
  table_info record;
  audit_txid bigint;
  short_table text;
begin
  execute clone_schema(from_schema, to_schema);

  -- disable all the constraints in the other schema
  -- disable all the triggers in the other schema 

  execute 'select txid from ' || from_schema || '.tags where tag = $1'
  using tag_name
  into audit_txid;

  if audit_txid is null then
    raise notice 'Tag not found.';
    return;
  end if;
  
  -- foreach table
  for table_info in 
    select * 
    from information_schema.tables
    where table_name like '%$a' 
      and table_schema = from_schema
  loop
    short_table := 
      substring(
        table_info.table_name 
        from 0 
        for length(table_info.table_name) - 1);

    -- pull from table where txid <= requested
    execute format(
      '-- copy table data from audit trail 
      insert into %I.%I
      with historical_data as (
        select
          id,
          array_agg(audit_action order by audit_txid desc) audit_action,
          %s
        from %I.%I
        where audit_txid <= %s
        group by id
      )
      select id, %s
      from historical_data
      where audit_action[1] <> ''D''
      ',
      to_schema,
      short_table,
      per_column('array_agg(${column} order by audit_txid desc) ${column}', ', ', from_schema, short_table, array['id']),
      from_schema,
      table_info.table_name,
      audit_txid,
      per_column('${column}[1] ${column}', ', ', from_schema, short_table, array['id'])
    );

    -- filter to most recent row per id
    -- do the insert into the new schema
  end loop;
  -- re-enable all the constraints in the other schema
  -- re-enable all the triggers in the other schema
end;
$fn$;


