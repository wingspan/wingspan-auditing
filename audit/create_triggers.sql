-- Create audit triggers
create or replace function create_triggers() returns void
language plpgsql AS $$
declare
  tables record;
  mode record;
  cols record;

  trigger_name text;
  proc_name text;
  table_name text;

  table_sql text;
  procedure_sql text;
  drop_trigger_sql text;
  trigger_sql text;

  type text;

  columns_list text;
  columns_type text;
  columns_insert text;
begin
  for tables in 
    select * 
    from information_schema.tables t
    where t.table_name not like '%$a' 
      and t.table_schema = current_schema
  loop
    columns_list := '';
    columns_type := '';
    columns_insert := '';  
  
    for cols in
      select * 
      from information_schema.columns c
      where c.table_name not like '%$a'
        and c.table_name = tables.table_name
        and c.table_schema = tables.table_schema
      order by ordinal_position
    loop     
      type := cols.data_type;   
      
      columns_list := columns_list || cols.column_name || ', ';
      columns_insert := columns_insert || '$1.' || cols.column_name || ', ';
      columns_type := columns_type || cols.column_name || ' ' || type || ', ';
    end loop;

    columns_list := 
      substring(
        columns_list 
        from 0 
        for length(columns_list) - 1);

    columns_insert := 
      substring(
        columns_insert 
        from 0 
        for length(columns_insert) - 1);
        
    columns_type := 
      substring(
        columns_type 
        from 0 
        for length(columns_type) - 1);

    table_name = current_schema || '.' || format('%I', tables.table_name);

    -- oddly this style of table creation does not allow 'if not exists'
    table_sql := 
      format(
       'create table %s$a
        as select t.*, 
             null::varchar(1) audit_action,
             null::varchar audit_request,
						 null::bigint audit_txid,
             null::varchar audit_user, 
             null::timestamp audit_date
           from %s t 
           where 0 = 1',
       table_name,
       table_name
    );
 
    raise notice '%', table_sql;
    execute table_sql;

    for mode in 
      select unnest(array['insert', 'update', 'delete']) op,
             unnest(array['new', 'new', 'old']) target,
             unnest(array['I', 'U', 'D']) "value"
    loop
      proc_name :=
         current_schema || '.' || format('%I', 'audit_' || mode.op || '_' || tables.table_name);

      procedure_sql := 
        format(
          e'create or replace function %s() returns trigger
            language plpgsql AS $fn$
            declare
              context text;
            begin
              execute 
                ''insert into %I$a 
                    (%s, audit_action, audit_request, audit_txid, audit_user, audit_date)
                  values
                    (%s, $2, $3, $4, $5, $6)''
                  using %s, ''%s'', get_request_id(), txid_current(), get_user_id(), now();

               return %I;
            end;
            $fn$;',
          proc_name,
          tables.table_name,
          columns_list,
          columns_insert,
          mode.target,
          mode.value,
          mode.target);

      trigger_name := format('%I', tables.table_name || '_' || mode.op || '$t');
      drop_trigger_sql := 'drop trigger if exists ' || trigger_name || ' on ' || table_name;
    
      trigger_sql :=
        format(
          'create trigger %s
             after %s on %s
           for each row execute procedure %s();',
          trigger_name,
          mode.op,
          table_name,
          proc_name
        );

      raise notice '%', procedure_sql;
      execute procedure_sql;

      raise notice '%', drop_trigger_sql;
      execute drop_trigger_sql;
        
      raise notice '%', trigger_sql;    
      execute trigger_sql;
    end loop;
  end loop;
end;
$$;

select create_triggers();
