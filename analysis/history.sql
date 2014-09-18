--
-- The function below creates a view that lets you query an entity's history
-- over time, by filtering to a specific date window.
--
--create view history as 
--with movies_history as (
--  select
--    m.id, m.title,
--    tstzrange(
--      coalesce(m.audit_date, '-infinity'), 
--      coalesce(lead(m.audit_date) over w_m, 'infinity'),
--      '[)'
--    ) movie_effective 
--  from movies$a m
--  window w_m as (partition by m.id order by m.audit_date asc)
--),
--licenses_history as (
--  select
--    l.id, l.title, movie_id,
--    tstzrange(
--      coalesce(l.audit_date, '-infinity'), 
--      coalesce(lead(l.audit_date) over w_l, 'infinity'),
--      '[)'
--    ) license_effective  
--  from licenses$a l
--  window w_l as (partition by l.id order by l.audit_date asc)
--
--  union all 
--  
--  select
--    l.id, l.title, movie_id,
--    tstzrange(
--      '-infinity',
--      l.audit_date,
--      '[)'
--    ) license_effective  
--  from licenses$a l
--  where audit_action = 'I'
--), 
--joined_history as (
--  select m.id movie_id, m.title movie_title,
--       l.id license_id, l.title license_title,
--       movie_effective,
--       coalesce(l.license_effective, '[-infinity,infinity]') license_effective
--  from movies_history m
--  left join licenses_history l
--  on l.movie_id = m.id 
--)
--select 
--  movie_id, movie_title, license_id, license_title,
--  movie_effective * license_effective effective
--from joined_history
--where movie_effective && license_effective;

create or replace function create_history_view(
  main_table_name text
) returns void
language plpgsql AS $$
declare
  fks record;
  col record;

  sql text;
  joins text;
  effective text;
  column_list text;
  
  table_list text[];
begin
  table_list := array[main_table_name];

  joins := '';
  effective := '';
   
  sql := format('
with %s_history as (
  select
    %s,
    tstzrange(
      coalesce(audit_date, ''-infinity''), 
      coalesce(lead(audit_date) over w, ''infinity''),
      ''[)''
    ) %s_effective 
  from %s$a
  window w as (partition by id order by audit_date asc)
),',
  main_table_name,
  per_column('${column}', ', ', current_schema, main_table_name, array['']),
  main_table_name,
  main_table_name,
  main_table_name
);

  for fks in 
    -- todo: recursive
    select 
      tc.constraint_name, tc.table_name, kcu.column_name, 
      ccu.table_name as foreign_table_name,
      ccu.column_name as foreign_column_name
    from information_schema.table_constraints as tc 
    join information_schema.key_column_usage as kcu
      on tc.constraint_name = kcu.constraint_name
    join information_schema.constraint_column_usage as ccu
      on ccu.constraint_name = tc.constraint_name
    where constraint_type = 'FOREIGN KEY' 
      and tc.table_schema = current_schema
      and ccu.constraint_schema = current_schema
      and tc.constraint_schema = current_schema
      and kcu.constraint_schema = current_schema
      and ccu.table_name = main_table_name
  loop
    -- TODO: logic for left / right / outer join
    table_list := array_append(table_list, fks.table_name::text);
     
    sql := sql || format('
%s_history as (
  select
    %s,
    tstzrange(
      coalesce(audit_date, ''-infinity''), 
      coalesce(lead(audit_date) over w, ''infinity''),
      ''[)''
    ) %s_effective 
  from %s$a
  window w as (partition by id order by audit_date asc)

  union all 
  
  select
    %s,
    tstzrange(
      ''-infinity'',
      audit_date,
      ''[)''
    ) %s_effective  
  from %s$a
  where audit_action = ''I''
),
',
  fks.table_name,
  per_column('${column}', ', ', current_schema, fks.table_name, array['']),
  fks.table_name,
  fks.table_name,
  per_column('${column}', ', ', current_schema, fks.table_name, array['']),
  fks.table_name,
  fks.table_name
);
    
    joins := joins || format('
left join %I 
  on %I.%I = %I.%I
  and %I.%I && %I.%I
', 
      fks.table_name || '_history', 
      fks.table_name || '_history', fks.column_name,
      main_table_name || '_history', fks.foreign_column_name,
      fks.table_name || '_history', fks.table_name || '_effective',
      main_table_name || '_history', main_table_name || '_effective'
);
    effective := effective ||
      format(' * coalesce(%I.%I, ''[-infinity, infinity]'')',
      fks.table_name || '_history', fks.table_name || '_effective');
  end loop;

  column_list := '';
  
  for col in 
    select * 
    from information_schema.columns
    where table_name = any(table_list)
      and table_schema = current_schema
    order by table_name, column_name
  loop    
    column_list := column_list ||
      col.table_name || '_history' || '.' || col.column_name || ' as ' || 
      col.table_name || '_history' || '_' || col.column_name || ', ';
  end loop;

  sql := 
    format('create or replace view %s_history_vw as ', main_table_name) ||
    substring(
      sql
      from 0
      for length(sql) - 1) ||
    format('
select %I.%I%s as effective_time,
',
  main_table_name || '_history',
  main_table_name || '_effective',
  effective 
) || 
    substring(
      column_list 
      from 0 
      for length(column_list) - 1) ||
    e'\nfrom ' || main_table_name || '_history' ||
    joins;

  raise notice '%', sql;

  execute sql;
end;
$$;

