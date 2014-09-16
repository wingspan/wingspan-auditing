create or replace function create_history_view(
  main_table_name text
) returns void
language plpgsql AS $$
declare
  fks record;
  col record;

  sql text;
  column_list text;
  
  table_list text[];
begin
  table_list := array[main_table_name];
      
  sql := 'from ' || main_table_name;
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
left join %I on %I.%I = %I.%I', 
      fks.table_name, 
      fks.table_name, fks.column_name,
      fks.table_name, fks.foreign_column_name);
  end loop;

  column_list := '';

  raise notice '*%*', table_list[2];
  
  for col in 
    select * 
    from information_schema.columns
    where table_name = any(table_list)
      and table_schema = current_schema
    order by table_name, column_name
  loop    
    column_list := column_list ||
      col.table_name || '.' || col.column_name || ' as ' || 
      col.table_name || '_' || col.column_name || ', ';
  end loop;

  sql :=
    'select ' || 
    substring(
      column_list 
      from 0 
      for length(column_list) - 1) ||
    e'\n' || sql;

  raise notice '%', sql;
end;
$$;

with movies_history as (
  select
    m.id, m.title,
    tstzrange(
      coalesce(m.audit_date, '-infinity'), 
      coalesce(lead(m.audit_date) over w_m, 'infinity'),
      '[)'
    ) movie_effective 
  from movies$a m
  window w_m as (partition by m.id order by m.audit_date asc)
),
licenses_history as (
  select
    l.id, l.title, movie_id,
    tstzrange(
      coalesce(l.audit_date, '-infinity'), 
      coalesce(lead(l.audit_date) over w_l, 'infinity'),
      '[)'
    ) license_effective  
  from licenses$a l
  window w_l as (partition by l.id order by l.audit_date asc)

  union all 
  
  select
    l.id, l.title, movie_id,
    tstzrange(
      '-infinity',
      l.audit_date,
      '[)'
    ) license_effective  
  from licenses$a l
  where audit_action = 'I'
)
select m.*,
       l.*,
       m.movie_effective * l.license_effective
from movies_history m
left join licenses_history l
on l.movie_id = m.id 
and movie_effective && license_effective;

-- Range test
select
  id, title
from
  movies_vw
where 
  license_effective @> now()

-- Range view
  CREATE view movie_ranges AS 
  SELECT
    tsrange(
      s.audit_date, 
      coalesce(lead(s.audit_date) 
                 over(
                   partition by s.i_id 
                   order by s.audit_date), 
               'infinity'), 
               '[)'
    ) m_effective,
    m.audit_date
    movie.name,
  FROM movie$a s

-- Range view 2
CREATE view movie_ranges AS 
  SELECT
    tsrange(
      s.audit_date, 
      coalesce(lead(s.audit_date) 
                 over(
                   partition by s.i_id 
                   order by s.audit_date), 
               'infinity'), 
               '[)'
    ) m_effective,
    m.audit_date
    movie.name,
  FROM movie$a s

-- Range view 3
WITH s as (
  SELECT
    *
  FROM movie_vw s
  LEFT JOIN (
    license_vw 
  ) l ON l.name = m.licensee
),
all_joined as (
  SELECT
    -- anything not found in a left join gets turned into an infinite range
    coalesce(mis_effective, tsrange('-infinity', 'infinity', '[]')) mis_effective,
    coalesce(mir_effective, tsrange('-infinity', 'infinity', '[]')) mir_effective,
    greatest(s._audit_date_, r._audit_date_) _audit_date_,
    s.tmf_level
  FROM s
  LEFT JOIN r ON s.id = r.id
)
select *
from all_joined
where ...


-- Blame with timestamp
SELECT movie_user, license_user, test
FROM movies_vw
WHERE id = ...
AND date_range <@ '12345'




