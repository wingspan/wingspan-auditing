drop schema tenant1 cascade
create schema tenant1;
set search_path to tenant1;

create table movies (
  id int primary key,
  title text
);

create table licenses (
  id int primary key,
  movie_id int references movies (id),
  title text, 
  start_date timestamp, 
  end_date timestamp
);


create or replace function get_request_id() returns varchar
language plpgsql AS $$
declare
  context varchar;
begin
  select application_name from pg_stat_activity where pid = pg_backend_pid() INTO context;
  return split_part(context, ',', 1);
end;
$$;

create or replace function get_user_id() returns varchar
language plpgsql AS $$
declare
  context varchar;
begin
  select application_name from pg_stat_activity where pid = pg_backend_pid() INTO context;
  return split_part(context, ',', 2);
end;
$$;

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

create sequence id_seq;

-- Queries to test creation of triggers
drop table tenant1.movies$a cascade;
drop table tenant1.licenses$a cascade;

select create_triggers();

set application_name to 'request_1,gary';

insert into movies(id, title) 
values (nextval(current_schema || '.id_seq'), 'Star Wars');

insert into licenses (id, movie_id, title, start_date, end_date)
values(nextval(current_schema || '.id_seq'), 1, 'Disney', '01-01-2000'::timestamp, '03-01-2000'::timestamp);

select * 
from movies m, licenses l
where m.id = l.movie_id;

set application_name to 'request_2,gary';

update movies 
set title = 'Star Wars - Phantom Menace' 
where title = 'Star Wars';

set application_name to 'request_3,greg';
delete from licenses;
delete from movies;

select * from tenant1.movies$a;
select * from tenant1.licenses$a;

select * 
from tenant1.movies$a m,
     tenant1.licenses$a l
where l.movie_id = m.id;


-- Create data...

-- Test queries

-- Blame query
select 
  *
from 
(
  select 
    audit_date,
    audit_user,
    audit_action,
    id,
    title,
    (case when ne(title, lead(title) over y) then audit_user else null end) title$u, 
    dense_rank() over y title$r,
    title$c
  from (
    select 
      id,
      title,
      ne(title, lead(title) over w) title$c,
      audit_date,
      audit_user,
      audit_action
    from movies$a
    where audit_action in ('I', 'U')
    WINDOW w AS ( PARTITION by id ORDER BY audit_date DESC)
  ) a
  WINDOW y AS ( PARTITION by id ORDER BY audit_date DESC, title$c DESC)
) b
WHERE title$r = 1
;

-- Rollback

-- triggers? foreign keys?

-- this finds the row that changed
-- updates are the interesting case 
-- for a delete or an insert, you're 
-- just replicating the row or deleting it
select * from movies$a 
where audit_request = 'request_2' 
and audit_action = 'U'

-- valuable to insert an integer that represents 
-- the revision number for an entity because it avoids this
select * from (
  select m.*, dense_rank() over (partition by id order by audit_date desc) audit_date$r
  from movies$a m
  where audit_date <= '2014-08-27 22:46:45.033'::timestamp
) a
where audit_date$r in (1, 2)
;

-- check columns that changed
select * from (
  select m.*, dense_rank() over (partition by id order by audit_date desc) audit_rank
  from movies$a m
  where audit_date <= '2014-08-27 22:46:45.033'::timestamp
) a
where audit_rank in (1, 2)

-- logic: see which columns changed, then build an update
-- realistically you need this procedure to print out
-- the query for you. trying to solve this for all possible
-- scenarios is impossible, but getting a starting point
-- in the right order is awesome and gets you most of the
-- way there
update movies
set title = '', something = ''
where id = 1 
from movies$a


drop function per_column(
  snippet varchar, search_table varchar, search_schema varchar
) 


create or replace function per_column(
  snippet varchar, delimeter varchar, search_schema varchar, search_table varchar, skip varchar[]
) returns varchar
language plpgsql AS $$
declare
  result varchar;
  cols record;
begin
  result := '';
  
  for cols in
    select * 
    from information_schema.columns c
    where c.table_name = search_table
      and c.table_schema = search_schema
      and not (c.column_name = any (skip))
    order by ordinal_position
  loop     
    result := result || 
      replace(snippet, '${column}', cols.column_name) || delimeter;          
  end loop;

  result := 
    substring(
      result 
      from 0 
      for length(result) - length(delimeter) + 1);
  
  return result;
end;
$$;


select per_column('${column}$c'::varchar, current_schema::varchar, 'movies'::varchar, array['title'::varchar]) 

-- query to find what to undo
 -- query to find what to undo
select (case when id <> id$p then 1 else 0 end) as id$c, (case when title <> title$p then 1 else 0 end) as title$c, (case when audit_date <> audit_date$p then 1 else 0 end) as audit_date$c, b.*
from (
  select a.*,
         lead(id) over w id$p, lead(title) over w title$p, lead(audit_date) over w audit_date$p
  from tenant1."movies$a" a
  window w as (partition by id order by audit_date desc)
) b
WHERE 1=1 AND audit_txid = 517093 AND audit_request = 'request_2'
order by audit_date desc

select undo(517093, 'request_2', null, null, null, array['movies$a', 'licenses$a']);
-- query to find what to undo
-- query to find what to undo


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

  where_clause := 'WHERE';
  if (audit_txid is not null) then
    where_clause := where_clause || ' AND audit_data.audit_txid = ' || audit_txid;
  end if;

  if (audit_request is not null) then
    where_clause := where_clause || ' AND audit_data.audit_request = ''' || format('%I', audit_request) || '''';
  end if;

  if (audit_action is not null) then
    where_clause := where_clause || ' AND audit_data.audit_action = ''' || format('%I', audit_action) || '''';
  end if;

  if (audit_user is not null) then
    where_clause := where_clause || ' AND audit_data.audit_user = ''' || format('%I', audit_user) || '''';
  end if;

  if (audit_interval is not null) then
    where_clause := where_clause || ' AND audit_data.audit_interval <% interval ''' || format('%I', audit_interval) || '''';
  end if;

  where_clause := regexp_replace(where_clause, 'WHERE AND', 'WHERE');

  -- todo sorting
  for tables in 
    select * 
    from information_schema.tables t
    where t.table_name not like '%$a' 
      and t.table_schema = current_schema
      and (audit_tables is null or t.table_name = any (audit_tables))

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
  loop  


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



