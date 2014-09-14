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

