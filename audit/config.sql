create or replace function cfg_get_id(current_schema text, table_name varchar)
returns varchar
language plpgsql AS $$
begin
  return 'id';
end;
$$;
