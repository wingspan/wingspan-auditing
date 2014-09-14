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
