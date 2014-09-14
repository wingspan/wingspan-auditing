-- The following example queries show some things you ca do with audit data
set search_path to movie_audit_demo;

-- Retrieve audit history for each entity
select * from movies$a;
select * from licenses$a;

-- Find out who changed something last
select * from blame('movies', 'title');
select * from blame('licenses', 'title');
