drop schema if exists movie_audit_demo cascade;
create schema movie_audit_demo;
set search_path to movie_audit_demo;

\i example/movies_schema.sql

\i install_auditing.sql

\i example/movies_data.sql

select create_history_view('movies');
