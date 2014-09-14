drop schema if exists movie_audit_demo;
create schema movie_audit_demo;
set search_path to movie_audit_demo;

\i movies_schema.sql

\i ../../install.sql

\i movies_data.sql
