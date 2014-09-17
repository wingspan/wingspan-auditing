wingspan-auditing
=================

Wingspan Auditing is a library of Postgres stored procedures for data analysis and correction tools, as well as a bolt-on auditing solution. These techniques came out of developments from consulting projects.

Package structure:
- Examples
  - Sample schema(s) / data setup to show how the package works
- Audit
  - Code to create the audit structure (minimum viable useful functionality)
- Analysis
  - Samples of functionality that can be executed with readonly privileges
- Correction
  - Features to do data corrections (undo / rollback / etc)

Setup:
- Set up a Postgres database (9.3+ supported, but some functionality may work on older versions)
- Ensure that psql is on your path
- To use the example schema, edit install_movies.sh to have correct connection information
- Run ./install_movies.sh
- OR run install_auditing.sql with psql against your own database

Common Configuration Changes:
* You may wish to exclude specific columns or column types from auditing. This can be controlled by changing audit/create_triggers.sql
* You may wish to change which information is used as 'context' data (username, request ID, etc) - this can be controlled by changing audit/context.sql

References
- <a href="https://rawgithub.com/garysieling/postgres-immutable-data/master/index.html">Talk about this library</a>
- <a href="https://github.com/2ndQuadrant/audit-trigger">2ndQuadrant Audit Triggers</a>
- <a href="https://wiki.postgresql.org/wiki/Audit_trigger">Postgres Wiki (Audit triggers)</a>
- <a href="https://wiki.postgresql.org/wiki/Audit_trigger_91plus">Postgres Wiki (Audit triggers - 9.1+)</a>
- <a href="http://docs.datomic.com/indexes.html">Datomic (Indexes)</a>
- <a href="http://en.wikipedia.org/wiki/Allen's_interval_algebra">Allen's Interval Algebra</a>

