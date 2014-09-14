postgres-auditing
=================

Data analysis and correction tools for auditing a Postgres database

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
- Choose the example schema and run the install.sql file with psql
- OR run install.sql with psql against your own database

Common Configuration Changes:
* You may wish to exclude specific columns or column types from auditing. This can be controlled by changing audit/create_triggers.sql
* You may wish to change which information is used as 'context' data (username, request ID, etc) - this can be controlled by changing audit/context.sql
