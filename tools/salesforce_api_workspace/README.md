# Salesforce Reverse ETL Task Orchestrator

## Why this orchestrator is needed

Salesforce reverse ETL delivery is often implemented as custom Python scripts plus scheduler glue. That approach works initially, but it creates long-term operational cost:

- repeated OAuth, retry, and polling logic across pipelines
- inconsistent error handling and observability across teams
- fragile handoff between one-off scripts and production orchestration
- high migration effort from legacy Airflow operator patterns

This repository packages a Databricks-native orchestration model for Salesforce Bulk API write workloads, with reusable primitives for function-style tasks, lifecycle operators, and migration-ready job definitions.

## Solution overview

The solution is delivered as a Databricks Asset Bundle (DAB) with Python Operator tasks.

- **Functions** provide explicit one-shot CRUD task entry points.
- **Operators** provide open/poll lifecycle management for external job orchestration.
- **Migration artifacts** map a concrete Airflow pattern into Databricks-native tasks.
- **Connection model** uses Unity Catalog HTTP connections for secure Salesforce API proxying.

## Repository structure

- `src/salesforce_api/functions.py`: Independent Salesforce function entry points.
- `src/salesforce_api/operators.py`: Salesforce operator classes for lifecycle orchestration.
- `resources/salesforce_bulk.yml`: Function and operator job definitions.
- `resources/salesforce_airflow_migration.yml`: Migrated workflow definition.
- `airflow_dags/salesforce_bulk_migration_source_dag.py`: Source Airflow DAG used for migration mapping.
- `databricks.yml`: Bundle target, variable, artifact, and deployment configuration.

## Functions model

The following functions are independently invokable via `python_operator_task.main`:

- `salesforce_api.salesforce_insert`
- `salesforce_api.salesforce_upsert`
- `salesforce_api.salesforce_update`

Each function accepts typed parameters, executes a Salesforce Bulk API operation through UC proxy, and can run in synchronous mode with `wait_for_completion=true`.

The bundled multi-step CRUD job (`salesforce_functions_crud`) chains these independent functions in sequence, but each function can also be scheduled or called as a standalone task in a separate workflow.

## Operators model

For long-running or lifecycle-sensitive scenarios, use operator classes:

- `salesforce_api.SalesforceBulkWriteOperator`
- `salesforce_api.SalesforceUpsertOperator`

Operator behavior:

- `open()`: Creates and starts the external Salesforce bulk job.
- `poll()`: Checks terminal state and defers between polls.
- `close()`: Reserved lifecycle hook.

This model is designed for orchestration flows where explicit external state management is preferred over one-shot function execution.

## Airflow migration model

This repository includes a concrete migration path from a real Airflow DAG to Databricks Python Operator tasks.

- Source pattern: `airflow_dags/salesforce_bulk_migration_source_dag.py`
- Target pattern: `resources/salesforce_airflow_migration.yml`
- Full mapping guidance: `AIRFLOW_MIGRATION_GUIDE.md`

Migration strategy:

- keep simple write steps as functions
- use operators where lifecycle management and polling are required
- replace Airflow XCom-style handoff with Databricks task values only where needed

## Deployment and execution

Build and deploy:

- `python3 setup.py bdist_wheel`
- `databricks bundle deploy --profile dogfood`

Run key workflows:

- `databricks bundle run salesforce_functions_crud --profile dogfood`
- `databricks bundle run salesforce_insert_operator --profile dogfood`
- `databricks bundle run salesforce_upsert_operator --profile dogfood`
- `databricks bundle run salesforce_update_operator --profile dogfood`
- `databricks bundle run salesforce_airflow_migration_demo --profile dogfood`

## Runtime customization and extensibility

The bundle is designed for runtime override and reuse through variables and task parameters.

- Connection/object defaults are managed in `databricks.yml`.
- Jobs can be run with target-specific overrides.
- Additional functions/operators can be added without changing orchestration patterns.

For implementation details and override examples, see:

- `RUNTIME_CUSTOMIZATION_PLAYBOOK.md`

## Additional references

- [Declarative Automation Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [Declarative Automation Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
