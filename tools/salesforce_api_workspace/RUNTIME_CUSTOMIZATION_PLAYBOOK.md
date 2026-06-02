# Runtime Customization Playbook

This playbook documents how to customize this plugin at runtime without editing core code.

## 1) Override variables per deploy target

Bundle defaults live in `databricks.yml` under `variables`. Override them at deploy/run time.

Examples:

- Deploy with a different Salesforce UC connection:
  - `databricks bundle deploy --target dev --var "salesforce_conn_id=my_other_sf_conn" --profile dogfood`
- Run Hightouch sensor for a different sync id:
  - `databricks bundle run hightouch_sync_completion_sensor --target dev --var "hightouch_sync_id=1234567" --profile dogfood`

## 2) Override task parameters at run time

Use `--params` when you want one-off payload changes for demos/tests.

Examples:

- One-off object override:
  - `databricks bundle run salesforce_functions_crud --params object_name=Lead --profile dogfood`
- One-off wait mode:
  - `databricks bundle run salesforce_functions_crud --params wait_for_completion=true --profile dogfood`

## 3) Environment and dependency customization

Each job uses an environment block in `resources/*.yml`.

Current baseline:

- `environment_version: "5"`
- wheel dependency: `../dist/*.whl`

Common extensions:

- Add extra package dependency:
  - append `requests-oauthlib` or similar to `dependencies`.
- Pin plugin wheel version:
  - replace wildcard with explicit wheel path/version artifact.

## 4) Cluster/compute behavior

Python operator tasks run on the lightweight Python operator runtime VM.

Practical implication:

- You do not need to define a Spark cluster to run function/operator/sensor tasks.
- If Spark work is needed, invoke SQL/command APIs from within the task.

## 5) Extending this plugin with new tasks

Use this repository as a template:

1. Add implementation in `src/<domain>/functions.py`, `operators.py`, or `sensors.py`.
2. Export public entry points in `src/<domain>/__init__.py`.
3. Add a job/task in `resources/*.yml` with `python_operator_task.main` set to the exported symbol.
4. Rebuild and deploy:
   - `python3 setup.py bdist_wheel`
   - `databricks bundle deploy --profile dogfood`

## 6) Safe demo patterns

For repeatable demos:

- Prefer run-scoped IDs in payloads (for example `{{job.run_id}}`) to avoid duplicate-key failures.
- Keep `wait_for_completion=true` in function demo jobs so failures surface immediately.
- Use operator tasks for long-running external lifecycle management.
