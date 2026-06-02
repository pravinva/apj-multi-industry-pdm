# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk==0.110.0
# MAGIC %restart_python

# COMMAND ----------

import subprocess
import time
import os
from typing import Dict, List

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _run_cmd(command: List[str], cwd: str, allow_fail: bool = False) -> bool:
    print(f"$ {' '.join(command)}")
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        if allow_fail:
            print(f"WARNING: command failed ({result.returncode}) and was skipped.")
            return False
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command)}")
    return True


def _find_job_id(name_suffix: str) -> int:
    for job in w.jobs.list():
        if job.settings and job.settings.name and job.settings.name.endswith(name_suffix):
            return int(job.job_id)
    raise ValueError(f"Could not find deployed job ending with: {name_suffix}")


def _wait_for_run(run_id: int, poll_seconds: int = 5, timeout_seconds: int = 1800) -> Dict[str, str]:
    start = time.time()
    while True:
        run = w.jobs.get_run(run_id=run_id)
        state = run.state
        life_cycle = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
        result_state = state.result_state.value if state and state.result_state else "NONE"
        if life_cycle in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
            return {
                "life_cycle_state": life_cycle,
                "result_state": result_state,
                "run_page_url": run.run_page_url,
            }
        if time.time() - start > timeout_seconds:
            w.jobs.cancel_run(run_id=run_id)
            raise TimeoutError(
                f"Run {run_id} exceeded {timeout_seconds}s and was canceled. "
                f"URL: {run.run_page_url}"
            )
        time.sleep(poll_seconds)


# COMMAND ----------

# Build wheel + deploy only when notebook has full bundle files available.
repo_root = "."
has_bundle_files = os.path.exists(os.path.join(repo_root, "databricks.yml")) and os.path.exists(
    os.path.join(repo_root, "setup.py")
)
if has_bundle_files:
    built = _run_cmd(["python3", "setup.py", "bdist_wheel"], cwd=repo_root, allow_fail=True)
    deployed = _run_cmd(["databricks", "bundle", "deploy", "--target", "dev"], cwd=repo_root, allow_fail=True)
    if not (built and deployed):
        print("Proceeding with existing deployed jobs.")
else:
    print("Bundle files not available in task working directory; skipping build/deploy.")

# COMMAND ----------

# Run the same jobs we validated from CLI
job_suffixes = [
    "[hackathon] Salesforce Functions CRUD",
    "[hackathon] Salesforce Insert Operator",
    "[hackathon] Salesforce Upsert Operator",
    "[hackathon] Salesforce Update Operator",
    "[hackathon] Airflow Migration - Salesforce Demo",
]

results = []
for suffix in job_suffixes:
    job_id = _find_job_id(suffix)
    run_now_response = w.jobs.run_now(job_id=job_id)
    run_id = int(run_now_response.response.run_id)
    final = _wait_for_run(run_id)
    print(f"{suffix} -> {final['life_cycle_state']} / {final['result_state']}")
    print(f"Run URL: {final['run_page_url']}")
    results.append({"job": suffix, **final})
    if final["result_state"] != "SUCCESS":
        raise RuntimeError(
            f"Fail-fast: stopping notebook because '{suffix}' ended with "
            f"{final['life_cycle_state']} / {final['result_state']}."
        )

# COMMAND ----------

# Compact summary table for demo screenshots
for row in results:
    print(
        f"{row['job']}: {row['life_cycle_state']} / {row['result_state']}\n"
        f"  {row['run_page_url']}"
    )

# Explicitly end notebook task so Jobs marks completion immediately.
dbutils.notebook.exit("SUCCESS: all selected jobs completed")
