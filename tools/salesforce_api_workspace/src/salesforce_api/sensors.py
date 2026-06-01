import datetime
from typing import Any, Dict, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from python_operator_task import Sensor, SensorResult


class SalesforceBulkJobSensor(Sensor):
    SUCCESS_STATES = {"JobComplete"}
    FAILURE_STATES = {"Failed", "Aborted"}
    TERMINAL_STATES = SUCCESS_STATES | FAILURE_STATES

    def __init__(
        self,
        conn_id: str,
        task_key: str,
        job_id: str = "",
        poll_interval_minutes: int = 1,
        api_version: str = "v62.0",
    ):
        self.conn_id = conn_id
        self.task_key = task_key
        self.job_id = job_id
        self.poll_interval_minutes = int(poll_interval_minutes)
        self.api_version = api_version
        self.w = WorkspaceClient()

    def _request_salesforce(self, method: str, resource_path: str, **kwargs: Any) -> requests.Response:
        proxy_base_url = f"{self.w.config.host}/api/2.0/unity-catalog/connections/{self.conn_id}/proxy"
        candidate_paths = [f"/services/data/{self.api_version}{resource_path}", resource_path]
        headers = {
            **self.w.config.authenticate(),
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        last_response: Optional[requests.Response] = None
        for idx, candidate_path in enumerate(candidate_paths):
            response = requests.request(
                method,
                f"{proxy_base_url}{candidate_path}",
                headers=headers,
                **kwargs,
            )
            last_response = response
            # Retry once with raw resource path when the connection already has base_path.
            if not (idx == 0 and response.status_code == 404):
                return response
        return last_response  # type: ignore[return-value]

    def _get_job_id(self) -> str:
        if self.job_id:
            return self.job_id
        job_id = dbutils.jobs.taskValues.get(self.task_key, "salesforce_job_id", default=None)
        if not job_id:
            raise ValueError(
                f"No job_id provided and none found in task values for '{self.task_key}'."
            )
        return job_id

    def poll(self) -> SensorResult:
        job_id = self._get_job_id()
        response = self._request_salesforce("GET", f"/jobs/ingest/{job_id}")
        if response.status_code != 200:
            raise Exception(f"Failed to get job status: {response.status_code} {response.text}")

        job_data: Dict[str, Any] = response.json()
        state = job_data.get("state", "Unknown")
        processed = job_data.get("numberRecordsProcessed", 0)
        failed = job_data.get("numberRecordsFailed", 0)
        print(f"[SalesforceBulkJobSensor] state={state} processed={processed} failed={failed}")

        if state in self.SUCCESS_STATES:
            dbutils.jobs.taskValues.set("records_processed", str(processed))
            dbutils.jobs.taskValues.set("records_failed", str(failed))
            if failed > 0:
                raise Exception(
                    f"Salesforce job completed with failed records: processed={processed}, failed={failed}"
                )
            return SensorResult.completed()

        if state in self.FAILURE_STATES:
            raise Exception(f"Salesforce job {state.lower()}: {job_data.get('errorMessage', 'Unknown error')}")

        return SensorResult.deferred(duration=datetime.timedelta(minutes=self.poll_interval_minutes))
