import datetime
from typing import Any, Dict, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from python_operator_task import Sensor, SensorResult


class HightouchSyncCompletionSensor(Sensor):
    """
    Poll a Hightouch sync run until terminal completion.
    """

    SUCCESS_STATES = {"healthy", "success", "succeeded", "completed", "complete"}
    FAILURE_STATES = {"failed", "aborted", "error", "cancelled", "canceled", "incomplete"}

    def __init__(
        self,
        conn_id: str,
        sync_id: str,
        task_key: str,
        run_id: str = "",
        poll_interval_minutes: int = 1,
        api_version: str = "v1",
    ):
        self.conn_id = conn_id
        self.sync_id = str(sync_id)
        self.task_key = task_key
        self.run_id = str(run_id) if run_id else ""
        self.poll_interval_minutes = int(poll_interval_minutes)
        self.api_version = api_version
        self.w = WorkspaceClient()

    def _request_hightouch(self, method: str, resource_path: str, **kwargs: Any) -> requests.Response:
        proxy_base_url = f"{self.w.config.host}/api/2.0/unity-catalog/connections/{self.conn_id}/proxy"
        candidate_paths = [f"/api/{self.api_version}{resource_path}", resource_path]
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
            if not (idx == 0 and response.status_code == 404):
                return response
        return last_response  # type: ignore[return-value]

    @staticmethod
    def _as_run_list(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return [r for r in data if isinstance(r, dict)]
            if isinstance(data, dict):
                return [data]
        return []

    @staticmethod
    def _run_status(run: Dict[str, Any]) -> str:
        return str(
            run.get("status")
            or run.get("state")
            or run.get("syncStatus")
            or "unknown"
        ).strip().lower()

    def _discover_latest_run_id(self) -> str:
        response = self._request_hightouch("GET", f"/syncs/{self.sync_id}/runs")
        if response.status_code != 200:
            raise Exception(
                f"Failed to list Hightouch sync runs: {response.status_code} {response.text}"
            )
        runs = self._as_run_list(response.json())
        if not runs:
            return ""
        return str(runs[0].get("id", "")).strip()

    def _get_target_run_id(self) -> str:
        if self.run_id:
            return self.run_id
        existing = dbutils.jobs.taskValues.get(self.task_key, "hightouch_run_id", default="")
        if existing:
            return str(existing)
        discovered = self._discover_latest_run_id()
        if discovered:
            dbutils.jobs.taskValues.set("hightouch_run_id", discovered)
        return discovered

    def poll(self) -> SensorResult:
        run_id = self._get_target_run_id()
        if not run_id:
            return SensorResult.deferred(duration=datetime.timedelta(minutes=self.poll_interval_minutes))

        response = self._request_hightouch("GET", f"/syncs/{self.sync_id}/runs")
        if response.status_code != 200:
            raise Exception(f"Failed to get Hightouch sync run status: {response.status_code} {response.text}")

        runs = self._as_run_list(response.json())
        if not runs:
            raise Exception(f"No runs returned for Hightouch sync {self.sync_id}")

        run = next((r for r in runs if str(r.get("id", "")) == run_id), None)
        if not run:
            raise Exception(f"Hightouch run {run_id} not found for sync {self.sync_id}")
        status = self._run_status(run)
        print(f"[HightouchSyncCompletionSensor] sync_id={self.sync_id} run_id={run_id} status={status}")

        if status in self.SUCCESS_STATES:
            dbutils.jobs.taskValues.set("hightouch_run_status", status)
            return SensorResult.completed()

        if status in self.FAILURE_STATES:
            error_text = run.get("error") or run.get("message") or "Unknown error"
            raise Exception(f"Hightouch sync run failed: status={status}, error={error_text}")

        return SensorResult.deferred(duration=datetime.timedelta(minutes=self.poll_interval_minutes))
