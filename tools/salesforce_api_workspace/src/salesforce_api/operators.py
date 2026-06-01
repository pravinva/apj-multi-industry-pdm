import datetime
import json
from typing import Any, Dict, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from python_operator_task import OperatorV0, SensorResult


def _coerce_records(records: Any) -> List[Dict[str, Any]]:
    if isinstance(records, str):
        records = json.loads(records)
    if not isinstance(records, list):
        raise ValueError("records must be a list of objects")
    for idx, item in enumerate(records):
        if not isinstance(item, dict):
            raise ValueError(f"records[{idx}] must be an object, got {type(item).__name__}")
    return records


def _records_to_csv(records: List[Dict[str, Any]]) -> str:
    import csv
    import io

    fieldnames = sorted({k for record in records for k in record.keys()})
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=fieldnames,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(records)
    return out.getvalue()


class SalesforceBulkWriteOperator(OperatorV0):
    SUPPORTED_OPERATIONS = {"insert", "update", "upsert", "delete", "hardDelete"}
    FAILURE_STATES = {"Failed", "Aborted"}

    def __init__(
        self,
        object_name: str,
        operation: str,
        records: list,
        conn_id: str,
        task_key: str,
        external_id_field: str = "",
        api_version: str = "v62.0",
        poll_interval_minutes: int = 1,
    ):
        self.object_name = object_name
        self.operation = operation
        self.records = _coerce_records(records)
        self.conn_id = conn_id
        self.task_key = task_key
        self.external_id_field = external_id_field
        self.api_version = api_version
        self.poll_interval_minutes = int(poll_interval_minutes)
        self.w = WorkspaceClient()
        if self.operation not in self.SUPPORTED_OPERATIONS:
            supported = ", ".join(sorted(self.SUPPORTED_OPERATIONS))
            raise ValueError(f"Unsupported operation '{self.operation}'. Supported: {supported}")
        if self.operation == "upsert" and not self.external_id_field:
            raise ValueError("external_id_field is required for upsert operation")
        if not self.records:
            raise ValueError("records cannot be empty")

    def _request_salesforce(
        self,
        method: str,
        resource_path: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        proxy_base_url = f"{self.w.config.host}/api/2.0/unity-catalog/connections/{self.conn_id}/proxy"
        candidate_paths = [f"/services/data/{self.api_version}{resource_path}", resource_path]
        merged_headers: Dict[str, str] = {
            **self.w.config.authenticate(),
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        if headers:
            merged_headers.update(headers)

        last_response: Optional[requests.Response] = None
        for idx, candidate_path in enumerate(candidate_paths):
            response = requests.request(
                method,
                f"{proxy_base_url}{candidate_path}",
                headers=merged_headers,
                **kwargs,
            )
            last_response = response
            # Retry once with raw resource path when the connection already has base_path.
            if not (idx == 0 and response.status_code == 404):
                return response
        return last_response  # type: ignore[return-value]

    def open(self):
        payload: Dict[str, Any] = {
            "object": self.object_name,
            "operation": self.operation,
            "lineEnding": "LF",
        }
        if self.external_id_field:
            payload["externalIdFieldName"] = self.external_id_field

        create_response = self._request_salesforce(
            "POST",
            "/jobs/ingest",
            headers={"Content-Type": "application/json"},
            json=payload,
        )
        if create_response.status_code != 200:
            raise Exception(f"Failed to create job: {create_response.status_code} {create_response.text}")

        job_id = create_response.json()["id"]
        dbutils.jobs.taskValues.set("salesforce_job_id", job_id)

        upload_response = self._request_salesforce(
            "PUT",
            f"/jobs/ingest/{job_id}/batches",
            headers={"Content-Type": "text/csv"},
            data=_records_to_csv(self.records),
        )
        if upload_response.status_code not in (200, 201):
            raise Exception(f"Failed to upload data: {upload_response.status_code} {upload_response.text}")

        close_response = self._request_salesforce(
            "PATCH",
            f"/jobs/ingest/{job_id}",
            headers={"Content-Type": "application/json"},
            json={"state": "UploadComplete"},
        )
        if close_response.status_code != 200:
            raise Exception(f"Failed to close job: {close_response.status_code} {close_response.text}")

    def poll(self) -> SensorResult:
        job_id = dbutils.jobs.taskValues.get(self.task_key, "salesforce_job_id", default=None)
        if not job_id:
            raise ValueError(
                "Could not load salesforce_job_id from task values; ensure open() completed and task_key is correct."
            )

        status_response = self._request_salesforce("GET", f"/jobs/ingest/{job_id}")
        if status_response.status_code != 200:
            raise Exception(f"Failed to get job status: {status_response.status_code} {status_response.text}")

        data = status_response.json()
        state = data.get("state", "Unknown")
        if state == "JobComplete":
            processed = int(data.get("numberRecordsProcessed", 0) or 0)
            failed = int(data.get("numberRecordsFailed", 0) or 0)
            dbutils.jobs.taskValues.set("records_processed", str(processed))
            dbutils.jobs.taskValues.set("records_failed", str(failed))
            if failed > 0:
                raise Exception(
                    f"Salesforce job completed with failed records: processed={processed}, failed={failed}"
                )
            return SensorResult.completed()
        if state in self.FAILURE_STATES:
            raise Exception(f"Salesforce job {state.lower()}: {data.get('errorMessage', 'Unknown error')}")

        return SensorResult.deferred(duration=datetime.timedelta(minutes=self.poll_interval_minutes))

    def close(self):
        # OperatorV0 close() may not execute on cancellation yet.
        pass


class SalesforceUpsertOperator(SalesforceBulkWriteOperator):
    def __init__(
        self,
        object_name: str,
        external_id_field: str,
        records: list,
        conn_id: str,
        task_key: str,
        api_version: str = "v62.0",
        poll_interval_minutes: int = 1,
    ):
        super().__init__(
            object_name=object_name,
            operation="upsert",
            records=records,
            conn_id=conn_id,
            task_key=task_key,
            external_id_field=external_id_field,
            api_version=api_version,
            poll_interval_minutes=poll_interval_minutes,
        )
