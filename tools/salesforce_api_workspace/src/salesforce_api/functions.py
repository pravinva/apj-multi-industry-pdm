import csv
import io
import json
from typing import Any, Dict, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils


def _coerce_records(records: Any) -> List[Dict[str, Any]]:
    if isinstance(records, str):
        records = json.loads(records)
    if not isinstance(records, list):
        raise ValueError("records must be a list of objects")
    for idx, item in enumerate(records):
        if not isinstance(item, dict):
            raise ValueError(f"records[{idx}] must be an object, got {type(item).__name__}")
    return records


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return bool(value)


def _request_salesforce(
    w: WorkspaceClient,
    conn_id: str,
    api_version: str,
    method: str,
    resource_path: str,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> requests.Response:
    proxy_base_url = f"{w.config.host}/api/2.0/unity-catalog/connections/{conn_id}/proxy"
    candidate_paths = [f"/services/data/{api_version}{resource_path}", resource_path]
    merged_headers: Dict[str, str] = {
        **w.config.authenticate(),
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
        print("[salesforce_api] path fallback: retrying with connection base_path")

    return last_response  # type: ignore[return-value]


def _records_to_csv(records: List[Dict[str, Any]]) -> str:
    if not records:
        raise ValueError("Cannot create CSV from empty records list")
    fieldnames = sorted({k for record in records for k in record.keys()})
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=fieldnames,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(records)
    return output.getvalue()


def _salesforce_bulk_operation(
    object_name: str,
    operation: str,
    records: Any,
    conn_id: str,
    api_version: str = "v62.0",
    external_id_field: Optional[str] = None,
    wait_for_completion: Any = False,
) -> Dict[str, Any]:
    w = WorkspaceClient()
    records = _coerce_records(records)
    wait_for_completion = _coerce_bool(wait_for_completion)
    csv_data = _records_to_csv(records)

    create_payload: Dict[str, Any] = {
        "object": object_name,
        "operation": operation,
        "lineEnding": "LF",
    }
    if external_id_field:
        create_payload["externalIdFieldName"] = external_id_field

    create_response = _request_salesforce(
        w,
        conn_id,
        api_version,
        "POST",
        "/jobs/ingest",
        headers={"Content-Type": "application/json"},
        json=create_payload,
    )
    if create_response.status_code != 200:
        raise Exception(f"Failed to create job: {create_response.status_code} {create_response.text}")

    job_id = create_response.json()["id"]
    dbutils.jobs.taskValues.set("salesforce_job_id", job_id)

    upload_response = _request_salesforce(
        w,
        conn_id,
        api_version,
        "PUT",
        f"/jobs/ingest/{job_id}/batches",
        headers={"Content-Type": "text/csv"},
        data=csv_data,
    )
    if upload_response.status_code not in (200, 201):
        raise Exception(f"Failed to upload data: {upload_response.status_code} {upload_response.text}")

    close_response = _request_salesforce(
        w,
        conn_id,
        api_version,
        "PATCH",
        f"/jobs/ingest/{job_id}",
        headers={"Content-Type": "application/json"},
        json={"state": "UploadComplete"},
    )
    if close_response.status_code != 200:
        raise Exception(f"Failed to close job: {close_response.status_code} {close_response.text}")

    result: Dict[str, Any] = {
        "job_id": job_id,
        "object": object_name,
        "operation": operation,
        "records_total": len(records),
        "state": close_response.json().get("state", "Unknown"),
    }

    if wait_for_completion:
        while True:
            status_response = _request_salesforce(
                w,
                conn_id,
                api_version,
                "GET",
                f"/jobs/ingest/{job_id}",
            )
            status_response.raise_for_status()
            status = status_response.json()
            state = status.get("state", "Unknown")
            if state == "JobComplete":
                processed = int(status.get("numberRecordsProcessed", 0) or 0)
                failed = int(status.get("numberRecordsFailed", 0) or 0)
                result["records_processed"] = processed
                result["records_failed"] = failed
                result["state"] = state
                if failed > 0:
                    raise Exception(
                        f"Salesforce job completed with failed records: processed={processed}, failed={failed}"
                    )
                break
            if state in {"Failed", "Aborted"}:
                raise Exception(f"Salesforce job {state.lower()}: {status.get('errorMessage', 'Unknown error')}")
            import time

            time.sleep(5)

    return result


def salesforce_upsert(
    object_name: str,
    external_id_field: str,
    records: list,
    conn_id: str,
    api_version: str = "v62.0",
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    return _salesforce_bulk_operation(
        object_name=object_name,
        operation="upsert",
        records=records,
        conn_id=conn_id,
        api_version=api_version,
        external_id_field=external_id_field,
        wait_for_completion=wait_for_completion,
    )


def salesforce_insert(
    object_name: str,
    records: list,
    conn_id: str,
    api_version: str = "v62.0",
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    return _salesforce_bulk_operation(
        object_name=object_name,
        operation="insert",
        records=records,
        conn_id=conn_id,
        api_version=api_version,
        wait_for_completion=wait_for_completion,
    )


def salesforce_update(
    object_name: str,
    records: list,
    conn_id: str,
    api_version: str = "v62.0",
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    return _salesforce_bulk_operation(
        object_name=object_name,
        operation="update",
        records=records,
        conn_id=conn_id,
        api_version=api_version,
        wait_for_completion=wait_for_completion,
    )
