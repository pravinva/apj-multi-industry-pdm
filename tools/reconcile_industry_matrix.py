#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = ROOT / "industries" / "deployment_matrix.yaml"

JOB_NAME_PATTERN = re.compile(
    r"ot-pdm-(training|scoring|zerobus-connector|financial-backfill)-([a-z0-9_-]+)$"
)
PIPELINE_NAME_PATTERN = re.compile(r"ot-pdm-dlt-([a-z0-9_-]+)$")


@dataclass
class MatrixConfig:
    target: str
    profile: str
    industries: list[str]
    required_jobs: list[str]
    required_pipeline: str


def _strip_env_prefix(name: str) -> str:
    return re.sub(r"^\[[^\]]+\]\s*", "", name.strip().lower())


def load_config(path: Path) -> MatrixConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    bundle = raw.get("bundle", {}) or {}
    required = raw.get("required", {}) or {}
    jobs = (required.get("jobs", []) or [])
    pipeline = str(required.get("pipeline", "dlt")).strip()

    industries = [str(v).strip() for v in (raw.get("industries", []) or []) if str(v).strip()]
    if not industries:
        raise ValueError("No industries found in matrix config")

    return MatrixConfig(
        target=str(bundle.get("target", "dev")).strip(),
        profile=str(bundle.get("profile", "DEFAULT")).strip(),
        industries=industries,
        required_jobs=[str(j).strip() for j in jobs if str(j).strip()],
        required_pipeline=pipeline,
    )


def resolve_owner_email(client: WorkspaceClient, explicit_owner: str | None) -> str:
    if explicit_owner:
        return explicit_owner.strip().lower()
    me = client.current_user.me()
    return str(getattr(me, "user_name", "") or "").strip().lower()


def fetch_job_matrix(
    client: WorkspaceClient,
    owner: str,
) -> dict[str, dict[str, int]]:
    jobs: dict[str, dict[str, int]] = defaultdict(dict)
    page = None
    for _ in range(100):
        query = {"limit": "100"}
        if page:
            query["page_token"] = page
        resp = client.api_client.do("GET", "/api/2.1/jobs/list", query=query)
        for job in (resp.get("jobs") or []):
            creator = str(job.get("creator_user_name") or "").strip().lower()
            if creator != owner:
                continue
            name = _strip_env_prefix((job.get("settings") or {}).get("name") or "")
            match = JOB_NAME_PATTERN.search(name)
            if not match:
                continue
            job_type, industry = match.group(1), match.group(2)
            jobs[job_type][industry] = int(job.get("job_id"))
        page = resp.get("next_page_token")
        if not page:
            break
    return jobs


def fetch_pipeline_matrix(client: WorkspaceClient) -> dict[str, str]:
    pipelines: dict[str, str] = {}
    page = None
    for _ in range(100):
        query = {"max_results": "100"}
        if page:
            query["page_token"] = page
        resp = client.api_client.do("GET", "/api/2.0/pipelines", query=query)
        for item in (resp.get("statuses") or []):
            name = _strip_env_prefix(item.get("name") or "")
            match = PIPELINE_NAME_PATTERN.search(name)
            if not match:
                continue
            pipelines[match.group(1)] = str(item.get("pipeline_id"))
        page = resp.get("next_page_token")
        if not page:
            break
    return pipelines


def summarize_gaps(cfg: MatrixConfig, jobs: dict[str, dict[str, int]], pipes: dict[str, str]) -> list[tuple[str, str]]:
    missing: list[tuple[str, str]] = []
    for industry in cfg.industries:
        for job_type in cfg.required_jobs:
            if industry not in jobs.get(job_type, {}):
                missing.append((job_type, industry))
        if cfg.required_pipeline == "dlt" and industry not in pipes:
            missing.append(("dlt", industry))
    return missing


def _replace_tokens(value: str, src_industry: str, dst_industry: str) -> str:
    updated = value
    updated = updated.replace(f"-{src_industry}", f"-{dst_industry}")
    updated = updated.replace(f"_{src_industry}", f"_{dst_industry}")
    updated = updated.replace(f"/{src_industry}", f"/{dst_industry}")
    updated = updated.replace(f"pdm_{src_industry}", f"pdm_{dst_industry}")
    return updated


def _deep_replace_strings(obj, src_industry: str, dst_industry: str):
    if isinstance(obj, dict):
        return {k: _deep_replace_strings(v, src_industry, dst_industry) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_replace_strings(v, src_industry, dst_industry) for v in obj]
    if isinstance(obj, str):
        return _replace_tokens(obj, src_industry, dst_industry)
    return obj


def create_job_from_template(
    client: WorkspaceClient,
    jobs: dict[str, dict[str, int]],
    job_type: str,
    dst_industry: str,
) -> int:
    source_industry = ""
    source_job_id = None
    for ind, jid in sorted(jobs.get(job_type, {}).items()):
        if ind == dst_industry:
            continue
        source_industry = ind
        source_job_id = jid
        break

    if not source_job_id or not source_industry:
        raise RuntimeError(f"No template found for job_type={job_type}")

    detail = client.api_client.do("GET", "/api/2.1/jobs/get", query={"job_id": str(source_job_id)})
    settings = deepcopy((detail or {}).get("settings") or {})
    if not settings:
        raise RuntimeError(f"Template job settings missing for job_id={source_job_id}")

    settings = _deep_replace_strings(settings, source_industry, dst_industry)
    if "name" in settings:
        settings["name"] = _replace_tokens(settings["name"], source_industry, dst_industry)

    created = client.api_client.do("POST", "/api/2.1/jobs/create", body=settings)
    new_id = int(created["job_id"])
    return new_id


def print_matrix(cfg: MatrixConfig, jobs: dict[str, dict[str, int]], pipes: dict[str, str]) -> None:
    for industry in cfg.industries:
        parts = []
        for job_type in cfg.required_jobs:
            parts.append(f"{job_type}:{'Y' if industry in jobs.get(job_type, {}) else 'N'}")
        parts.append(f"dlt:{'Y' if industry in pipes else 'N'}")
        print(f"{industry:14} {' '.join(parts)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile OT-PDM job/pipeline parity from config.")
    parser.add_argument("--config", default=str(DEFAULT_MATRIX), help="Path to industry matrix YAML.")
    parser.add_argument("--owner", default="", help="Creator email to filter jobs.")
    parser.add_argument("--apply", action="store_true", help="Create missing industry jobs from template jobs.")
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    client = WorkspaceClient(profile=cfg.profile)
    owner = resolve_owner_email(client, args.owner or None)

    print(f"owner={owner}")
    print(f"target={cfg.target} profile={cfg.profile}")
    print("configured_industries=", ",".join(cfg.industries))

    jobs = fetch_job_matrix(client, owner)
    pipes = fetch_pipeline_matrix(client)
    print("\nCurrent matrix:")
    print_matrix(cfg, jobs, pipes)

    missing = summarize_gaps(cfg, jobs, pipes)
    if not missing:
        print("\nStatus: matrix already complete.")
        return 0

    print("\nMissing resources:")
    for resource, industry in missing:
        print(f"- {resource}::{industry}")

    if not args.apply:
        print("\nRun with --apply to create missing jobs from config pattern.")
        return 2

    print("\nReconciling missing jobs from existing OT-PDM templates...")
    create_failures: list[tuple[str, str]] = []
    for resource, industry in missing:
        if resource == "dlt":
            continue
        try:
            new_id = create_job_from_template(client, jobs, resource, industry)
            jobs.setdefault(resource, {})[industry] = new_id
            print(f"[created] {resource}::{industry} -> job_id={new_id}")
        except Exception as exc:
            create_failures.append((f"{resource}::{industry}", str(exc)))

    # Refresh state after deploy attempts.
    jobs = fetch_job_matrix(client, owner)
    pipes = fetch_pipeline_matrix(client)
    print("\nPost-reconcile matrix:")
    print_matrix(cfg, jobs, pipes)
    missing_after = summarize_gaps(cfg, jobs, pipes)

    if create_failures:
        print("\nCreate failures:")
        for key, reason in create_failures:
            print(f"- {key}: {reason}")

    if missing_after:
        print("\nStill missing after reconcile:")
        for resource, industry in missing_after:
            print(f"- {resource}::{industry}")
        return 3

    print("\nStatus: matrix complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
