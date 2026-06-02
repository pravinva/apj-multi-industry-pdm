#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml


@dataclass
class JobSpec:
    key: str
    name: str
    industry_scoped: bool
    table_templates: list[str]


JOB_TABLE_MAP: dict[str, list[str]] = {
    "ot_pdm_zerobus_connector_job": [
        "{catalog}.bronze.pravin_zerobus",
    ],
    "ot_pdm_scoring_job": [
        "{catalog}.gold.pdm_predictions",
        "{catalog}.gold.financial_impact_events",
    ],
    "ot_pdm_training_job": [
        "{catalog}.models.ot_pdm_rul_* (model registry assets)",
    ],
    "ot_pdm_financial_backfill_job": [
        "{catalog}.finance.pm_financial_daily",
        "{catalog}.finance.pm_site_financial_daily",
    ],
    "ot_pdm_workspace_bootstrap_job": [
        "{catalog}.bronze.sensor_readings",
        "{catalog}.bronze.pi_simulated_tags",
        "{catalog}.gold.pdm_predictions",
        "{catalog}.gold.financial_impact_events",
        "{catalog}.lakebase.parts_inventory",
        "{catalog}.lakebase.maintenance_schedule",
        "{catalog}.lakebase.work_orders",
        "{catalog}.finance.pm_financial_daily",
        "{catalog}.finance.pm_site_financial_daily",
    ],
    "ot_pdm_erp_bdc_seed_job": [
        "{catalog}.bronze.erp_bdc_work_orders",
        "{catalog}.bronze.erp_bdc_cost_centers",
        "{catalog}.lakebase.work_orders",
        "{catalog}.finance.work_orders_genie",
    ],
    "ot_pdm_demo_scheduled_refresh": [
        "{catalog}.finance.pm_financial_daily",
        "{catalog}.finance.pm_site_financial_daily",
        "{catalog}.bronze.erp_bdc_work_orders",
        "{catalog}.bronze.erp_bdc_cost_centers",
        "{catalog}.lakebase.work_orders",
        "{catalog}.finance.work_orders_genie",
    ],
    "ot_pdm_dlt_daily_refresh_job": [
        "{catalog}.bronze.sensor_readings",
        "{catalog}.silver.sensor_features",
        "{catalog}.gold.pdm_predictions",
        "{catalog}.gold.financial_impact_events",
    ],
}


def sh_json(cmd: list[str]) -> Any:
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)


def fmt_ts(ms: int | None) -> str:
    if not ms:
        return "never"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_bundle_jobs(bundle_path: Path) -> list[JobSpec]:
    doc = yaml.safe_load(bundle_path.read_text(encoding="utf-8")) or {}
    jobs = (((doc.get("resources") or {}).get("jobs")) or {})
    specs: list[JobSpec] = []
    for key, body in jobs.items():
        name = str((body or {}).get("name") or key)
        specs.append(
            JobSpec(
                key=key,
                name=name,
                industry_scoped="${var.industry}" in name,
                table_templates=JOB_TABLE_MAP.get(key, []),
            )
        )
    return specs


def load_industry_catalogs(repo_root: Path) -> dict[str, str]:
    catalogs: dict[str, str] = {}
    for industry in ["mining", "energy", "water", "automotive", "semiconductor"]:
        p = repo_root / "industries" / industry / "config.yaml"
        if not p.exists():
            catalogs[industry] = f"pdm_{industry}"
            continue
        cfg = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        catalogs[industry] = str(cfg.get("catalog") or f"pdm_{industry}")
    return catalogs


def current_user_alias(profile: str) -> str:
    me = sh_json(["databricks", "current-user", "me", "--profile", profile, "--output", "json"])
    user = str(me.get("userName") or "")
    return user.split("@")[0].replace(".", "_")


def resolve_job(profile: str, raw_name: str, alias: str) -> dict[str, Any] | None:
    candidates = [raw_name, f"[dev {alias}] {raw_name}"]
    found: list[dict[str, Any]] = []
    for cand in candidates:
        arr = sh_json(["databricks", "jobs", "list", "--profile", profile, "--name", cand, "--output", "json"])
        if isinstance(arr, list):
            found.extend(arr)
    if not found:
        return None
    return max(found, key=lambda j: int(j.get("created_time") or 0))


def last_success_ms(profile: str, job_id: int) -> int | None:
    runs = sh_json(
        [
            "databricks",
            "jobs",
            "list-runs",
            "--profile",
            profile,
            "--job-id",
            str(job_id),
            "--completed-only",
            "--limit",
            "25",
            "--output",
            "json",
        ]
    )
    arr = runs.get("runs", []) if isinstance(runs, dict) else (runs or [])
    for r in arr:
        st = (r.get("state") or {}).get("result_state")
        if st == "SUCCESS":
            return int(r.get("end_time") or r.get("start_time") or 0)
    return None


def expand_tables(templates: list[str], industry: str | None, catalogs: dict[str, str]) -> str:
    if not templates:
        return "-"
    if industry:
        catalog = catalogs.get(industry, f"pdm_{industry}")
        return "; ".join(t.format(catalog=catalog) for t in templates)
    # Non-industry jobs: mention all industry catalogs once.
    all_tables: list[str] = []
    for ind, cat in catalogs.items():
        for t in templates:
            all_tables.append(f"{t.format(catalog=cat)} [{ind}]")
    return "; ".join(all_tables)


def resolve_table_names(templates: list[str], industry: str | None, catalogs: dict[str, str]) -> list[str]:
    if not templates:
        return []
    resolved: list[str] = []
    if industry:
        catalog = catalogs.get(industry, f"pdm_{industry}")
        for t in templates:
            # Skip non-table placeholders (for example model registry wildcard notes).
            if "*" in t or "(" in t:
                continue
            resolved.append(t.format(catalog=catalog))
        return resolved
    for ind, cat in catalogs.items():
        for t in templates:
            if "*" in t or "(" in t:
                continue
            resolved.append(t.format(catalog=cat))
    return resolved


def run_sql_statement(profile: str, warehouse_id: str, statement: str) -> dict[str, Any]:
    return sh_json(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--profile",
            profile,
            "--json",
            json.dumps({"warehouse_id": warehouse_id, "statement": statement}),
        ]
    )


def parse_describe_detail(resp: dict[str, Any]) -> tuple[int | None, datetime | None]:
    result = resp.get("result") or {}
    manifest = result.get("manifest") or {}
    schema = manifest.get("schema") or {}
    cols = schema.get("columns") or []
    data = result.get("data_array") or []
    if not data:
        return None, None
    row = data[0]
    idx_num_rows = None
    idx_last_modified = None
    for i, c in enumerate(cols):
        name = str((c or {}).get("name") or "")
        if name == "numRows":
            idx_num_rows = i
        elif name == "lastModified":
            idx_last_modified = i
    num_rows: int | None = None
    last_modified: datetime | None = None
    if idx_num_rows is not None and idx_num_rows < len(row):
        try:
            num_rows = int(float(row[idx_num_rows]))
        except (TypeError, ValueError):
            num_rows = None
    if idx_last_modified is not None and idx_last_modified < len(row):
        raw = row[idx_last_modified]
        if raw:
            try:
                last_modified = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            except ValueError:
                last_modified = None
    return num_rows, last_modified


def monitor_tables(
    profile: str,
    warehouse_id: str | None,
    tables: list[str],
    cache: dict[str, str],
) -> str:
    if not tables:
        return "-"
    if not warehouse_id:
        return "warehouse_not_set"
    counts = {"fresh": 0, "stale": 0, "empty": 0, "missing": 0, "error": 0}
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(hours=24)
    for t in tables:
        if t not in cache:
            stmt = f"DESCRIBE DETAIL {t}"
            try:
                resp = run_sql_statement(profile, warehouse_id, stmt)
                state = str(((resp.get("status") or {}).get("state")) or "")
                if state != "SUCCEEDED":
                    err_msg = str((resp.get("status") or {}).get("error") or "")
                    cache[t] = "missing" if "TABLE_OR_VIEW_NOT_FOUND" in err_msg else "error"
                else:
                    num_rows, last_modified = parse_describe_detail(resp)
                    if num_rows == 0:
                        cache[t] = "empty"
                    elif last_modified and last_modified < stale_cutoff:
                        cache[t] = "stale"
                    else:
                        cache[t] = "fresh"
            except subprocess.CalledProcessError:
                cache[t] = "error"
        counts[cache[t]] = counts.get(cache[t], 0) + 1
    return (
        f"fresh:{counts['fresh']} "
        f"stale:{counts['stale']} "
        f"empty:{counts['empty']} "
        f"missing:{counts['missing']} "
        f"error:{counts['error']}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Report OT-PDM jobs, last success, and updated tables.")
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--bundle", default="databricks.yml")
    ap.add_argument(
        "--warehouse-id",
        default=os.getenv("OT_PDM_WAREHOUSE_ID", ""),
        help="Warehouse ID for table monitoring (or set OT_PDM_WAREHOUSE_ID).",
    )
    args = ap.parse_args()

    repo_root = Path.cwd()
    specs = load_bundle_jobs(repo_root / args.bundle)
    catalogs = load_industry_catalogs(repo_root)
    alias = current_user_alias(args.profile)

    headers = ["Job", "Industry", "Workspace Job ID", "Last Successful Run", "Tables Updated", "Table Monitor"]
    rows: list[list[str]] = []
    table_monitor_cache: dict[str, str] = {}

    for spec in specs:
        if spec.industry_scoped:
            for industry in catalogs:
                name = spec.name.replace("${var.industry}", industry)
                j = resolve_job(args.profile, name, alias)
                job_id = str(j.get("job_id")) if j else "missing"
                last_ok = fmt_ts(last_success_ms(args.profile, int(job_id))) if j else "never"
                rows.append(
                    [
                        name,
                        industry,
                        job_id,
                        last_ok,
                        expand_tables(spec.table_templates, industry, catalogs),
                        monitor_tables(
                            args.profile,
                            args.warehouse_id or None,
                            resolve_table_names(spec.table_templates, industry, catalogs),
                            table_monitor_cache,
                        ),
                    ]
                )
        else:
            j = resolve_job(args.profile, spec.name, alias)
            job_id = str(j.get("job_id")) if j else "missing"
            last_ok = fmt_ts(last_success_ms(args.profile, int(job_id))) if j else "never"
            rows.append(
                [
                    spec.name,
                    "all",
                    job_id,
                    last_ok,
                    expand_tables(spec.table_templates, None, catalogs),
                    monitor_tables(
                        args.profile,
                        args.warehouse_id or None,
                        resolve_table_names(spec.table_templates, None, catalogs),
                        table_monitor_cache,
                    ),
                ]
            )

    # Markdown table (easy to paste/read).
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        safe = [c.replace("\n", " ").strip() for c in r]
        print("| " + " | ".join(safe) + " |")


if __name__ == "__main__":
    main()
