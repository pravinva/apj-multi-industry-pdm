# Databricks notebook source
"""
OT PdM Intelligence - Workspace Bootstrap Notebook

Single notebook to make the demo usable in a fresh workspace after bundle deploy:
- Creates catalogs/schemas/tables for all industry skins
- Seeds ERP/Lakebase/demo operational data
- Backfills 2 years of daily finance data
- Applies access grants
- Triggers one run each of training/scoring/finance jobs (optional)
- Supports idempotent reruns with reset/non-reset modes
"""

import json
import math
import random
import re
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient


def _resolve_repo_root() -> Path:
    candidates: list[Path] = []
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parent)
    cwd = Path.cwd().resolve()
    candidates.extend([cwd, *cwd.parents])
    for root in candidates:
        if (root / "core" / "catalog" / "schema.sql").exists() and (root / "industries").exists():
            return root
    return cwd


ROOT = _resolve_repo_root()


def _runtime_params() -> tuple[list[str], int, str, bool, bool, bool]:
    defaults = {
        "industries_csv": "mining,energy,water,automotive,semiconductor",
        "history_days": "730",
        "grant_principal": "account users",
        "trigger_jobs": "true",
        "reset_existing": "true",
        "seed_demo_planning_case": "true",
    }
    try:
        dbutils.widgets.text("industries_csv", defaults["industries_csv"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.text("history_days", defaults["history_days"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.text("grant_principal", defaults["grant_principal"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.dropdown("trigger_jobs", defaults["trigger_jobs"], ["true", "false"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.dropdown("reset_existing", defaults["reset_existing"], ["true", "false"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.dropdown("seed_demo_planning_case", defaults["seed_demo_planning_case"], ["true", "false"])  # type: ignore[name-defined] # noqa: F821
        industries_csv = dbutils.widgets.get("industries_csv")  # type: ignore[name-defined] # noqa: F821
        history_days_raw = dbutils.widgets.get("history_days")  # type: ignore[name-defined] # noqa: F821
        grant_principal = dbutils.widgets.get("grant_principal")  # type: ignore[name-defined] # noqa: F821
        trigger_jobs_raw = dbutils.widgets.get("trigger_jobs")  # type: ignore[name-defined] # noqa: F821
        reset_existing_raw = dbutils.widgets.get("reset_existing")  # type: ignore[name-defined] # noqa: F821
        seed_demo_planning_case_raw = dbutils.widgets.get("seed_demo_planning_case")  # type: ignore[name-defined] # noqa: F821
    except Exception:
        parser = ArgumentParser(add_help=False)
        parser.add_argument("--industries_csv", default=defaults["industries_csv"])
        parser.add_argument("--history_days", default=defaults["history_days"])
        parser.add_argument("--grant_principal", default=defaults["grant_principal"])
        parser.add_argument("--trigger_jobs", default=defaults["trigger_jobs"])
        parser.add_argument("--reset_existing", default=defaults["reset_existing"])
        parser.add_argument("--seed_demo_planning_case", default=defaults["seed_demo_planning_case"])
        args, _ = parser.parse_known_args()
        industries_csv = args.industries_csv
        history_days_raw = args.history_days
        grant_principal = args.grant_principal
        trigger_jobs_raw = args.trigger_jobs
        reset_existing_raw = args.reset_existing
        seed_demo_planning_case_raw = args.seed_demo_planning_case

    industries = [i.strip().lower() for i in industries_csv.split(",") if i.strip()]
    history_days = max(30, int(history_days_raw or "730"))
    principal = (grant_principal or defaults["grant_principal"]).strip()
    trigger = str(trigger_jobs_raw).strip().lower() == "true"
    reset_existing = str(reset_existing_raw).strip().lower() == "true"
    seed_demo_planning_case = str(seed_demo_planning_case_raw).strip().lower() == "true"
    return industries, history_days, principal, trigger, reset_existing, seed_demo_planning_case


INDUSTRIES, HISTORY_DAYS, GRANT_PRINCIPAL, TRIGGER_JOBS, RESET_EXISTING, SEED_DEMO_PLANNING_CASE = _runtime_params()


def _safe(v: str) -> str:
    return v.replace("`", "")


def _lit(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return "NULL"
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"


def _run_sql(stmt: str) -> None:
    spark.sql(stmt)  # noqa: F821


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _load_json(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _render_schema_sql(catalog: str) -> None:
    ddl = (ROOT / "core" / "catalog" / "schema.sql").read_text(encoding="utf-8")
    ddl = ddl.replace("${catalog_name}", catalog)
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        stmt_to_run = stmt
        upper = stmt.upper()
        if (
            "CREATE TABLE" in upper
            and "DEFAULT" in upper
            and "USING DELTA" in upper
            and "DELTA.FEATURE.ALLOWCOLUMNDEFAULTS" not in upper
        ):
            if "TBLPROPERTIES" in upper:
                stmt_to_run = re.sub(
                    r"TBLPROPERTIES\s*\(",
                    "TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported', ",
                    stmt,
                    count=1,
                    flags=re.IGNORECASE,
                )
            else:
                stmt_to_run = re.sub(
                    r"USING\s+DELTA",
                    "USING DELTA TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')",
                    stmt,
                    count=1,
                    flags=re.IGNORECASE,
                )
        _run_sql(stmt_to_run)


def _create_finance_table(catalog: str) -> None:
    _run_sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.finance")
    _run_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.finance.pm_financial_daily (
          ds DATE,
          industry STRING,
          currency STRING,
          avoided_downtime_cost DOUBLE,
          avoided_quality_cost DOUBLE,
          avoided_energy_cost DOUBLE,
          intervention_cost DOUBLE,
          platform_cost DOUBLE,
          ebit_saved DOUBLE,
          net_benefit DOUBLE,
          baseline_monthly_ebit DOUBLE,
          updated_at TIMESTAMP
        ) USING DELTA
        """
    )
    _run_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.finance.pm_bootstrap_runs (
          run_ts TIMESTAMP,
          industries_csv STRING,
          history_days INT,
          reset_existing BOOLEAN,
          trigger_jobs BOOLEAN,
          seed_demo_planning_case BOOLEAN,
          executed_by STRING,
          notes STRING
        ) USING DELTA
        """
    )
    _run_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.finance.pm_demo_planning_case (
          ds DATE,
          industry STRING,
          equipment_id STRING,
          anomaly_score DOUBLE,
          risk_rank INT,
          has_maintenance_window BOOLEAN,
          crew_available BOOLEAN,
          shift_label STRING,
          maintenance_window_start TIMESTAMP,
          maintenance_window_end TIMESTAMP,
          threshold_risk_flag BOOLEAN,
          combined_risk_score DOUBLE,
          combined_risk_flag BOOLEAN,
          recommendation STRING,
          updated_at TIMESTAMP
        ) USING DELTA
        """
    )


def _grant_access(catalog: str) -> None:
    p = _safe(GRANT_PRINCIPAL)
    _run_sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{p}`")
    for sch in ["bronze", "silver", "gold", "lakebase", "agent_tools", "models", "finance"]:
        _run_sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{sch} TO `{p}`")

    for tbl in [
        "bronze.pravin_zerobus",
        "bronze.pi_simulated_tags",
        "bronze.sensor_readings",
        "silver.sensor_features",
        "silver.ot_pi_aligned",
        "gold.pdm_predictions",
        "gold.financial_impact_events",
        "gold.maintenance_alerts",
        "lakebase.parts_inventory",
        "lakebase.maintenance_schedule",
        "bronze.asset_metadata",
        "finance.pm_financial_daily",
        "finance.pm_demo_planning_case",
        "finance.pm_bootstrap_runs",
    ]:
        try:
            _run_sql(f"GRANT SELECT ON TABLE {catalog}.{tbl} TO `{p}`")
        except Exception:
            # Some tables (like maintenance_alerts) may not exist yet pre-DLT.
            pass


def _truncate_seed_targets(catalog: str) -> None:
    for tbl in [
        "bronze.pravin_zerobus",
        "bronze.pi_simulated_tags",
        "bronze.sensor_readings",
        "silver.sensor_features",
        "silver.ot_pi_aligned",
        "gold.pdm_predictions",
        "gold.financial_impact_events",
        "lakebase.parts_inventory",
        "lakebase.maintenance_schedule",
        "bronze.asset_metadata",
        "finance.pm_financial_daily",
        "finance.pm_demo_planning_case",
    ]:
        try:
            _run_sql(f"DELETE FROM {catalog}.{tbl} WHERE TRUE")
        except Exception:
            pass


def _seed_json_table(catalog: str, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    values = []
    for r in rows:
        values.append("(" + ", ".join(_lit(r.get(c)) for c in cols) + ")")
    _run_sql(
        f"INSERT INTO {catalog}.{table} ({', '.join(cols)}) VALUES {', '.join(values)}"
    )


def _seed_asset_metadata(industry: str, cfg: dict[str, Any]) -> None:
    catalog = cfg["catalog"]
    cur = cfg.get("agent", {}).get("terminology", {}).get("cost_currency", "USD")
    cpu = float(cfg.get("accounts", {}).get("pipeline_monthly", 0.0)) / 720.0
    rows = []
    for a in cfg.get("simulator", {}).get("assets", []):
        rows.append(
            {
                "equipment_id": a["id"],
                "site_id": a.get("site", ""),
                "area_id": a.get("area", ""),
                "unit_id": a.get("unit", ""),
                "asset_type": a.get("type", ""),
                "asset_model": a.get("model", ""),
                "industry": industry,
                "cost_per_unit": round(cpu, 6),
                "cost_currency": cur,
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    _seed_json_table(catalog, "bronze.asset_metadata", rows)


def _seed_feature_vectors(cfg: dict[str, Any]) -> None:
    # Feature vectors are standardized as DLT-managed materialized views in
    # bronze schema and should not be directly seeded.
    return


FINANCE_PROFILES = {
    "mining": {"currency": "AUD", "baseline_monthly_ebit": 42_000_000.0},
    "energy": {"currency": "AUD", "baseline_monthly_ebit": 28_000_000.0},
    "water": {"currency": "AUD", "baseline_monthly_ebit": 9_000_000.0},
    "automotive": {"currency": "JPY", "baseline_monthly_ebit": 620_000_000.0},
    "semiconductor": {"currency": "USD", "baseline_monthly_ebit": 55_000_000.0},
}


def _finance_rows(industry: str, days: int) -> list[dict[str, Any]]:
    profile = FINANCE_PROFILES.get(industry, FINANCE_PROFILES["mining"])
    baseline = float(profile["baseline_monthly_ebit"])
    cur = str(profile["currency"])
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(30, days))
    rng = random.Random(f"{industry}:finance:v1")
    base_daily = baseline / 30.0
    rows = []
    day = start
    i = 0
    while day <= end:
        season = 1.0 + 0.12 * math.sin((2 * math.pi * i) / 30.0) + 0.06 * math.sin((2 * math.pi * i) / 365.0)
        drift = 1.0 + (0.0003 * i)
        noise = rng.uniform(0.87, 1.13)
        ebit = max(0.0, base_daily * 0.009 * season * drift * noise)
        down = ebit * rng.uniform(1.48, 1.72)
        qual = ebit * rng.uniform(0.10, 0.20)
        energy = ebit * rng.uniform(0.11, 0.22)
        intervention = ebit * rng.uniform(0.20, 0.34)
        platform = ebit * rng.uniform(0.08, 0.16)
        net = down + qual + energy - intervention - platform
        ebit = max(0.0, net)
        rows.append(
            {
                "ds": day.isoformat(),
                "industry": industry,
                "currency": cur,
                "avoided_downtime_cost": round(down, 2),
                "avoided_quality_cost": round(qual, 2),
                "avoided_energy_cost": round(energy, 2),
                "intervention_cost": round(intervention, 2),
                "platform_cost": round(platform, 2),
                "ebit_saved": round(ebit, 2),
                "net_benefit": round(net, 2),
                "baseline_monthly_ebit": round(baseline, 2),
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        day += timedelta(days=1)
        i += 1
    return rows


def _seed_finance(industry: str, cfg: dict[str, Any], days: int) -> None:
    catalog = cfg["catalog"]
    rows = _finance_rows(industry, days)
    _seed_json_table(catalog, "finance.pm_financial_daily", rows)


def _seed_demo_planning_case(industry: str, cfg: dict[str, Any]) -> None:
    """Seed deterministic planning case rows so scenario demos are portable."""
    catalog = cfg["catalog"]
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ds = now.date().isoformat()

    if industry == "mining":
        rows = [
            {
                "ds": ds,
                "industry": industry,
                "equipment_id": "HT-001",
                "anomaly_score": 0.94,
                "risk_rank": 1,
                "has_maintenance_window": False,
                "crew_available": False,
                "shift_label": None,
                "maintenance_window_start": None,
                "maintenance_window_end": None,
                "threshold_risk_flag": True,
                "combined_risk_score": 0.93,
                "combined_risk_flag": True,
                "recommendation": "Highest risk but no maintenance window or crew assignment. Escalate scheduler and assign emergency crew.",
                "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            },
            {
                "ds": ds,
                "industry": industry,
                "equipment_id": "HT-012",
                "anomaly_score": 0.78,
                "risk_rank": 2,
                "has_maintenance_window": True,
                "crew_available": True,
                "shift_label": "Day Shift",
                "maintenance_window_start": (now + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),
                "maintenance_window_end": (now + timedelta(hours=10)).strftime("%Y-%m-%d %H:%M:%S"),
                "threshold_risk_flag": True,
                "combined_risk_score": 0.81,
                "combined_risk_flag": True,
                "recommendation": "Planned window and crew available. Execute planned intervention this shift.",
                "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            },
            {
                "ds": ds,
                "industry": industry,
                "equipment_id": "HT-007",
                "anomaly_score": 0.71,
                "risk_rank": 3,
                "has_maintenance_window": True,
                "crew_available": True,
                "shift_label": "Night Shift",
                "maintenance_window_start": (now + timedelta(hours=14)).strftime("%Y-%m-%d %H:%M:%S"),
                "maintenance_window_end": (now + timedelta(hours=16)).strftime("%Y-%m-%d %H:%M:%S"),
                "threshold_risk_flag": True,
                "combined_risk_score": 0.74,
                "combined_risk_flag": True,
                "recommendation": "Planned window and crew available. Schedule maintenance in upcoming night shift.",
                "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            },
        ]
    else:
        assets = [a.get("id") for a in cfg.get("simulator", {}).get("assets", []) if a.get("id")][:3]
        if len(assets) < 3:
            assets = (assets + ["ASSET-001", "ASSET-002", "ASSET-003"])[:3]
        rows = []
        for i, aid in enumerate(assets):
            has_window = i != 0
            anomaly = round(max(0.52, 0.86 - (i * 0.12)), 2)
            rows.append(
                {
                    "ds": ds,
                    "industry": industry,
                    "equipment_id": aid,
                    "anomaly_score": anomaly,
                    "risk_rank": i + 1,
                    "has_maintenance_window": has_window,
                    "crew_available": has_window,
                    "shift_label": None if not has_window else ("Day Shift" if i == 1 else "Night Shift"),
                    "maintenance_window_start": None if not has_window else (now + timedelta(hours=6 + (i * 6))).strftime("%Y-%m-%d %H:%M:%S"),
                    "maintenance_window_end": None if not has_window else (now + timedelta(hours=8 + (i * 6))).strftime("%Y-%m-%d %H:%M:%S"),
                    "threshold_risk_flag": anomaly >= 0.65,
                    "combined_risk_score": round(min(0.98, anomaly * 0.96 + 0.03), 2),
                    "combined_risk_flag": True,
                    "recommendation": "Use combined risk score for prioritization when maintenance window data is missing.",
                    "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
    _seed_json_table(catalog, "finance.pm_demo_planning_case", rows)


def _record_bootstrap_run(catalog: str, notes: str = "") -> None:
    _run_sql(
        f"""
        INSERT INTO {catalog}.finance.pm_bootstrap_runs
        (run_ts, industries_csv, history_days, reset_existing, trigger_jobs, seed_demo_planning_case, executed_by, notes)
        VALUES
        (
          current_timestamp(),
          {_lit(",".join(INDUSTRIES))},
          {HISTORY_DAYS},
          {str(RESET_EXISTING).upper()},
          {str(TRIGGER_JOBS).upper()},
          {str(SEED_DEMO_PLANNING_CASE).upper()},
          current_user(),
          {_lit(notes)}
        )
        """
    )


def bootstrap_industry(industry: str) -> None:
    cfg_path = ROOT / "industries" / industry / "config.yaml"
    seed_root = ROOT / "industries" / industry / "seed"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config for industry={industry}: {cfg_path}")

    cfg = _load_yaml(cfg_path)
    catalog = cfg["catalog"]
    print(f"[bootstrap] {industry} -> {catalog}")

    _run_sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for sch in ["bronze", "silver", "gold", "lakebase", "agent_tools", "models", "finance"]:
        _run_sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{sch}")

    _render_schema_sql(catalog)
    _create_finance_table(catalog)
    if RESET_EXISTING:
        _truncate_seed_targets(catalog)

    _seed_json_table(catalog, "lakebase.parts_inventory", _load_json(seed_root / "parts_inventory.json"))
    _seed_json_table(catalog, "lakebase.maintenance_schedule", _load_json(seed_root / "maintenance_schedule.json"))
    _seed_asset_metadata(industry, cfg)
    _seed_feature_vectors(cfg)
    _seed_finance(industry, cfg, HISTORY_DAYS)
    if SEED_DEMO_PLANNING_CASE:
        _seed_demo_planning_case(industry, cfg)
    _grant_access(catalog)
    _record_bootstrap_run(catalog, notes=f"bootstrap_industry={industry}")

    print(f"[ok] {industry} ready")


def _trigger_job_runs() -> None:
    ws = WorkspaceClient()
    wanted_prefixes = [
        "ot-pdm-financial-backfill-",
        "ot-pdm-training-",
        "ot-pdm-scoring-",
    ]
    jobs = list(ws.jobs.list(expand_tasks=False))
    name_to_id = {j.settings.name: j.job_id for j in jobs if j.settings and j.settings.name and j.job_id}
    for name, jid in sorted(name_to_id.items()):
        if any(name.startswith(p) for p in wanted_prefixes):
            ws.jobs.run_now(job_id=jid)
            print(f"[run_now] {name} ({jid})")


def main() -> None:
    for ind in INDUSTRIES:
        bootstrap_industry(ind)
    if TRIGGER_JOBS:
        _trigger_job_runs()
    print("[complete] Workspace bootstrap finished.")


if __name__ == "__main__":
    main()

