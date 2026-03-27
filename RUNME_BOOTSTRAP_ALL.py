# Databricks notebook source
"""
OT PdM Intelligence - Workspace Bootstrap Notebook

Single notebook to make the demo usable in a fresh workspace after bundle deploy:
- Creates catalogs/schemas/tables for all industry skins
- Seeds ERP/Lakebase/demo operational data
- Backfills 2 years of daily finance data
- Applies access grants
- Triggers one run each of training/scoring/finance jobs (optional)
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


def _runtime_params() -> tuple[list[str], int, str, bool]:
    defaults = {
        "industries_csv": "mining,energy,water,automotive,semiconductor",
        "history_days": "730",
        "grant_principal": "account users",
        "trigger_jobs": "true",
    }
    try:
        dbutils.widgets.text("industries_csv", defaults["industries_csv"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.text("history_days", defaults["history_days"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.text("grant_principal", defaults["grant_principal"])  # type: ignore[name-defined] # noqa: F821
        dbutils.widgets.dropdown("trigger_jobs", defaults["trigger_jobs"], ["true", "false"])  # type: ignore[name-defined] # noqa: F821
        industries_csv = dbutils.widgets.get("industries_csv")  # type: ignore[name-defined] # noqa: F821
        history_days_raw = dbutils.widgets.get("history_days")  # type: ignore[name-defined] # noqa: F821
        grant_principal = dbutils.widgets.get("grant_principal")  # type: ignore[name-defined] # noqa: F821
        trigger_jobs_raw = dbutils.widgets.get("trigger_jobs")  # type: ignore[name-defined] # noqa: F821
    except Exception:
        parser = ArgumentParser(add_help=False)
        parser.add_argument("--industries_csv", default=defaults["industries_csv"])
        parser.add_argument("--history_days", default=defaults["history_days"])
        parser.add_argument("--grant_principal", default=defaults["grant_principal"])
        parser.add_argument("--trigger_jobs", default=defaults["trigger_jobs"])
        args, _ = parser.parse_known_args()
        industries_csv = args.industries_csv
        history_days_raw = args.history_days
        grant_principal = args.grant_principal
        trigger_jobs_raw = args.trigger_jobs

    industries = [i.strip().lower() for i in industries_csv.split(",") if i.strip()]
    history_days = max(30, int(history_days_raw or "730"))
    principal = (grant_principal or defaults["grant_principal"]).strip()
    trigger = str(trigger_jobs_raw).strip().lower() == "true"
    return industries, history_days, principal, trigger


INDUSTRIES, HISTORY_DAYS, GRANT_PRINCIPAL, TRIGGER_JOBS = _runtime_params()


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


def _grant_access(catalog: str) -> None:
    p = _safe(GRANT_PRINCIPAL)
    _run_sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{p}`")
    for sch in ["bronze", "silver", "gold", "lakebase", "agent_tools", "models", "finance"]:
        _run_sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{sch} TO `{p}`")

    for tbl in [
        "bronze.sensor_readings",
        "silver.sensor_features",
        "gold.feature_vectors",
        "gold.pdm_predictions",
        "gold.maintenance_alerts",
        "lakebase.parts_inventory",
        "lakebase.maintenance_schedule",
        "bronze.asset_metadata",
        "finance.pm_financial_daily",
    ]:
        try:
            _run_sql(f"GRANT SELECT ON TABLE {catalog}.{tbl} TO `{p}`")
        except Exception:
            # Some tables (like maintenance_alerts) may not exist yet pre-DLT.
            pass


def _truncate_seed_targets(catalog: str) -> None:
    for tbl in [
        "bronze.sensor_readings",
        "silver.sensor_features",
        "gold.feature_vectors",
        "gold.pdm_predictions",
        "lakebase.parts_inventory",
        "lakebase.maintenance_schedule",
        "bronze.asset_metadata",
        "finance.pm_financial_daily",
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
    catalog = cfg["catalog"]
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    fv_rows = []

    assets = cfg.get("simulator", {}).get("assets", [])
    sensors_cfg = cfg.get("sensors", {})
    for asset in assets:
        eid = asset["id"]
        site = asset.get("site", "")
        area = asset.get("area", "")
        unit_id = asset.get("unit", "")
        a_type = asset.get("type", "")
        sev = float(asset.get("fault_severity", 0.0))
        rng = random.Random(f"{catalog}:{eid}:bootstrap")
        sensors = sensors_cfg.get(a_type, [])

        for s in sensors:
            tag = s.get("name", "unknown")
            unit = s.get("unit", "")
            lo, hi = s.get("normal_range", [0.0, 1.0])
            base = (float(lo) + float(hi)) / 2.0
            spread = max(0.001, (float(hi) - float(lo)) * 0.08)
            direction = float(s.get("dir", 1))
            # Feature vectors for model/scoring UX
            f1 = round(max(0.01, 0.7 + sev + rng.uniform(-0.05, 0.05)), 6)
            f2 = round(max(0.01, 0.4 + sev * 0.8 + rng.uniform(-0.05, 0.05)), 6)
            f3 = round(max(0.01, 0.2 + sev * 1.1 + rng.uniform(-0.05, 0.05)), 6)
            f4 = round(max(0.01, 0.1 + sev * 1.4 + rng.uniform(-0.05, 0.05)), 6)
            fv_rows.append(
                "("
                + ", ".join(
                    [
                        _lit(eid),
                        _lit((now - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")),
                        _lit(now.strftime("%Y-%m-%d %H:%M:%S")),
                        _lit(f1),
                        _lit(f2),
                        _lit(f3),
                        _lit(f4),
                        "current_timestamp()",
                    ]
                )
                + ")"
            )

    if fv_rows:
        _run_sql(
            f"""
            INSERT INTO {catalog}.gold.feature_vectors
            (equipment_id, window_start, window_end, feature_1, feature_2, feature_3, feature_4, _processed_at)
            VALUES {", ".join(fv_rows)}
            """
        )


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
    _truncate_seed_targets(catalog)

    _seed_json_table(catalog, "lakebase.parts_inventory", _load_json(seed_root / "parts_inventory.json"))
    _seed_json_table(catalog, "lakebase.maintenance_schedule", _load_json(seed_root / "maintenance_schedule.json"))
    _seed_asset_metadata(industry, cfg)
    _seed_feature_vectors(cfg)
    _seed_finance(industry, cfg, HISTORY_DAYS)
    _grant_access(catalog)

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

