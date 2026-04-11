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

SITE_CURRENCY_OVERRIDES = {
    "gangwon_mine": "KRW",
    "seoul_hub": "KRW",
    "ulsan_plant": "KRW",
    "giheung_fab": "KRW",
    "jeju_grid": "KRW",
    "nagoya": "JPY",
    "naka_fab": "JPY",
    "odisha_hub": "INR",
    "chennai_hub": "INR",
    "pune_plant": "INR",
    "bengaluru_fab": "INR",
    "gujarat_grid": "INR",
    "kalimantan_hub": "SGD",
    "singapore_hub": "SGD",
    "bangkok_plant": "SGD",
    "penang_fab": "SGD",
    "vietnam_delta": "SGD",
}


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


def _render_erp_bdc_schema_sql(catalog: str) -> None:
    path = ROOT / "core" / "catalog" / "erp_bdc_schema.sql"
    if not path.exists():
        return
    ddl = path.read_text(encoding="utf-8").replace("${catalog_name}", catalog)
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        try:
            _run_sql(stmt)
        except Exception as e:
            print(f"[warn] erp_bdc ddl skipped: {e}")


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
    _run_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.finance.pm_site_financial_daily (
          ds DATE,
          industry STRING,
          site_id STRING,
          currency STRING,
          avoided_cost DOUBLE,
          intervention_cost DOUBLE,
          net_benefit DOUBLE,
          critical_assets INT,
          warning_assets INT,
          updated_at TIMESTAMP
        ) USING DELTA
        """
    )


def _ensure_lakebase_columns(catalog: str) -> None:
    alter_stmts = [
        f"ALTER TABLE {catalog}.lakebase.maintenance_schedule ADD COLUMNS (site_id STRING, area_id STRING, unit_id STRING)",
        f"ALTER TABLE {catalog}.lakebase.parts_inventory ADD COLUMNS (site_id STRING, area_id STRING, unit_id STRING, equipment_id STRING)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (site_id STRING, area_id STRING, unit_id STRING)",
        # ERP / ODS extensions (SAP BDC-shaped pipeline → Lakebase operational store)
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (work_center STRING, cost_center STRING, plant_code STRING)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (expected_failure_cost DOUBLE, intervention_cost DOUBLE, net_ebit_impact DOUBLE)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (avoided_downtime_cost DOUBLE, avoided_quality_cost DOUBLE, avoided_energy_cost DOUBLE)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (failure_probability DOUBLE, rul_hours DOUBLE)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (source_system STRING, bdc_session_id STRING, amount_currency STRING)",
    ]
    for stmt in alter_stmts:
        try:
            _run_sql(stmt)
        except Exception:
            # Ignore if columns already exist.
            pass


# Mirrors app/server.py _executive_profile plant metadata for believable SAP labels.
_ERP_DEMO_META: dict[str, dict[str, Any]] = {
    "mining": {
        "plant_code": "AU-MIN-01",
        "planner": "MIN-PLAN-A",
        "currency": "AUD",
        "cost_centers": ["MIN-EXTR-210", "MIN-MAINT-110", "MIN-CRUSH-320"],
        "work_centers": ["Pit Maintenance", "Mobile Equipment", "Crushing Line"],
    },
    "energy": {
        "plant_code": "AU-ENE-07",
        "planner": "ENE-PLAN-B",
        "currency": "AUD",
        "cost_centers": ["ENE-GRID-410", "ENE-OPS-120", "ENE-STOR-230"],
        "work_centers": ["Generation Ops", "Grid Reliability", "BESS Maintenance"],
    },
    "water": {
        "plant_code": "AU-WAT-04",
        "planner": "WAT-PLAN-C",
        "currency": "AUD",
        "cost_centers": ["WAT-DIST-180", "WAT-PUMP-220", "WAT-QUAL-090"],
        "work_centers": ["Pump Station Team", "Distribution Ops", "Water Quality"],
    },
    "automotive": {
        "plant_code": "JP-AUTO-12",
        "planner": "AUTO-PLAN-A",
        "currency": "JPY",
        "cost_centers": ["AUTO-PRESS-510", "AUTO-BODY-540", "AUTO-MACH-530"],
        "work_centers": ["Press Shop", "Body Welding", "CNC Machining"],
    },
    "semiconductor": {
        "plant_code": "JP-SEM-22",
        "planner": "SEM-PLAN-Z",
        "currency": "USD",
        "cost_centers": ["SEM-ETCH-710", "SEM-LITHO-730", "SEM-METRO-760"],
        "work_centers": ["Etch Bay", "Lithography Bay", "Yield Engineering"],
    },
}


def _seed_erp_bdc_demo(catalog: str, industry: str, cfg: dict[str, Any]) -> None:
    """Land SAP-shaped rows in bronze + hydrate lakebase.work_orders (simulates BDC → ODS)."""
    sim_assets = [a for a in (cfg.get("simulator", {}) or {}).get("assets", []) if a.get("id")][:6]
    if not sim_assets:
        return
    meta = _ERP_DEMO_META.get(industry, _ERP_DEMO_META["mining"])
    plant = str(meta["plant_code"])
    ccy = str(meta["currency"])
    centers = list(meta["cost_centers"] or ["OPS-000"])
    works = list(meta["work_centers"] or ["Maintenance"])
    ind_u = industry[:3].upper()

    try:
        _run_sql(f"DELETE FROM {catalog}.lakebase.work_orders WHERE source_system = 'SAP_BDC_DEMO'")
        _run_sql(
            f"DELETE FROM {catalog}.bronze.erp_bdc_work_orders WHERE industry = {_lit(industry)} AND bdc_session_id LIKE 'BDC-SESS-%'"
        )
        _run_sql(f"DELETE FROM {catalog}.bronze.erp_bdc_cost_centers WHERE industry = {_lit(industry)}")
    except Exception:
        pass

    for cc in centers[:4]:
        try:
            _run_sql(
                f"""
                INSERT INTO {catalog}.bronze.erp_bdc_cost_centers
                  (cost_center, plant_code, controlling_area, description, industry, valid_from, bdc_session_id)
                VALUES
                  (
                    {_lit(cc)},
                    {_lit(plant)},
                    '1000',
                    {_lit(f"Maint cost center {cc}")},
                    {_lit(industry)},
                    current_date(),
                    {_lit(f"BDC-SESS-{ind_u}-CC")}
                  )
                """
            )
        except Exception as e:
            print(f"[warn] erp_bdc cost_center seed {cc}: {e}")

    union_parts: list[str] = []
    for idx, a in enumerate(sim_assets):
        aid = str(a.get("id") or "")
        site = str(a.get("site") or "site_1")
        area = str(a.get("area") or "area_1")
        unit = str(a.get("unit") or "unit_1")
        aufnr = f"00{ind_u}{100045 + idx}"
        bdc_sess = f"BDC-SESS-{ind_u}-20260411"
        bdc_batch = f"BATCH-{ind_u}-{4200 + idx}"
        sap_stat = "REL" if idx % 2 == 0 else "GSTR"
        lb_status = "open" if idx % 2 == 0 else "scheduled"
        pri_sap = "1" if idx == 0 else "2"
        lb_pri = "P1" if idx == 0 else "P2"
        wc = works[idx % len(works)]
        kostl = centers[idx % len(centers)]
        base = 14000.0 + (idx * 9200.0)
        planned = round(base * 0.92, 2)
        actual = round(base * 0.88, 2) if idx > 2 else None
        efc = round(base * 1.62, 2)
        ic = round(base * 0.48, 2)
        net = round(max(0.0, efc - ic), 2)
        ad = round(net * 0.55, 2)
        aq = round(net * 0.28, 2)
        ae = round(net * 0.17, 2)
        fp = round(0.55 + idx * 0.06, 3)
        rul = round(120.0 - idx * 14.0, 1)
        short = f"PM corrective — {aid} vibration / thermal watch"

        try:
            _run_sql(
                f"""
                INSERT INTO {catalog}.bronze.erp_bdc_work_orders
                  (aufnr, equipment_id, site_id, plant_code, work_center, cost_center, order_type,
                   sap_system_status, sap_user_status, priority_sap, planned_cost, actual_cost, currency,
                   short_text, bdc_session_id, bdc_batch_id, industry)
                VALUES
                  (
                    {_lit(aufnr)}, {_lit(aid)}, {_lit(site)}, {_lit(plant)}, {_lit(wc)}, {_lit(kostl)},
                    'PM01', {_lit(sap_stat)}, 'PMCL', {_lit(pri_sap)}, {planned}, {actual if actual is not None else 'NULL'},
                    {_lit(ccy)}, {_lit(short)}, {_lit(bdc_sess)}, {_lit(bdc_batch)}, {_lit(industry)}
                  )
                """
            )
        except Exception as e:
            print(f"[warn] erp_bdc bronze wo {aufnr}: {e}")

        union_parts.append(
            f"""SELECT
              {_lit(aufnr)} AS work_order_id,
              {_lit(site)} AS site_id,
              {_lit(area)} AS area_id,
              {_lit(unit)} AS unit_id,
              {_lit(aid)} AS equipment_id,
              {_lit("bearing_wear")} AS failure_mode,
              {_lit(lb_pri)} AS priority,
              {_lit(lb_status)} AS status,
              current_timestamp() AS scheduled_time,
              CAST(ARRAY() AS ARRAY<STRING>) AS parts_required,
              {round(4.5 + idx * 0.5, 2)} AS estimated_hours,
              {_lit(wc)} AS work_center,
              {_lit(kostl)} AS cost_center,
              {_lit(plant)} AS plant_code,
              {efc} AS expected_failure_cost,
              {ic} AS intervention_cost,
              {net} AS net_ebit_impact,
              {ad} AS avoided_downtime_cost,
              {aq} AS avoided_quality_cost,
              {ae} AS avoided_energy_cost,
              {fp} AS failure_probability,
              {rul} AS rul_hours,
              'SAP_BDC_DEMO' AS source_system,
              {_lit(bdc_sess)} AS bdc_session_id,
              {_lit(ccy)} AS amount_currency"""
        )

    if not union_parts:
        return
    try:
        body = " UNION ALL ".join(union_parts)
        _run_sql(
            f"""
            INSERT INTO {catalog}.lakebase.work_orders
              (work_order_id, site_id, area_id, unit_id, equipment_id, failure_mode, priority, status,
               scheduled_time, parts_required, estimated_hours, work_center, cost_center, plant_code,
               expected_failure_cost, intervention_cost, net_ebit_impact,
               avoided_downtime_cost, avoided_quality_cost, avoided_energy_cost,
               failure_probability, rul_hours, source_system, bdc_session_id, amount_currency)
            {body}
            """
        )
        print(f"[seed] erp_bdc + lakebase work_orders demo rows for {industry}")
    except Exception as e:
        print(f"[warn] lakebase erp hydrate failed: {e}")


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
        "lakebase.work_orders",
        "bronze.asset_metadata",
        "bronze.erp_bdc_work_orders",
        "bronze.erp_bdc_cost_centers",
        "finance.pm_financial_daily",
        "finance.pm_site_financial_daily",
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
        "finance.pm_site_financial_daily",
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


def _site_currency(site_id: str, default_currency: str) -> str:
    return SITE_CURRENCY_OVERRIDES.get(site_id, default_currency)


def _build_parts_inventory(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    cur = str(cfg.get("agent", {}).get("terminology", {}).get("cost_currency", "USD"))
    rows: list[dict[str, Any]] = []
    for a in cfg.get("simulator", {}).get("assets", []):
        aid = str(a.get("id", ""))
        site_id = str(a.get("site", ""))
        area_id = str(a.get("area", ""))
        unit_id = str(a.get("unit", ""))
        if not aid:
            continue
        site_cur = _site_currency(site_id, cur)
        rows.append(
            {
                "site_id": site_id,
                "area_id": area_id,
                "unit_id": unit_id,
                "equipment_id": aid,
                "part_number": f"{aid}-KIT-A",
                "description": f"{aid} maintenance kit",
                "quantity": 2,
                "location": area_id or "main",
                "depot": site_id or "central",
                "unit_cost": 1200.0,
                "currency": site_cur,
                "reorder_point": 1,
                "lead_time_days": 7,
                "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        rows.append(
            {
                "site_id": site_id,
                "area_id": area_id,
                "unit_id": unit_id,
                "equipment_id": aid,
                "part_number": f"{aid}-SPARE-B",
                "description": f"{aid} critical spare",
                "quantity": 1,
                "location": area_id or "main",
                "depot": site_id or "central",
                "unit_cost": 2200.0,
                "currency": site_cur,
                "reorder_point": 1,
                "lead_time_days": 14,
                "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return rows


def _build_maintenance_schedule(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    shifts = [("Day Shift", 8), ("Night Shift", 20)]
    for i, a in enumerate(cfg.get("simulator", {}).get("assets", [])):
        sev = float(a.get("fault_severity", 0.0) or 0.0)
        shift_label, shift_hour = shifts[i % len(shifts)]
        shift_start = now.replace(hour=shift_hour)
        if shift_start < now:
            shift_start = shift_start + timedelta(days=1)
        shift_end = shift_start + timedelta(hours=12)
        has_window = sev >= 0.5
        window_start = shift_start + timedelta(hours=2) if has_window else None
        window_end = (window_start + timedelta(hours=2)) if has_window else None
        rows.append(
            {
                "site_id": str(a.get("site", "")),
                "area_id": str(a.get("area", "")),
                "unit_id": str(a.get("unit", "")),
                "equipment_id": str(a.get("id", "")),
                "shift_label": shift_label,
                "shift_start": shift_start.strftime("%Y-%m-%d %H:%M:%S"),
                "shift_end": shift_end.strftime("%Y-%m-%d %H:%M:%S"),
                "planned_downtime_hours": 2.0 if has_window else 0.5,
                "maintenance_window_start": window_start.strftime("%Y-%m-%d %H:%M:%S") if window_start else None,
                "maintenance_window_end": window_end.strftime("%Y-%m-%d %H:%M:%S") if window_end else None,
                "crew_available": True if has_window else (i % 3 != 0),
            }
        )
    return rows


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


def _seed_site_finance(industry: str, cfg: dict[str, Any], days: int) -> None:
    catalog = cfg["catalog"]
    profile = FINANCE_PROFILES.get(industry, FINANCE_PROFILES["mining"])
    default_cur = str(profile["currency"])
    assets = cfg.get("simulator", {}).get("assets", [])
    site_stats: dict[str, dict[str, int]] = {}
    for a in assets:
        site = str(a.get("site", ""))
        sev = float(a.get("fault_severity", 0.0) or 0.0)
        s = site_stats.setdefault(site, {"critical": 0, "warning": 0, "total": 0})
        s["total"] += 1
        if sev >= 0.8:
            s["critical"] += 1
        elif sev >= 0.5:
            s["warning"] += 1
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(30, days))
    rng = random.Random(f"{industry}:site-finance:v2")
    rows: list[dict[str, Any]] = []
    day = start
    while day <= end:
        for site_id, st in site_stats.items():
            critical = st["critical"]
            warning = st["warning"]
            risk_weight = (critical * 1.0) + (warning * 0.45)
            avoided = max(0.0, rng.uniform(900.0, 2200.0) * (1.0 + risk_weight))
            intervention = max(0.0, avoided * rng.uniform(0.22, 0.45))
            rows.append(
                {
                    "ds": day.isoformat(),
                    "industry": industry,
                    "site_id": site_id,
                    "currency": _site_currency(site_id, default_cur),
                    "avoided_cost": round(avoided, 2),
                    "intervention_cost": round(intervention, 2),
                    "net_benefit": round(avoided - intervention, 2),
                    "critical_assets": critical,
                    "warning_assets": warning,
                    "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        day += timedelta(days=1)
    _seed_json_table(catalog, "finance.pm_site_financial_daily", rows)


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
    _render_erp_bdc_schema_sql(catalog)
    _ensure_lakebase_columns(catalog)
    _create_finance_table(catalog)
    if RESET_EXISTING:
        _truncate_seed_targets(catalog)

    _seed_json_table(catalog, "lakebase.parts_inventory", _build_parts_inventory(cfg))
    _seed_json_table(catalog, "lakebase.maintenance_schedule", _build_maintenance_schedule(cfg))
    _seed_asset_metadata(industry, cfg)
    _seed_erp_bdc_demo(catalog, industry, cfg)
    _seed_feature_vectors(cfg)
    _seed_finance(industry, cfg, HISTORY_DAYS)
    _seed_site_finance(industry, cfg, HISTORY_DAYS)
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

