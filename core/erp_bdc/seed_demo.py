"""
SAP/BDC-shaped bronze + Lakebase ODS hydrate for executive work orders demo.

Used by RUNME_BOOTSTRAP_ALL and by ot-pdm-erp-bdc-seed job for refresh.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any


def sql_lit(v: Any) -> str:
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


ERP_DEMO_META: dict[str, dict[str, Any]] = {
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


def apply_erp_bdc_schema(spark, catalog: str, repo_root: Path) -> None:
    path = repo_root / "core" / "catalog" / "erp_bdc_schema.sql"
    if not path.exists():
        return
    ddl = path.read_text(encoding="utf-8").replace("${catalog_name}", catalog)
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        try:
            spark.sql(stmt)
        except Exception as e:
            print(f"[warn] erp_bdc ddl skipped: {e}")


def ensure_lakebase_work_orders_table(spark, catalog: str) -> None:
    """Minimal table if bootstrap never created lakebase.work_orders."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.lakebase")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.lakebase.work_orders (
          work_order_id    STRING NOT NULL,
          site_id          STRING,
          area_id          STRING,
          unit_id          STRING,
          equipment_id     STRING NOT NULL,
          failure_mode     STRING,
          priority         STRING NOT NULL,
          status           STRING NOT NULL,
          scheduled_time   TIMESTAMP,
          parts_required   ARRAY<STRING>,
          estimated_hours  DOUBLE,
          created_at       TIMESTAMP,
          updated_at       TIMESTAMP
        )
        USING DELTA
        """
    )


def ensure_lakebase_columns(spark, catalog: str) -> None:
    """Idempotent ALTERs matching RUNME_BOOTSTRAP_ALL (site + ERP ODS columns)."""
    alter_stmts = [
        f"ALTER TABLE {catalog}.lakebase.maintenance_schedule ADD COLUMNS (site_id STRING, area_id STRING, unit_id STRING)",
        f"ALTER TABLE {catalog}.lakebase.parts_inventory ADD COLUMNS (site_id STRING, area_id STRING, unit_id STRING, equipment_id STRING)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (site_id STRING, area_id STRING, unit_id STRING)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (work_center STRING, cost_center STRING, plant_code STRING)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (expected_failure_cost DOUBLE, intervention_cost DOUBLE, net_ebit_impact DOUBLE)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (avoided_downtime_cost DOUBLE, avoided_quality_cost DOUBLE, avoided_energy_cost DOUBLE)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (failure_probability DOUBLE, rul_hours DOUBLE)",
        f"ALTER TABLE {catalog}.lakebase.work_orders ADD COLUMNS (source_system STRING, bdc_session_id STRING, amount_currency STRING)",
    ]
    for stmt in alter_stmts:
        try:
            spark.sql(stmt)
        except Exception:
            pass


def seed_erp_bdc_demo(spark, catalog: str, industry: str, cfg: dict[str, Any]) -> None:
    """Land SAP-shaped rows in bronze + hydrate lakebase.work_orders (simulates BDC → ODS)."""

    def _sql(stmt: str) -> None:
        spark.sql(stmt)

    sim_assets = [a for a in (cfg.get("simulator", {}) or {}).get("assets", []) if a.get("id")][:6]
    if not sim_assets:
        print(f"[skip] erp_bdc seed: no simulator assets for industry={industry}")
        return
    meta = ERP_DEMO_META.get(industry, ERP_DEMO_META["mining"])
    plant = str(meta["plant_code"])
    ccy = str(meta["currency"])
    centers = list(meta["cost_centers"] or ["OPS-000"])
    works = list(meta["work_centers"] or ["Maintenance"])
    ind_u = industry[:3].upper()

    try:
        _sql(f"DELETE FROM {catalog}.lakebase.work_orders WHERE source_system = 'SAP_BDC_DEMO'")
        _sql(
            f"DELETE FROM {catalog}.bronze.erp_bdc_work_orders WHERE industry = {sql_lit(industry)} AND bdc_session_id LIKE 'BDC-SESS-%'"
        )
        _sql(f"DELETE FROM {catalog}.bronze.erp_bdc_cost_centers WHERE industry = {sql_lit(industry)}")
    except Exception:
        pass

    for cc in centers[:4]:
        try:
            _sql(
                f"""
                INSERT INTO {catalog}.bronze.erp_bdc_cost_centers
                  (cost_center, plant_code, controlling_area, description, industry, valid_from, bdc_session_id)
                VALUES
                  (
                    {sql_lit(cc)},
                    {sql_lit(plant)},
                    '1000',
                    {sql_lit(f"Maint cost center {cc}")},
                    {sql_lit(industry)},
                    current_date(),
                    {sql_lit(f"BDC-SESS-{ind_u}-CC")}
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
            _sql(
                f"""
                INSERT INTO {catalog}.bronze.erp_bdc_work_orders
                  (aufnr, equipment_id, site_id, plant_code, work_center, cost_center, order_type,
                   sap_system_status, sap_user_status, priority_sap, planned_cost, actual_cost, currency,
                   short_text, bdc_session_id, bdc_batch_id, industry)
                VALUES
                  (
                    {sql_lit(aufnr)}, {sql_lit(aid)}, {sql_lit(site)}, {sql_lit(plant)}, {sql_lit(wc)}, {sql_lit(kostl)},
                    'PM01', {sql_lit(sap_stat)}, 'PMCL', {sql_lit(pri_sap)}, {planned}, {actual if actual is not None else 'NULL'},
                    {sql_lit(ccy)}, {sql_lit(short)}, {sql_lit(bdc_sess)}, {sql_lit(bdc_batch)}, {sql_lit(industry)}
                  )
                """
            )
        except Exception as e:
            print(f"[warn] erp_bdc bronze wo {aufnr}: {e}")

        union_parts.append(
            f"""SELECT
              {sql_lit(aufnr)} AS work_order_id,
              {sql_lit(site)} AS site_id,
              {sql_lit(area)} AS area_id,
              {sql_lit(unit)} AS unit_id,
              {sql_lit(aid)} AS equipment_id,
              {sql_lit("bearing_wear")} AS failure_mode,
              {sql_lit(lb_pri)} AS priority,
              {sql_lit(lb_status)} AS status,
              current_timestamp() AS scheduled_time,
              CAST(ARRAY() AS ARRAY<STRING>) AS parts_required,
              {round(4.5 + idx * 0.5, 2)} AS estimated_hours,
              {sql_lit(wc)} AS work_center,
              {sql_lit(kostl)} AS cost_center,
              {sql_lit(plant)} AS plant_code,
              {efc} AS expected_failure_cost,
              {ic} AS intervention_cost,
              {net} AS net_ebit_impact,
              {ad} AS avoided_downtime_cost,
              {aq} AS avoided_quality_cost,
              {ae} AS avoided_energy_cost,
              {fp} AS failure_probability,
              {rul} AS rul_hours,
              'SAP_BDC_DEMO' AS source_system,
              {sql_lit(bdc_sess)} AS bdc_session_id,
              {sql_lit(ccy)} AS amount_currency"""
        )

    if not union_parts:
        return
    try:
        body = " UNION ALL ".join(union_parts)
        _sql(
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
        print(f"[seed] erp_bdc + lakebase work_orders demo rows for {industry} ({catalog})")
    except Exception as e:
        print(f"[warn] lakebase erp hydrate failed: {e}")


def run_refresh_for_industry(spark, repo_root: Path, industry: str) -> None:
    """Create schemas/tables as needed, extend columns, refresh demo rows for one industry."""
    import yaml

    cfg_path = repo_root / "industries" / industry / "config.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config: {cfg_path}")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    catalog = str(cfg.get("catalog") or f"pdm_{industry}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.lakebase")
    apply_erp_bdc_schema(spark, catalog, repo_root)
    ensure_lakebase_work_orders_table(spark, catalog)
    ensure_lakebase_columns(spark, catalog)
    seed_erp_bdc_demo(spark, catalog, industry, cfg)
