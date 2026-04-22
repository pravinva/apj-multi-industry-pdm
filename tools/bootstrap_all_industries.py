import json
import math
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service

ROOT = Path(__file__).resolve().parents[1]
INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
WAREHOUSE_ID = "4b9b953939869799"


def _escape(v: str) -> str:
    return v.replace("'", "''")


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return "NULL"
        return str(v)
    if isinstance(v, str):
        return f"'{_escape(v)}'"
    return f"'{_escape(str(v))}'"


def _run_sql(client: WorkspaceClient, statement: str) -> None:
    resp = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="50s",
        disposition=sql_service.Disposition.INLINE,
    )
    status = resp.status.state if resp.status else None
    if status != sql_service.StatementState.SUCCEEDED:
        err = resp.status.error if resp.status else None
        msg = ""
        if err is not None:
            msg = f"{getattr(err, 'error_code', '')}: {getattr(err, 'message', '')}"
        raise RuntimeError(f"SQL failed [{status}] {msg}: {statement[:300]}")


def _run_sql_soft(client: WorkspaceClient, statement: str) -> None:
    try:
        _run_sql(client, statement)
    except Exception:
        return


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _load_json(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _exec_schema(client: WorkspaceClient, catalog: str) -> None:
    _run_sql(client, f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for sch in ["bronze", "silver", "gold", "lakebase", "agent_tools", "models"]:
        _run_sql(client, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{sch}")
        _run_sql(client, f"GRANT USE SCHEMA ON SCHEMA {catalog}.{sch} TO `account users`")
    _run_sql(client, f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.bronze.sensor_readings (
          site_id STRING,
          area_id STRING,
          unit_id STRING,
          equipment_id STRING,
          component_id STRING,
          tag_name STRING,
          value DOUBLE,
          unit STRING,
          quality STRING,
          quality_code STRING,
          source_protocol STRING,
          timestamp TIMESTAMP,
          _ingested_at TIMESTAMP
        ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.bronze.pi_simulated_tags (
          site_id STRING,
          area_id STRING,
          unit_id STRING,
          equipment_id STRING,
          component_id STRING,
          tag_name STRING,
          value DOUBLE,
          unit STRING,
          quality STRING,
          quality_code STRING,
          source_protocol STRING,
          timestamp TIMESTAMP
        ) USING DELTA
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.silver.sensor_features (
          equipment_id STRING,
          tag_name STRING,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          mean_15m DOUBLE,
          stddev_15m DOUBLE,
          slope_1h DOUBLE,
          zscore_30d DOUBLE,
          cumsum_24h DOUBLE,
          quality_good_pct DOUBLE,
          reading_count INT,
          _processed_at TIMESTAMP
        ) USING DELTA
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.silver.ot_pi_aligned (
          site_id STRING,
          area_id STRING,
          unit_id STRING,
          equipment_id STRING,
          tag_name STRING,
          ot_timestamp TIMESTAMP,
          ot_value DOUBLE,
          ot_unit STRING,
          ot_quality STRING,
          pi_timestamp TIMESTAMP,
          pi_value DOUBLE,
          pi_unit STRING,
          pi_quality STRING,
          time_delta_seconds BIGINT,
          data_source STRING,
          _processed_at TIMESTAMP
        ) USING DELTA
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.gold.pdm_predictions (
          equipment_id STRING,
          prediction_timestamp TIMESTAMP,
          anomaly_score DOUBLE,
          anomaly_label STRING,
          rul_hours DOUBLE,
          predicted_failure_date TIMESTAMP,
          top_contributing_sensor STRING,
          top_contributing_score DOUBLE,
          model_version_anomaly STRING,
          model_version_rul STRING,
          _scored_at TIMESTAMP
        ) USING DELTA
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.gold.financial_impact_events (
          equipment_id STRING,
          prediction_timestamp TIMESTAMP,
          severity STRING,
          anomaly_score DOUBLE,
          rul_hours DOUBLE,
          event_type STRING,
          shift_label STRING,
          maintenance_window_start TIMESTAMP,
          maintenance_window_end TIMESTAMP,
          has_maintenance_window BOOLEAN,
          crew_available BOOLEAN,
          downtime_hours DOUBLE,
          maintenance_cost DOUBLE,
          production_loss DOUBLE,
          expected_failure_cost DOUBLE,
          avoided_cost DOUBLE,
          total_event_cost DOUBLE,
          data_source STRING,
          source_table STRING,
          _computed_at TIMESTAMP
        ) USING DELTA
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.lakebase.parts_inventory (
          part_number STRING,
          description STRING,
          quantity INT,
          location STRING,
          depot STRING,
          unit_cost DOUBLE,
          currency STRING,
          reorder_point INT,
          lead_time_days INT,
          last_updated TIMESTAMP
        ) USING DELTA
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.lakebase.maintenance_schedule (
          equipment_id STRING,
          shift_label STRING,
          shift_start TIMESTAMP,
          shift_end TIMESTAMP,
          planned_downtime_hours DOUBLE,
          maintenance_window_start TIMESTAMP,
          maintenance_window_end TIMESTAMP,
          crew_available BOOLEAN
        ) USING DELTA
        """.strip(),
    )
    _run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {catalog}.bronze.asset_metadata (
          equipment_id STRING,
          site_id STRING,
          area_id STRING,
          unit_id STRING,
          asset_type STRING,
          asset_model STRING,
          industry STRING,
          cost_per_unit DOUBLE,
          cost_currency STRING,
          created_at TIMESTAMP
        ) USING DELTA
        """.strip(),
    )

    for tbl in [
        "bronze.pi_simulated_tags",
        "bronze.sensor_readings",
        "silver.sensor_features",
        "silver.ot_pi_aligned",
        "gold.pdm_predictions",
        "gold.financial_impact_events",
        "lakebase.parts_inventory",
        "lakebase.maintenance_schedule",
        "bronze.asset_metadata",
    ]:
        _run_sql(client, f"GRANT SELECT ON TABLE {catalog}.{tbl} TO `account users`")


def _truncate_for_refresh(client: WorkspaceClient, catalog: str) -> None:
    for tbl in [
        "bronze.pi_simulated_tags",
        "bronze.sensor_readings",
        "silver.sensor_features",
        "silver.ot_pi_aligned",
        "gold.pdm_predictions",
        "gold.financial_impact_events",
        "lakebase.parts_inventory",
        "lakebase.maintenance_schedule",
        "bronze.asset_metadata",
    ]:
        _run_sql(client, f"DELETE FROM {catalog}.{tbl} WHERE TRUE")


def _seed_json_table(client: WorkspaceClient, catalog: str, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    values = []
    for r in rows:
        values.append("(" + ", ".join(_sql_literal(r.get(c)) for c in cols) + ")")
    _run_sql(
        client,
        f"INSERT INTO {catalog}.{table} ({', '.join(cols)}) VALUES {', '.join(values)}",
    )


def _seed_asset_metadata(client: WorkspaceClient, industry: str, config: dict[str, Any]) -> None:
    catalog = config["catalog"]
    cur = config.get("agent", {}).get("terminology", {}).get("cost_currency", "USD")
    cpu = float(config.get("accounts", {}).get("pipeline_monthly", 0)) / 720.0
    rows = []
    for a in config.get("simulator", {}).get("assets", []):
        rows.append(
            {
                "equipment_id": a["id"],
                "site_id": a.get("site", ""),
                "area_id": a.get("area", ""),
                "unit_id": a.get("unit", ""),
                "asset_type": a.get("type", ""),
                "asset_model": a.get("model", ""),
                "industry": industry,
                "cost_per_unit": round(cpu, 4),
                "cost_currency": cur,
            }
        )
    _seed_json_table(client, catalog, "bronze.asset_metadata", rows)


def _seed_feature_vectors(client: WorkspaceClient, config: dict[str, Any]) -> None:
    # Feature vectors are DLT-managed in bronze schema and should not be
    # written directly during bootstrap.
    return


def _seed_sensor_data(client: WorkspaceClient, config: dict[str, Any]) -> None:
    catalog = config["catalog"]
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    bronze_rows = []
    pi_rows = []
    silver_rows = []
    assets = config.get("simulator", {}).get("assets", [])
    sensors_cfg = config.get("sensors", {})

    for asset in assets:
        eid = asset["id"]
        site = asset.get("site", "")
        area = asset.get("area", "")
        unit_id = asset.get("unit", "")
        a_type = asset.get("type", "")
        sev = float(asset.get("fault_severity", 0.0))
        rng = random.Random(f"sensor:{catalog}:{eid}")
        sensors = sensors_cfg.get(a_type, [])

        for s in sensors:
            tag = s.get("name", "unknown")
            unit = s.get("unit", "")
            lo, hi = s.get("normal_range", [0.0, 1.0])
            base = (float(lo) + float(hi)) / 2.0
            spread = max(0.001, (float(hi) - float(lo)) * 0.08)
            direction = float(s.get("dir", 1))

            values = []
            for i in range(12):
                ts = now - timedelta(minutes=(55 - i * 5))
                v = base + (sev * direction * spread * 2.5) + rng.uniform(-spread, spread)
                values.append(v)
                bronze_rows.append(
                    "("
                    + ", ".join(
                        [
                            _sql_literal(site),
                            _sql_literal(area),
                            _sql_literal(unit_id),
                            _sql_literal(eid),
                            _sql_literal(None),
                            _sql_literal(tag),
                            _sql_literal(round(v, 6)),
                            _sql_literal(unit),
                            _sql_literal("good"),
                            _sql_literal("0x00"),
                            _sql_literal(config.get("simulator", {}).get("protocol", "OPC-UA")),
                            _sql_literal(ts.strftime("%Y-%m-%d %H:%M:%S")),
                            "current_timestamp()",
                        ]
                    )
                    + ")"
                )
                pi_rows.append(
                    "("
                    + ", ".join(
                        [
                            _sql_literal(site),
                            _sql_literal(area),
                            _sql_literal(unit_id),
                            _sql_literal(eid),
                            _sql_literal(None),
                            _sql_literal(tag),
                            _sql_literal(round(v * (1.0 + rng.uniform(-0.01, 0.01)), 6)),
                            _sql_literal(unit),
                            _sql_literal("good"),
                            _sql_literal("0x00"),
                            _sql_literal("PI-SIM"),
                            _sql_literal((ts - timedelta(seconds=12)).strftime("%Y-%m-%d %H:%M:%S")),
                        ]
                    )
                    + ")"
                )

            mean_v = sum(values) / len(values)
            var = sum((x - mean_v) ** 2 for x in values) / max(1, len(values) - 1)
            std_v = math.sqrt(var)
            slope = (values[-1] - values[0]) / max(1, len(values) - 1)
            zscore = 0.0 if std_v < 1e-9 else (values[-1] - mean_v) / std_v
            cumsum = sum(values)
            silver_rows.append(
                "("
                + ", ".join(
                    [
                        _sql_literal(eid),
                        _sql_literal(tag),
                        _sql_literal((now - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")),
                        _sql_literal(now.strftime("%Y-%m-%d %H:%M:%S")),
                        _sql_literal(round(mean_v, 6)),
                        _sql_literal(round(std_v, 6)),
                        _sql_literal(round(slope, 6)),
                        _sql_literal(round(zscore, 6)),
                        _sql_literal(round(cumsum, 6)),
                        _sql_literal(1.0),
                        _sql_literal(len(values)),
                        "current_timestamp()",
                    ]
                )
                + ")"
            )

    if bronze_rows:
        _run_sql(
            client,
            f"""
            INSERT INTO {catalog}.bronze.sensor_readings
            (site_id, area_id, unit_id, equipment_id, component_id, tag_name, value, unit, quality, quality_code, source_protocol, timestamp, _ingested_at)
            VALUES {", ".join(bronze_rows)}
            """.strip(),
        )
    if pi_rows:
        _run_sql(
            client,
            f"""
            INSERT INTO {catalog}.bronze.pi_simulated_tags
            (site_id, area_id, unit_id, equipment_id, component_id, tag_name, value, unit, quality, quality_code, source_protocol, timestamp)
            VALUES {", ".join(pi_rows)}
            """.strip(),
        )
    if silver_rows:
        _run_sql(
            client,
            f"""
            INSERT INTO {catalog}.silver.sensor_features
            (equipment_id, tag_name, window_start, window_end, mean_15m, stddev_15m, slope_1h, zscore_30d, cumsum_24h, quality_good_pct, reading_count, _processed_at)
            VALUES {", ".join(silver_rows)}
            """.strip(),
        )


def bootstrap_industry(client: WorkspaceClient, industry: str) -> None:
    cfg_path = ROOT / "industries" / industry / "config.yaml"
    seed_root = ROOT / "industries" / industry / "seed"
    cfg = _load_yaml(cfg_path)
    catalog = cfg["catalog"]
    parts = _load_json(seed_root / "parts_inventory.json")
    maint = _load_json(seed_root / "maintenance_schedule.json")

    _exec_schema(client, catalog)
    _truncate_for_refresh(client, catalog)
    _seed_json_table(client, catalog, "lakebase.parts_inventory", parts)
    _seed_json_table(client, catalog, "lakebase.maintenance_schedule", maint)
    _seed_asset_metadata(client, industry, cfg)
    _seed_sensor_data(client, cfg)
    _seed_feature_vectors(client, cfg)
    try:
        from tools.seed_finance_genie_support import SqlRunner as FinanceSqlRunner, seed_industry as seed_finance_support_industry

        wh_id = (
            os.getenv("OT_PDM_WAREHOUSE_ID", "").strip()
            or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "").strip()
            or WAREHOUSE_ID
        )
        seed_finance_support_industry(FinanceSqlRunner(client=client, warehouse_id=wh_id), industry)
    except Exception as e:
        print(f"[warn] finance genie support seeding skipped for {industry}: {e}")

    print(f"[ok] bootstrapped {industry} -> {catalog}")


def main() -> None:
    client = WorkspaceClient(profile="DEFAULT")
    for ind in INDUSTRIES:
        bootstrap_industry(client, ind)


if __name__ == "__main__":
    main()
