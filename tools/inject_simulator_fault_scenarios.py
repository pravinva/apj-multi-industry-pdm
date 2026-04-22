from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service

ROOT = Path(__file__).resolve().parents[1]
INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
DEFAULT_WAREHOUSE_ID = "4b9b953939869799"


def _esc(value: str) -> str:
    return str(value).replace("'", "''")


def _utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class SqlRunner:
    client: WorkspaceClient
    warehouse_id: str

    def exec(self, statement: str) -> None:
        resp = self.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            wait_timeout="50s",
            disposition=sql_service.Disposition.INLINE,
        )
        status = resp.status.state if resp.status else None
        if status != sql_service.StatementState.SUCCEEDED:
            err = resp.status.error if resp.status else None
            msg = getattr(err, "message", "") if err else ""
            raise RuntimeError(f"SQL failed [{status}] {msg}\n{statement[:500]}")

    def query_rows(self, statement: str) -> list[list[Any]]:
        resp = self.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            wait_timeout="50s",
            disposition=sql_service.Disposition.INLINE,
        )
        status = resp.status.state if resp.status else None
        if status != sql_service.StatementState.SUCCEEDED:
            err = resp.status.error if resp.status else None
            msg = getattr(err, "message", "") if err else ""
            raise RuntimeError(f"SQL query failed [{status}] {msg}\n{statement[:500]}")
        return getattr(getattr(resp, "result", None), "data_array", None) or []


def _load_cfg(industry: str) -> dict[str, Any]:
    path = ROOT / "industries" / industry / "config.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _asset_sensor_names(cfg: dict[str, Any], asset_type: str) -> list[dict[str, str]]:
    sensors = (cfg.get("sensors", {}) or {}).get(asset_type) or []
    out: list[dict[str, str]] = []
    for s in sensors[:3]:
        out.append(
            {
                "name": str(s.get("name") or "sensor_value"),
                "unit": str(s.get("unit") or ""),
                "warn": str(s.get("warning_threshold") or ""),
                "crit": str(s.get("critical_threshold") or ""),
            }
        )
    if not out:
        out = [{"name": "sensor_value", "unit": "", "warn": "", "crit": ""}]
    return out


def _ensure_tables(r: SqlRunner, catalog: str) -> None:
    r.exec(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    r.exec(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
    r.exec(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
    r.exec(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.sensor_readings (
          site_id STRING NOT NULL,
          area_id STRING NOT NULL,
          unit_id STRING NOT NULL,
          equipment_id STRING NOT NULL,
          component_id STRING,
          tag_name STRING NOT NULL,
          value DOUBLE NOT NULL,
          unit STRING,
          quality STRING NOT NULL,
          quality_code STRING NOT NULL,
          source_protocol STRING NOT NULL,
          timestamp TIMESTAMP NOT NULL,
          _ingested_at TIMESTAMP
        ) USING DELTA
        """
    )
    r.exec(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.sensor_features (
          equipment_id STRING NOT NULL,
          tag_name STRING NOT NULL,
          timestamp TIMESTAMP NOT NULL,
          value DOUBLE NOT NULL,
          unit STRING,
          quality STRING NOT NULL,
          quality_code STRING NOT NULL,
          mean_15m DOUBLE,
          stddev_15m DOUBLE,
          slope_1h DOUBLE,
          zscore_30d DOUBLE,
          cumsum_24h DOUBLE,
          quality_good_pct DOUBLE,
          _processed_at TIMESTAMP
        ) USING DELTA
        """
    )
    r.exec(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.pdm_predictions (
          equipment_id STRING NOT NULL,
          prediction_timestamp TIMESTAMP NOT NULL,
          anomaly_score DOUBLE NOT NULL,
          anomaly_label STRING NOT NULL,
          rul_hours DOUBLE,
          predicted_failure_date TIMESTAMP,
          top_contributing_sensor STRING,
          top_contributing_score DOUBLE,
          model_version_anomaly STRING,
          model_version_rul STRING,
          _scored_at TIMESTAMP
        ) USING DELTA
        """
    )


def _insert_rows(
    r: SqlRunner,
    catalog: str,
    cfg: dict[str, Any],
    critical_assets: list[dict[str, Any]],
    warning_assets: list[dict[str, Any]],
) -> dict[str, int]:
    protocol = str((cfg.get("simulator", {}) or {}).get("protocol") or "OPC-UA")
    now = _utc_ts()
    sr_values: list[str] = []
    sf_values: list[str] = []
    pred_values: list[str] = []

    def add_for_asset(asset: dict[str, Any], severity: str) -> None:
        aid = str(asset.get("id") or "").strip()
        site = str(asset.get("site") or "site_1")
        area = str(asset.get("area") or "area_1")
        unit = str(asset.get("unit") or "unit_1")
        atype = str(asset.get("type") or "equipment")
        sensor_defs = _asset_sensor_names(cfg, atype)
        if severity == "critical":
            score = round(random.uniform(0.88, 0.97), 4)
            rul = round(random.uniform(2.0, 10.0), 2)
            z_base = 2.1
            quality = "bad"
        else:
            score = round(random.uniform(0.58, 0.76), 4)
            rul = round(random.uniform(14.0, 48.0), 2)
            z_base = 1.2
            quality = "uncertain"
        fail_ts = (datetime.now(timezone.utc) + timedelta(hours=rul)).strftime("%Y-%m-%d %H:%M:%S")

        for sd in sensor_defs:
            tag = str(sd["name"])
            value = round(random.uniform(30.0, 95.0), 3)
            qcode = "0x40" if quality == "bad" else "0x08"
            sr_values.append(
                "("
                + ", ".join(
                    [
                        f"'{_esc(site)}'",
                        f"'{_esc(area)}'",
                        f"'{_esc(unit)}'",
                        f"'{_esc(aid)}'",
                        "NULL",
                        f"'{_esc(tag)}'",
                        str(value),
                        f"'{_esc(sd['unit'])}'",
                        f"'{quality}'",
                        f"'{qcode}'",
                        f"'{_esc(protocol)}'",
                        f"TIMESTAMP '{_esc(now)}'",
                        "current_timestamp()",
                    ]
                )
                + ")"
            )
            sf_values.append(
                "("
                + ", ".join(
                    [
                        f"'{_esc(aid)}'",
                        f"'{_esc(tag)}'",
                        f"TIMESTAMP '{_esc(now)}'",
                        str(value),
                        f"'{_esc(sd['unit'])}'",
                        f"'{quality}'",
                        f"'{qcode}'",
                        str(value),
                        str(max(0.001, abs(value) * 0.05)),
                        str(round(random.uniform(-0.03, 0.06), 4)),
                        str(round(z_base + random.uniform(0.05, 0.8), 4)),
                        str(round(value * 8.0, 4)),
                        "0.55" if quality != "good" else "1.0",
                        "current_timestamp()",
                    ]
                )
                + ")"
            )

        pred_values.append(
            "("
            + ", ".join(
                [
                    f"'{_esc(aid)}'",
                    f"TIMESTAMP '{_esc(now)}'",
                    str(score),
                    "'anomaly'" if severity == "critical" else "'warning'",
                    str(rul),
                    f"TIMESTAMP '{_esc(fail_ts)}'",
                    "'sim_fault_script'",
                    str(score),
                    "'sim_fault_script'",
                    "'sim_fault_script'",
                ]
            )
            + ")"
        )

    for asset in critical_assets:
        add_for_asset(asset, "critical")
    for asset in warning_assets:
        add_for_asset(asset, "warning")

    if sr_values:
        r.exec(
            f"INSERT INTO {catalog}.bronze.sensor_readings "
            "(site_id, area_id, unit_id, equipment_id, component_id, tag_name, value, unit, quality, quality_code, source_protocol, timestamp, _ingested_at) VALUES "
            + ", ".join(sr_values)
        )
    if sf_values:
        r.exec(
            f"INSERT INTO {catalog}.bronze.sensor_features "
            "(equipment_id, tag_name, timestamp, value, unit, quality, quality_code, mean_15m, stddev_15m, slope_1h, zscore_30d, cumsum_24h, quality_good_pct, _processed_at) VALUES "
            + ", ".join(sf_values)
        )
    if pred_values:
        r.exec(
            f"INSERT INTO {catalog}.gold.pdm_predictions "
            "(equipment_id, prediction_timestamp, anomaly_score, anomaly_label, rul_hours, predicted_failure_date, top_contributing_sensor, top_contributing_score, model_version_anomaly, model_version_rul) VALUES "
            + ", ".join(pred_values)
        )

    return {
        "bronze_sensor_readings_rows": len(sr_values),
        "bronze_sensor_features_rows": len(sf_values),
        "gold_predictions_rows": len(pred_values),
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Inject randomized simulator fault scenarios and seed bronze/silver/gold tables.",
    )
    p.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    p.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID, help="Databricks SQL warehouse ID")
    p.add_argument(
        "--industries",
        default=",".join(INDUSTRIES),
        help="Comma-separated industries (default: all)",
    )
    p.add_argument(
        "--heavy-industries",
        type=int,
        default=2,
        help="How many random industries get 2 critical + 2 warning (others get 1 critical + 2 warning)",
    )
    p.add_argument("--seed", type=int, default=None, help="Optional random seed")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    requested = [s.strip().lower() for s in str(args.industries or "").split(",") if s.strip()]
    industries = [i for i in requested if i in INDUSTRIES]
    if not industries:
        raise SystemExit("No valid industries selected.")

    heavy_n = max(0, min(len(industries), int(args.heavy_industries)))
    heavy = set(random.sample(industries, heavy_n)) if heavy_n else set()

    client = WorkspaceClient(profile=args.profile)
    runner = SqlRunner(client=client, warehouse_id=args.warehouse_id)

    out: dict[str, Any] = {
        "profile": args.profile,
        "warehouse_id": args.warehouse_id,
        "heavy_industries": sorted(list(heavy)),
        "industries": {},
    }

    for ind in industries:
        cfg = _load_cfg(ind)
        catalog = str(cfg.get("catalog") or f"pdm_{ind}")
        assets = list((cfg.get("simulator", {}) or {}).get("assets") or [])
        if len(assets) < 4:
            raise RuntimeError(f"Industry '{ind}' has too few assets in config ({len(assets)}).")
        _ensure_tables(runner, catalog)

        random.shuffle(assets)
        crit_n, warn_n = (2, 2) if ind in heavy else (1, 2)
        selected = assets[: crit_n + warn_n]
        critical_assets = selected[:crit_n]
        warning_assets = selected[crit_n : crit_n + warn_n]
        write_counts = _insert_rows(runner, catalog, cfg, critical_assets, warning_assets)

        counts = runner.query_rows(
            f"""
            SELECT
              SUM(CASE WHEN anomaly_score >= 0.80 THEN 1 ELSE 0 END) AS critical_rows,
              SUM(CASE WHEN anomaly_score >= 0.50 AND anomaly_score < 0.80 THEN 1 ELSE 0 END) AS warning_rows
            FROM {catalog}.gold.pdm_predictions
            WHERE prediction_timestamp >= current_timestamp() - INTERVAL 30 MINUTES
            """
        )
        recent_crit = int(float((counts[0][0] if counts and counts[0] and counts[0][0] is not None else 0)))
        recent_warn = int(float((counts[0][1] if counts and counts[0] and counts[0][1] is not None else 0)))

        out["industries"][ind] = {
            "catalog": catalog,
            "plan": {"critical": crit_n, "warning": warn_n},
            "critical_assets": [str(a.get("id") or "") for a in critical_assets],
            "warning_assets": [str(a.get("id") or "") for a in warning_assets],
            "writes": write_counts,
            "recent_prediction_rows_30m": {"critical": recent_crit, "warning": recent_warn},
        }

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
