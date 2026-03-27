from __future__ import annotations

import json
import csv
import os
import random
import re
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import yaml
from cryptography.fernet import Fernet, InvalidToken

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import sql as sql_service
except Exception:  # pragma: no cover - optional runtime dependency
    WorkspaceClient = None
    sql_service = None

try:
    import psycopg
except Exception:  # pragma: no cover - optional runtime dependency
    psycopg = None

ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"
SDT_REPORT_DIR = (ROOT / "sdt-compression") if (ROOT / "sdt-compression").exists() else (ROOT.parent / "docs" / "sdt-compression")
GENIE_ROOM_MAP_PATH = ROOT / "genie_rooms.json"
INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
ISA_EMOJI = {
    "site": "🏭",
    "area": "📍",
    "unit": "⚙",
    "equipment": "🛠",
    "component": "🔩",
}
SIM_STATE: dict[str, dict[str, Any]] = {}
_SQL_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_CACHE_TTL_S = 20.0
_WAREHOUSE_ID = os.getenv("OT_PDM_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID") or "4b9b953939869799"
_DEFAULT_ZEROBUS_WORKSPACE_URL = os.getenv("OT_PDM_DEFAULT_WORKSPACE_URL", "https://e2-demo-field-eng.cloud.databricks.com")
_DEFAULT_ZEROBUS_ENDPOINT = os.getenv(
    "OT_PDM_DEFAULT_ZEROBUS_ENDPOINT",
    "https://1444828305810485.zerobus.us-west-2.zerobuss.cloud.databricks.com",
)
_DEFAULT_ZEROBUS_CLIENT_ID = os.getenv(
    "OT_PDM_DEFAULT_ZEROBUS_CLIENT_ID",
    "6ff2b11b-fdb8-4c2c-9360-ed105d5f6dcb",
)
_DEFAULT_ZEROBUS_TARGET_TABLE = os.getenv("OT_PDM_DEFAULT_ZEROBUS_TARGET_TABLE", "pravin_zerobus")
_LAKEBASE_PG_DSN = os.getenv("OT_PDM_LAKEBASE_PG_DSN", "").strip()
_LAKEBASE_PG_HOST = os.getenv("OT_PDM_LAKEBASE_PG_HOST", "ep-jolly-sea-d1xnctly.database.us-west-2.cloud.databricks.com").strip()
_LAKEBASE_PG_PORT = int(os.getenv("OT_PDM_LAKEBASE_PG_PORT", "5432"))
_LAKEBASE_PG_DB = os.getenv("OT_PDM_LAKEBASE_PG_DB", "databricks_postgres").strip()
_LAKEBASE_PG_USER = os.getenv("OT_PDM_LAKEBASE_PG_USER", "pravin.varma@databricks.com").strip()
_LAKEBASE_PG_PASSWORD = os.getenv("OT_PDM_LAKEBASE_PG_PASSWORD", "").strip()
_LAKEBASE_PG_SSLMODE = os.getenv("OT_PDM_LAKEBASE_PG_SSLMODE", "require").strip() or "require"
_LAKEBASE_ACTION_TABLE = os.getenv("OT_PDM_LAKEBASE_ACTION_TABLE", "otpdm.operator_recommendation_actions").strip() or "otpdm.operator_recommendation_actions"
_LIVE_SCORING_STATE: dict[str, dict[str, Any]] = {}
_SIM_STAGING_READY: dict[str, bool] = {}
_LIVE_SCORING_MIN_INTERVAL_S = int(os.getenv("OT_PDM_LIVE_SCORING_MIN_INTERVAL_S", "180"))
_LIVE_SCORING_STALENESS_S = int(os.getenv("OT_PDM_LIVE_SCORING_STALENESS_S", "90"))
_LIVE_SCORING_FRESH_LOOKBACK_S = int(os.getenv("OT_PDM_LIVE_SCORING_FRESH_LOOKBACK_S", "15"))
ZEROBUS_PROTOCOLS = ["opcua", "mqtt", "modbus"]
ZEROBUS_STATUS: dict[str, dict[str, bool]] = {
    p: {"active": False, "has_config": False} for p in ZEROBUS_PROTOCOLS
}
ZEROBUS_CONFIG_DIR = ROOT / ".zerobus-configs"
ZEROBUS_KEY_PATH = ROOT / ".zerobus-key"
_FERNET: Fernet | None = None
SIMULATED_TAGS: dict[str, list[dict[str, str]]] = {
    "opcua": [
        {"id": "ns=2;s=Engine.EGT", "name": "Engine.EGT", "type": "Float", "desc": "Exhaust gas temperature"},
        {"id": "ns=2;s=Engine.CoolantTemp", "name": "Engine.CoolantTemp", "type": "Float", "desc": "Engine coolant temperature"},
        {"id": "ns=2;s=Engine.OilTemp", "name": "Engine.OilTemp", "type": "Float", "desc": "Engine oil temperature"},
        {"id": "ns=2;s=Drivetrain.VibrationRMS", "name": "Drivetrain.VibrationRMS", "type": "Float", "desc": "Drivetrain vibration"},
        {"id": "ns=2;s=Tyres.FL.Pressure", "name": "Tyres.FL.Pressure", "type": "Float", "desc": "Tyre pressure"},
    ],
    "mqtt": [
        {"id": "plant/zone1/pump01/vibration", "name": "plant/zone1/pump01/vibration", "type": "JSON", "desc": "Pump vibration"},
        {"id": "plant/zone1/pump01/pressure", "name": "plant/zone1/pump01/pressure", "type": "JSON", "desc": "Pump pressure"},
        {"id": "water/meters/district4/flow", "name": "water/meters/district4/flow", "type": "JSON", "desc": "Flow rate"},
        {"id": "factory/press_line3/tonnage", "name": "factory/press_line3/tonnage", "type": "JSON", "desc": "Press tonnage"},
    ],
    "modbus": [
        {"id": "40001", "name": "Holding Register 40001", "type": "Int16", "desc": "Motor current"},
        {"id": "40002", "name": "Holding Register 40002", "type": "Int16", "desc": "Discharge pressure"},
        {"id": "40003", "name": "Holding Register 40003", "type": "Int16", "desc": "Suction pressure"},
        {"id": "30001", "name": "Input Register 30001", "type": "UInt16", "desc": "Fault word"},
    ],
    "canbus": [
        {"id": "0x0C8", "name": "PGN 200 (0x0C8)", "type": "CAN Frame", "desc": "Engine speed"},
        {"id": "0x0CF", "name": "PGN 207 (0x0CF)", "type": "CAN Frame", "desc": "Engine temperature"},
        {"id": "0x18FEF100", "name": "PGN 65265", "type": "CAN Frame", "desc": "Vehicle speed"},
    ],
}

app = FastAPI(title="OT PdM Intelligence")

if (DIST / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")

ZEROBUS_CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def _get_fernet() -> Fernet:
    global _FERNET
    if _FERNET is not None:
        return _FERNET

    env_key = os.getenv("OT_PDM_SECRET_KEY", "").strip()
    if env_key:
        _FERNET = Fernet(env_key.encode("utf-8"))
        return _FERNET

    if ZEROBUS_KEY_PATH.exists():
        key = ZEROBUS_KEY_PATH.read_text(encoding="utf-8").strip().encode("utf-8")
    else:
        key = Fernet.generate_key()
        ZEROBUS_KEY_PATH.write_text(key.decode("utf-8"), encoding="utf-8")
        os.chmod(ZEROBUS_KEY_PATH, 0o600)
    _FERNET = Fernet(key)
    return _FERNET


def _load_seed(industry: str, seed_name: str) -> list[dict]:
    path = _industry_seed_path(industry, seed_name)
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _live_stream_points(industry: str, max_ticks: int) -> list[dict[str, Any]]:
    target_fqn, _ = _zerobus_action_target(industry)
    limit = max(500, min(30000, max_ticks * 40))
    statement = f"""
    SELECT equipment_id, tag_name, value, quality, timestamp
    FROM {target_fqn}
    WHERE tag_name <> 'operator.recommendation.action'
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    rows = _run_sql(statement, cache_key=f"{target_fqn}:live_stream_points:{max_ticks}")
    rows = [r for r in rows if r.get("tag_name")]
    rows.sort(key=lambda r: (_parse_dt(r.get("timestamp")) or datetime.fromtimestamp(0, tz=timezone.utc)))
    return rows


def _live_sdt_window_metrics(industry: str, points: list[dict[str, Any]], ticks: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    from core.simulator.sdt import SwingingDoorCompressor

    def _to_float(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    if not points:
        return (
            {"industry": industry, "raw_total": 0, "sdt_total": 0, "kept_pct": 0.0, "drop_pct": 0.0},
            [],
        )

    # Build per-tag latest N points and replay SDT locally for a true rolling-window estimate.
    per_tag: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in points:
        key = (str(r.get("equipment_id") or ""), str(r.get("tag_name") or ""))
        per_tag.setdefault(key, []).append(r)
    for key in per_tag:
        per_tag[key] = per_tag[key][-max(1, ticks) :]

    comp = SwingingDoorCompressor(_industry_cfg(industry).get("simulator", {}))
    raw_total = 0
    sdt_total = 0
    tag_raw: dict[str, int] = {}
    tag_sdt: dict[str, int] = {}
    for (equipment_id, tag_name), tag_points in per_tag.items():
        for p in tag_points:
            ts = _parse_dt(p.get("timestamp")) or datetime.now(timezone.utc)
            raw_total += 1
            tag_raw[tag_name] = tag_raw.get(tag_name, 0) + 1
            value = _to_float(p.get("value"))
            quality = str(p.get("quality") or "good")
            emit = comp.should_emit(f"{equipment_id}:{tag_name}", tag_name, value, quality, ts)
            if emit:
                sdt_total += 1
                tag_sdt[tag_name] = tag_sdt.get(tag_name, 0) + 1

    kept_pct = (sdt_total / raw_total * 100.0) if raw_total else 0.0
    drop_pct = 100.0 - kept_pct
    overall = {
        "industry": industry,
        "raw_total": int(raw_total),
        "sdt_total": int(sdt_total),
        "kept_pct": float(kept_pct),
        "drop_pct": float(drop_pct),
    }
    tags: list[dict[str, Any]] = []
    for tag_name, rcount in sorted(tag_raw.items()):
        scount = int(tag_sdt.get(tag_name, 0))
        t_kept = (scount / rcount * 100.0) if rcount else 0.0
        tags.append(
            {
                "industry": industry,
                "tag_name": tag_name,
                "raw_count": int(rcount),
                "sdt_count": scount,
                "kept_pct": float(t_kept),
                "drop_pct": float(100.0 - t_kept),
            }
        )
    tags.sort(key=lambda x: x["drop_pct"], reverse=True)
    return overall, tags


def _report_dir_for_ticks(ticks: int) -> Path:
    return SDT_REPORT_DIR / str(ticks)


def _available_sdt_ticks() -> list[int]:
    ticks: list[int] = []
    if SDT_REPORT_DIR.exists():
        for p in SDT_REPORT_DIR.iterdir():
            if p.is_dir() and p.name.isdigit():
                ticks.append(int(p.name))
    return sorted(ticks)


def _industry_config_path(industry: str) -> Path:
    candidates = [
        ROOT.parent / "industries" / industry / "config.yaml",
        ROOT / "industries" / industry / "config.yaml",
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def _industry_seed_path(industry: str, seed_name: str) -> Path:
    candidates = [
        ROOT.parent / "industries" / industry / "seed" / seed_name,
        ROOT / "industries" / industry / "seed" / seed_name,
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def _default_industry_cfg(industry: str) -> dict[str, Any]:
    defaults = {
        "mining": [
            {"id": "HT-012", "type": "haul_truck", "site": "gudai_darri", "area": "pit_a", "unit": "haul_fleet"},
            {"id": "HT-007", "type": "haul_truck", "site": "gudai_darri", "area": "pit_b", "unit": "haul_fleet"},
            {"id": "HT-001", "type": "haul_truck", "site": "gudai_darri", "area": "pit_a", "unit": "haul_fleet"},
            {"id": "CV-003", "type": "conveyor", "site": "gudai_darri", "area": "process", "unit": "crushing"},
        ],
        "energy": [
            {"id": "WT-004", "type": "wind_turbine", "site": "northhub", "area": "wind", "unit": "gen"},
            {"id": "BESS-01", "type": "bess", "site": "northhub", "area": "storage", "unit": "battery"},
            {"id": "TX-07", "type": "transformer", "site": "northhub", "area": "substation", "unit": "transmission"},
            {"id": "WT-011", "type": "wind_turbine", "site": "northhub", "area": "wind", "unit": "gen"},
        ],
        "water": [
            {"id": "PS-07", "type": "pump", "site": "prospect", "area": "station_7", "unit": "distribution"},
            {"id": "MT-03", "type": "smart_meter", "site": "cbd", "area": "zone_3", "unit": "metering"},
            {"id": "TP-01", "type": "chlorination_unit", "site": "prospect", "area": "treatment", "unit": "dosing"},
            {"id": "VS-11", "type": "vent_shaft", "site": "north", "area": "tunnel", "unit": "ventilation"},
        ],
        "automotive": [
            {"id": "TP-07", "type": "stamping_press", "site": "nagoya", "area": "press", "unit": "line_a"},
            {"id": "WR-14", "type": "robotic_welder", "site": "nagoya", "area": "body", "unit": "line_a"},
            {"id": "CNC-22", "type": "cnc_machine", "site": "nagoya", "area": "machining", "unit": "line_b"},
            {"id": "CV-A3", "type": "assembly_conveyor", "site": "nagoya", "area": "assembly", "unit": "line_c"},
        ],
        "semiconductor": [
            {"id": "ET-04", "type": "etch_tool", "site": "naka_fab", "area": "bay_3", "unit": "etch"},
            {"id": "LT-11", "type": "stepper", "site": "naka_fab", "area": "bay_5", "unit": "lithography"},
            {"id": "CMP-07", "type": "cmp_tool", "site": "naka_fab", "area": "bay_2", "unit": "polish"},
            {"id": "IN-02", "type": "inspection_tool", "site": "naka_fab", "area": "bay_6", "unit": "metrology"},
        ],
    }
    assets = defaults.get(industry, defaults["mining"])
    sensors: dict[str, list[dict[str, Any]]] = {}
    for a in assets:
        sensors.setdefault(
            a["type"],
            [
                {"name": "temperature", "unit": "C", "normal_range": [45, 75], "failure_mode": "degradation"},
                {"name": "vibration_rms", "unit": "mm/s", "normal_range": [1.0, 3.0], "failure_mode": "degradation"},
                {"name": "current", "unit": "A", "normal_range": [10, 50], "failure_mode": "degradation"},
            ],
        )
    return {
        "industry": industry,
        "display_name": industry.title(),
        "catalog": f"pdm_{industry}",
        "simulator": {"protocol": "OPC-UA", "assets": assets},
        "sensors": sensors,
    }


def _asset_ids(industry: str) -> list[str]:
    cfg_path = _industry_config_path(industry)
    if not cfg_path.exists():
        return [a["id"] for a in _default_industry_cfg(industry).get("simulator", {}).get("assets", [])]
    import yaml

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    return [a["id"] for a in cfg.get("simulator", {}).get("assets", [])]


def _asset_token_norm(value: str) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _resolve_asset_alias(industry: str, user_text: str) -> tuple[str | None, str | None]:
    text = str(user_text or "").strip()
    if not text:
        return None, None
    assets = _asset_ids(industry)
    if not assets:
        return None, None

    # Exact match while ignoring case and separators (e.g., "hT 007" -> "HT-007").
    text_norm = _asset_token_norm(text)
    norm_to_asset: dict[str, str] = {_asset_token_norm(a): a for a in assets if a}
    for key, canonical in norm_to_asset.items():
        if key and key in text_norm:
            variants = re.findall(r"\b[A-Za-z]{1,6}[-_\s]?\d{1,6}\b", text)
            matched_variant = next((v for v in variants if _asset_token_norm(v) == key), canonical)
            return canonical, matched_variant
    return None, None


def _industry_cfg(industry: str) -> dict[str, Any]:
    cfg_path = _industry_config_path(industry)
    if not cfg_path.exists():
        return _default_industry_cfg(industry)
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}


def _asset_defs(industry: str) -> list[dict[str, Any]]:
    return _industry_cfg(industry).get("simulator", {}).get("assets", [])


def _sensor_defs(industry: str, asset_type: str) -> list[dict[str, Any]]:
    return _industry_cfg(industry).get("sensors", {}).get(asset_type, [])


def _run_sql(statement: str, cache_key: str | None = None) -> list[dict[str, Any]]:
    now = time.time()
    key = cache_key or statement
    cached = _SQL_CACHE.get(key)
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    if WorkspaceClient is None or sql_service is None:
        return []

    try:
        client = WorkspaceClient()
        resp = client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=_WAREHOUSE_ID,
            wait_timeout="20s",
            disposition=sql_service.Disposition.INLINE,
        )
        status = getattr(resp, "status", None)
        if status is None or status.state != sql_service.StatementState.SUCCEEDED:
            return []
        manifest = getattr(resp, "manifest", None)
        result = getattr(resp, "result", None)
        data_array = getattr(result, "data_array", None) or []
        columns = []
        if manifest and manifest.schema and manifest.schema.columns:
            columns = [c.name for c in manifest.schema.columns]
        rows: list[dict[str, Any]] = []
        for r in data_array:
            if columns:
                rows.append({columns[i]: r[i] for i in range(min(len(columns), len(r)))})
            else:
                rows.append({str(i): v for i, v in enumerate(r)})
        _SQL_CACHE[key] = (now, rows)
        return rows
    except Exception as e:
        print(f"[sql] query failed for warehouse={_WAREHOUSE_ID}: {e}")
        return []


def _predictions_map(industry: str) -> dict[str, dict[str, Any]]:
    _maybe_trigger_live_scoring(industry)
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    statement = f"""
    SELECT equipment_id,
           prediction_timestamp,
           anomaly_score,
           anomaly_label,
           rul_hours,
           predicted_failure_date,
           top_contributing_sensor,
           top_contributing_score,
           model_version_anomaly,
           model_version_rul
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY prediction_timestamp DESC) AS rn
      FROM {catalog}.gold.pdm_predictions
    ) t
    WHERE rn = 1
    """
    rows = _run_sql(statement, cache_key=f"{catalog}:predictions")
    return {r.get("equipment_id"): r for r in rows if r.get("equipment_id")}


def _parse_dt(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _resolve_scoring_job_id(client: WorkspaceClient, industry: str) -> int | None:
    state = _LIVE_SCORING_STATE.setdefault(industry, {})
    cached = state.get("job_id")
    if cached:
        return int(cached)

    env_key = f"OT_PDM_SCORING_JOB_ID_{industry.upper()}"
    env_val = os.getenv(env_key) or os.getenv("OT_PDM_SCORING_JOB_ID")
    if env_val:
        try:
            job_id = int(env_val)
            state["job_id"] = job_id
            return job_id
        except Exception:
            pass

    job_name = f"ot-pdm-scoring-{industry}"
    try:
        listing = client.api_client.do("GET", "/api/2.1/jobs/list", query={"name": job_name, "limit": "100"})
        jobs = listing.get("jobs", []) if isinstance(listing, dict) else []
        for j in jobs:
            settings = j.get("settings", {}) or {}
            if settings.get("name") == job_name and j.get("job_id"):
                state["job_id"] = int(j["job_id"])
                return int(j["job_id"])
    except Exception:
        pass

    # Fallback in case `name` server-side filtering is unavailable.
    try:
        listing = client.api_client.do("GET", "/api/2.1/jobs/list", query={"limit": "100"})
        jobs = listing.get("jobs", []) if isinstance(listing, dict) else []
        for j in jobs:
            settings = j.get("settings", {}) or {}
            if settings.get("name") == job_name and j.get("job_id"):
                state["job_id"] = int(j["job_id"])
                return int(j["job_id"])
    except Exception:
        pass
    return None


def _is_prediction_stale(industry: str) -> bool:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    freshness_stmt = f"""
    SELECT
      (SELECT MAX(timestamp) FROM {catalog}.bronze.sensor_readings) AS latest_stream_ts,
      (SELECT MAX(prediction_timestamp) FROM {catalog}.gold.pdm_predictions) AS latest_pred_ts
    """
    rows = _run_sql(freshness_stmt, cache_key=f"{catalog}:freshness:{int(time.time() // _LIVE_SCORING_FRESH_LOOKBACK_S)}")
    if not rows:
        return False
    row = rows[0]
    latest_stream = _parse_dt(row.get("latest_stream_ts"))
    latest_pred = _parse_dt(row.get("latest_pred_ts"))
    if latest_stream is None:
        return False
    if latest_pred is None:
        return True
    lag_s = (latest_stream - latest_pred).total_seconds()
    return lag_s > _LIVE_SCORING_STALENESS_S


def _maybe_trigger_live_scoring(industry: str) -> None:
    if WorkspaceClient is None:
        return
    if industry not in INDUSTRIES:
        return
    if not _is_prediction_stale(industry):
        return

    state = _LIVE_SCORING_STATE.setdefault(industry, {})
    now = time.time()
    last = float(state.get("last_trigger_s", 0.0) or 0.0)
    if now - last < _LIVE_SCORING_MIN_INTERVAL_S:
        return

    try:
        client = WorkspaceClient()
        job_id = _resolve_scoring_job_id(client, industry)
        if not job_id:
            state["last_error"] = "scoring_job_not_found"
            return
        resp = client.api_client.do("POST", "/api/2.1/jobs/run-now", body={"job_id": int(job_id)})
        state["last_trigger_s"] = now
        if isinstance(resp, dict):
            state["last_run_id"] = resp.get("run_id")
            state["last_number_in_job"] = resp.get("number_in_job")
        # Purge cached predictions for this industry so UI sees updates quickly after job completion.
        catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
        keys = [k for k in _SQL_CACHE.keys() if isinstance(k, str) and k.startswith(f"{catalog}:predictions")]
        for k in keys:
            _SQL_CACHE.pop(k, None)
    except Exception as e:
        state["last_error"] = str(e)


def _sql_escape(v: str) -> str:
    return v.replace("'", "''")


def _persist_simulator_rows(industry: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    if WorkspaceClient is None or sql_service is None:
        return
    target_fqn, _ = _zerobus_action_target(industry)
    client = WorkspaceClient()
    if not _SIM_STAGING_READY.get(industry):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {target_fqn} (
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
        """
        try:
            client.statement_execution.execute_statement(
                statement=ddl,
                warehouse_id=_WAREHOUSE_ID,
                wait_timeout="20s",
                disposition=sql_service.Disposition.INLINE,
            )
        finally:
            _SIM_STAGING_READY[industry] = True

    values_sql: list[str] = []
    for r in rows:
        ts_raw = str(r.get("timestamp", "") or "")
        ts = ts_raw[:19] if len(ts_raw) >= 19 else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        values_sql.append(
            "("
            + ", ".join(
                [
                    f"'{_sql_escape(str(r.get('site_id', '')))}'",
                    f"'{_sql_escape(str(r.get('area_id', '')))}'",
                    f"'{_sql_escape(str(r.get('unit_id', '')))}'",
                    f"'{_sql_escape(str(r.get('equipment_id', '')))}'",
                    "NULL",
                    f"'{_sql_escape(str(r.get('tag_name', '')))}'",
                    str(float(r.get("value", 0.0))),
                    f"'{_sql_escape(str(r.get('unit', '')))}'",
                    f"'{_sql_escape(str(r.get('quality', 'good')))}'",
                    "'0x00'",
                    f"'{_sql_escape(str(r.get('source_protocol', 'OPC-UA')))}'",
                    f"TIMESTAMP '{_sql_escape(ts)}'",
                ]
            )
            + ")"
        )

    chunk = 200
    for i in range(0, len(values_sql), chunk):
        stmt = (
            f"INSERT INTO {target_fqn} "
            "(site_id, area_id, unit_id, equipment_id, component_id, tag_name, value, unit, quality, quality_code, source_protocol, timestamp) VALUES "
            + ", ".join(values_sql[i : i + chunk])
        )
        client.statement_execution.execute_statement(
            statement=stmt,
            warehouse_id=_WAREHOUSE_ID,
            wait_timeout="20s",
            disposition=sql_service.Disposition.INLINE,
        )


def _sensor_features_map(industry: str) -> dict[tuple[str, str], dict[str, Any]]:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    statement = f"""
    SELECT equipment_id, tag_name, mean_15m, stddev_15m, slope_1h, zscore_30d, reading_count, window_end
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY equipment_id, tag_name ORDER BY window_end DESC) AS rn
      FROM {catalog}.silver.sensor_features
    ) t
    WHERE rn = 1
    """
    rows = _run_sql(statement, cache_key=f"{catalog}:sensor_features")
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        eid = r.get("equipment_id")
        tag = r.get("tag_name")
        if eid and tag:
            out[(eid, tag)] = r
    return out


def _parts_rows(industry: str) -> list[dict[str, Any]]:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    statement = f"""
    SELECT part_number, description, quantity, location, depot, unit_cost, currency, reorder_point, lead_time_days, last_updated
    FROM {catalog}.lakebase.parts_inventory
    ORDER BY quantity ASC, part_number
    LIMIT 500
    """
    rows = _run_sql(statement, cache_key=f"{catalog}:parts_inventory")
    return rows if rows else _load_seed(industry, "parts_inventory.json")


def _maintenance_rows(industry: str, equipment_id: str) -> list[dict[str, Any]]:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    statement = f"""
    SELECT equipment_id, shift_label, shift_start, shift_end, planned_downtime_hours, maintenance_window_start, maintenance_window_end, crew_available
    FROM {catalog}.lakebase.maintenance_schedule
    WHERE equipment_id = '{equipment_id}'
    ORDER BY shift_start
    LIMIT 20
    """
    return _run_sql(statement, cache_key=f"{catalog}:maint:{equipment_id}")


def _lakebase_pg_enabled() -> bool:
    has_psycopg = psycopg is not None
    has_psql = shutil.which("psql") is not None
    if not has_psycopg and not has_psql:
        return False
    if _LAKEBASE_PG_DSN:
        return True
    return bool(_LAKEBASE_PG_HOST and _LAKEBASE_PG_DB)


def _lakebase_pg_runtime_user() -> str:
    if _LAKEBASE_PG_USER:
        return _LAKEBASE_PG_USER
    if WorkspaceClient is None:
        return ""
    try:
        user = WorkspaceClient().current_user.me()
        return str(getattr(user, "user_name", "") or "").strip()
    except Exception:
        return ""


def _lakebase_pg_runtime_password() -> str:
    if _LAKEBASE_PG_PASSWORD:
        return _LAKEBASE_PG_PASSWORD
    if WorkspaceClient is None:
        return ""
    try:
        headers: dict[str, str] = {}
        WorkspaceClient().config.authenticate(headers)
        auth = headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[len("Bearer ") :].strip()
    except Exception:
        return ""
    return ""


def _lakebase_pg_conninfo() -> str:
    if _LAKEBASE_PG_DSN:
        return _LAKEBASE_PG_DSN
    user = _lakebase_pg_runtime_user()
    password = _lakebase_pg_runtime_password()
    return (
        f"host={_LAKEBASE_PG_HOST} port={_LAKEBASE_PG_PORT} "
        f"dbname={_LAKEBASE_PG_DB} user={user} "
        f"password={password} sslmode={_LAKEBASE_PG_SSLMODE}"
    )


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lakebase_action_table_sql_name() -> str:
    raw = _LAKEBASE_ACTION_TABLE.strip()
    if not raw:
        return '"otpdm"."operator_recommendation_actions"'
    parts = [p for p in raw.split(".") if p]
    if len(parts) == 1:
        return _quote_ident(parts[0])
    return ".".join(_quote_ident(p) for p in parts[:2])


def _lakebase_action_index_name() -> str:
    base = _LAKEBASE_ACTION_TABLE.replace(".", "_")
    safe = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in base).strip("_")
    return f'idx_{safe}_industry_eid_decided_at'


def _lakebase_psql_exec(sql: str, fetch: bool = False) -> list[tuple[str, ...]]:
    user = _lakebase_pg_runtime_user()
    password = _lakebase_pg_runtime_password()
    if not user or not password:
        raise RuntimeError("Lakebase credentials unavailable for psql fallback")
    conn = f"host={_LAKEBASE_PG_HOST} port={_LAKEBASE_PG_PORT} dbname={_LAKEBASE_PG_DB} user={user} sslmode={_LAKEBASE_PG_SSLMODE}"
    cmd = ["psql", conn, "-v", "ON_ERROR_STOP=1"]
    if fetch:
        cmd.extend(["-A", "-F", "\t", "-t"])
    cmd.extend(["-c", sql])
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    proc = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "psql command failed").strip())
    if not fetch:
        return []
    rows: list[tuple[str, ...]] = []
    for line in (proc.stdout or "").splitlines():
        s = line.strip()
        if not s:
            continue
        rows.append(tuple(s.split("\t")))
    return rows


def _lakebase_ensure_action_table() -> None:
    if not _lakebase_pg_enabled():
        return
    table_name = _lakebase_action_table_sql_name()
    index_name = _lakebase_action_index_name()
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      recommendation_id TEXT PRIMARY KEY,
      industry TEXT NOT NULL,
      equipment_id TEXT NOT NULL,
      decision TEXT NOT NULL,
      decision_note TEXT,
      decided_by TEXT,
      prediction_timestamp TIMESTAMPTZ,
      decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      zerobus_target TEXT,
      zerobus_event_ts TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS {index_name}
      ON {table_name} (industry, equipment_id, decided_at DESC);
    """
    if psycopg is not None:
        with psycopg.connect(_lakebase_pg_conninfo(), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
    else:
        _lakebase_psql_exec(ddl, fetch=False)


def _lakebase_record_action(
    *,
    recommendation_id: str,
    industry: str,
    equipment_id: str,
    decision: str,
    note: str,
    decided_by: str,
    prediction_timestamp: datetime | None,
    zerobus_target: str,
) -> bool:
    if not _lakebase_pg_enabled():
        return False
    _lakebase_ensure_action_table()
    table_name = _lakebase_action_table_sql_name()
    sql = f"""
    INSERT INTO {table_name}
      (recommendation_id, industry, equipment_id, decision, decision_note, decided_by, prediction_timestamp, decided_at, zerobus_target, zerobus_event_ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, NOW())
    ON CONFLICT (recommendation_id) DO NOTHING
    """
    if psycopg is not None:
        with psycopg.connect(_lakebase_pg_conninfo(), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        recommendation_id,
                        industry,
                        equipment_id,
                        decision,
                        note or None,
                        decided_by or "operator",
                        prediction_timestamp,
                        zerobus_target,
                    ),
                )
    else:
        def esc(v: str) -> str:
            return v.replace("'", "''")

        pred_expr = "NULL"
        if prediction_timestamp is not None:
            pred_expr = f"TIMESTAMPTZ '{esc(prediction_timestamp.isoformat())}'"
        insert_sql = f"""
        INSERT INTO {table_name}
          (recommendation_id, industry, equipment_id, decision, decision_note, decided_by, prediction_timestamp, decided_at, zerobus_target, zerobus_event_ts)
        VALUES (
          '{esc(recommendation_id)}',
          '{esc(industry)}',
          '{esc(equipment_id)}',
          '{esc(decision)}',
          {'NULL' if not note else "'" + esc(note) + "'"},
          '{esc(decided_by or "operator")}',
          {pred_expr},
          NOW(),
          '{esc(zerobus_target)}',
          NOW()
        )
        ON CONFLICT (recommendation_id) DO NOTHING
        """
        _lakebase_psql_exec(insert_sql, fetch=False)
    return True


def _lakebase_actioned_assets(industry: str) -> set[str]:
    if not _lakebase_pg_enabled():
        return set()
    _lakebase_ensure_action_table()
    table_name = _lakebase_action_table_sql_name()
    sql = f"""
    SELECT equipment_id
    FROM (
      SELECT equipment_id, decision, decided_at,
             ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY decided_at DESC) AS rn
      FROM {table_name}
      WHERE industry = %s
    ) t
    WHERE rn = 1
      AND lower(decision) IN ('approved', 'rejected', 'deferred')
    """
    out: set[str] = set()
    if psycopg is not None:
        with psycopg.connect(_lakebase_pg_conninfo(), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (industry,))
                for row in cur.fetchall():
                    if row and row[0]:
                        out.add(str(row[0]))
    else:
        safe_industry = industry.replace("'", "''")
        q = f"""
        SELECT equipment_id
        FROM (
          SELECT equipment_id, decision, decided_at,
                 ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY decided_at DESC) AS rn
          FROM {table_name}
          WHERE industry = '{safe_industry}'
        ) t
        WHERE rn = 1
          AND lower(decision) IN ('approved', 'rejected', 'deferred')
        """
        for row in _lakebase_psql_exec(q, fetch=True):
            if row and row[0]:
                out.add(str(row[0]))
    return out


def _normalize_zerobus_protocol(raw: str | None) -> str:
    p = str(raw or "opcua").strip().lower().replace("-", "").replace("_", "")
    if p in {"opcua", "mqtt", "modbus"}:
        return p
    return "opcua"


def _zerobus_action_target(industry: str) -> tuple[str, dict[str, Any]]:
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    protocol = _normalize_zerobus_protocol(cfg.get("simulator", {}).get("protocol"))
    zcfg = _load_zerobus_config(protocol) or {}
    if not zcfg:
        # Fallback to any saved protocol config if current protocol has none.
        for p in ZEROBUS_PROTOCOLS:
            cand = _load_zerobus_config(p)
            if cand:
                zcfg = cand
                break
    target = (zcfg.get("target", {}) or {}) if isinstance(zcfg, dict) else {}
    target_catalog = str(target.get("catalog") or catalog).strip()
    target_schema = str(target.get("schema") or "bronze").strip()
    target_table = str(target.get("table") or "_zerobus_staging").strip()
    return f"{target_catalog}.{target_schema}.{target_table}", zcfg


def _recommendation_actioned_assets(industry: str) -> set[str]:
    # Prefer OLTP Lakebase state when configured.
    try:
        lakebase_rows = _lakebase_actioned_assets(industry)
        if lakebase_rows:
            return lakebase_rows
    except Exception as e:
        print(f"[lakebase] unable to read actioned assets for {industry}: {e}")

    # Fallback to Zerobus stream rows when OLTP is unavailable.
    target_fqn, _ = _zerobus_action_target(industry)
    statement = f"""
    SELECT equipment_id
    FROM (
      SELECT equipment_id, timestamp,
             ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY timestamp DESC) AS rn
      FROM {target_fqn}
      WHERE tag_name = 'operator.recommendation.action'
        AND source_protocol = 'ZEROBUS_OPERATOR_ACTION'
    ) t
    WHERE rn = 1
    """
    rows = _run_sql(statement, cache_key=f"{target_fqn}:recommendation_actions_latest")
    return {str(r.get("equipment_id")) for r in rows if r.get("equipment_id")}


def _bronze_latest(industry: str, limit: int) -> list[dict[str, Any]]:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    statement = f"""
    SELECT site_id, area_id, unit_id, equipment_id, tag_name, value, unit, quality, source_protocol, timestamp
    FROM {catalog}.bronze.sensor_readings
    ORDER BY timestamp DESC
    LIMIT {max(1, min(200, int(limit)))}
    """
    return _run_sql(statement, cache_key=f"{catalog}:bronze:{max(1, min(200, int(limit)))}")

def _normalize_text(value: str) -> str:
    return value.replace("_", " ").title()


def _zerobus_protocol(payload: dict[str, Any]) -> str:
    protocol = str(payload.get("protocol", "opcua") or "opcua").lower().strip()
    if protocol not in ZEROBUS_PROTOCOLS:
        raise HTTPException(status_code=400, detail=f"Unsupported protocol: {protocol}")
    return protocol


def _zerobus_config_path(protocol: str) -> Path:
    return ZEROBUS_CONFIG_DIR / f"{protocol}.json"


def _load_zerobus_config(protocol: str) -> dict[str, Any] | None:
    path = _zerobus_config_path(protocol)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _encrypt_secret(value: str) -> str:
    return _get_fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def _decrypt_secret(value: str) -> str:
    try:
        return _get_fernet().decrypt(value.encode("utf-8")).decode("utf-8")
    except (InvalidToken, ValueError):
        return ""


def _load_genie_room_map() -> dict[str, str]:
    env_map = os.getenv("OT_PDM_GENIE_ROOM_MAP", "").strip()
    if env_map:
        try:
            parsed = json.loads(env_map)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v}
        except Exception:
            pass
    if GENIE_ROOM_MAP_PATH.exists():
        try:
            parsed = json.loads(GENIE_ROOM_MAP_PATH.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v}
        except Exception:
            pass
    return {}


def _genie_extract_text(message: dict[str, Any]) -> str:
    attachments = message.get("attachments") or []
    chunks: list[str] = []
    for att in attachments:
        if not isinstance(att, dict):
            continue
        text_val = att.get("text")
        if isinstance(text_val, str) and text_val.strip():
            chunks.append(text_val.strip())
        elif isinstance(text_val, dict):
            content = text_val.get("content")
            if isinstance(content, str) and content.strip():
                chunks.append(content.strip())
            elif isinstance(content, list):
                joined = "".join([str(x) for x in content if x is not None]).strip()
                if joined:
                    chunks.append(joined)
        elif isinstance(text_val, list):
            joined = "".join([str(x) for x in text_val if x is not None]).strip()
            if joined:
                chunks.append(joined)
    if chunks:
        return "\n\n".join(chunks)
    return ""


def _sanitize_zerobus_config_for_response(cfg: dict[str, Any]) -> dict[str, Any]:
    safe = json.loads(json.dumps(cfg))
    auth = (safe.get("auth", {}) or {})
    has_secret = bool(auth.get("client_secret_encrypted") or auth.get("client_secret"))
    auth.pop("client_secret", None)
    auth.pop("client_secret_encrypted", None)
    auth["has_client_secret"] = has_secret
    safe["auth"] = auth
    return safe


def _asset_rng(industry: str, asset_id: str) -> random.Random:
    return random.Random(f"{industry}:{asset_id}")


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _fmt_money(amount: float, currency: str) -> str:
    whole = int(round(_to_float(amount, 0.0)))
    sign = "-" if whole < 0 else ""
    return f"{currency} {sign}{abs(whole):,}"


def _executive_profile(industry: str, cfg: dict[str, Any]) -> dict[str, Any]:
    default_currency = str(
        cfg.get("agent", {}).get("terminology", {}).get("cost_currency", "USD")
    )
    # Sector-specific ERP + finance assumptions for executive value simulation.
    profiles: dict[str, dict[str, Any]] = {
        "mining": {
            "plant_code": "AU-MIN-01",
            "fiscal_period": "FY2026-P03",
            "cost_centers": ["MIN-EXTR-210", "MIN-MAINT-110", "MIN-CRUSH-320"],
            "work_centers": ["Pit Maintenance", "Mobile Equipment", "Crushing Line"],
            "planner_group": "MIN-PLAN-A",
            "currency": "AUD",
            "downtime_cost_per_hour": 56000.0,
            "quality_cost_per_hour": 6500.0,
            "energy_cost_per_hour": 2400.0,
            "labor_cost_per_hour": 320.0,
            "parts_cost_base": 18500.0,
            "dispatch_cost": 2800.0,
            "platform_cost_monthly_alloc": 42000.0,
        },
        "energy": {
            "plant_code": "AU-ENE-07",
            "fiscal_period": "FY2026-P03",
            "cost_centers": ["ENE-GRID-410", "ENE-OPS-120", "ENE-STOR-230"],
            "work_centers": ["Generation Ops", "Grid Reliability", "BESS Maintenance"],
            "planner_group": "ENE-PLAN-B",
            "currency": "AUD",
            "downtime_cost_per_hour": 47000.0,
            "quality_cost_per_hour": 3900.0,
            "energy_cost_per_hour": 5100.0,
            "labor_cost_per_hour": 300.0,
            "parts_cost_base": 21000.0,
            "dispatch_cost": 2500.0,
            "platform_cost_monthly_alloc": 36000.0,
        },
        "water": {
            "plant_code": "AU-WAT-04",
            "fiscal_period": "FY2026-P03",
            "cost_centers": ["WAT-DIST-180", "WAT-PUMP-220", "WAT-QUAL-090"],
            "work_centers": ["Pump Station Team", "Distribution Ops", "Water Quality"],
            "planner_group": "WAT-PLAN-C",
            "currency": "AUD",
            "downtime_cost_per_hour": 18500.0,
            "quality_cost_per_hour": 4200.0,
            "energy_cost_per_hour": 1300.0,
            "labor_cost_per_hour": 220.0,
            "parts_cost_base": 8200.0,
            "dispatch_cost": 1600.0,
            "platform_cost_monthly_alloc": 18000.0,
        },
        "automotive": {
            "plant_code": "JP-AUTO-12",
            "fiscal_period": "FY2026-M03",
            "cost_centers": ["AUTO-PRESS-510", "AUTO-BODY-540", "AUTO-MACH-530"],
            "work_centers": ["Press Shop", "Body Welding", "CNC Machining"],
            "planner_group": "AUTO-PLAN-A",
            "currency": "JPY",
            "downtime_cost_per_hour": 950000.0,
            "quality_cost_per_hour": 180000.0,
            "energy_cost_per_hour": 42000.0,
            "labor_cost_per_hour": 11000.0,
            "parts_cost_base": 340000.0,
            "dispatch_cost": 70000.0,
            "platform_cost_monthly_alloc": 1200000.0,
        },
        "semiconductor": {
            "plant_code": "JP-SEM-22",
            "fiscal_period": "FY2026-M03",
            "cost_centers": ["SEM-ETCH-710", "SEM-LITHO-730", "SEM-METRO-760"],
            "work_centers": ["Etch Bay", "Lithography Bay", "Yield Engineering"],
            "planner_group": "SEM-PLAN-Z",
            "currency": "USD",
            "downtime_cost_per_hour": 68000.0,
            "quality_cost_per_hour": 24000.0,
            "energy_cost_per_hour": 5200.0,
            "labor_cost_per_hour": 420.0,
            "parts_cost_base": 36000.0,
            "dispatch_cost": 4500.0,
            "platform_cost_monthly_alloc": 56000.0,
        },
    }
    profile = dict(profiles.get(industry, profiles["mining"]))
    profile["currency"] = str(profile.get("currency") or default_currency)
    profile["expedite_parts_multiplier"] = 1.35
    profile["emergency_labor_multiplier"] = 1.4
    profile["planned_parts_ratio"] = 0.45
    return profile


def _executive_work_orders(
    industry: str, assets: list[dict[str, Any]], profile: dict[str, Any]
) -> list[dict[str, Any]]:
    scoped = [a for a in assets if a.get("status") != "healthy"]
    scoped.sort(key=lambda a: _to_float(a.get("anomaly_score"), 0.0), reverse=True)
    center_list = profile.get("cost_centers", [])
    work_centers = profile.get("work_centers", [])
    prefix = industry[:3].upper()
    orders: list[dict[str, Any]] = []
    for idx, asset in enumerate(scoped[:8], start=1):
        anomaly = max(0.05, min(0.98, _to_float(asset.get("anomaly_score"), 0.25)))
        critical = str(asset.get("status")) == "critical"
        repair_h = 8.0 if critical else 5.5
        planned_h = repair_h * (0.58 if critical else 0.66)
        unplanned_h = repair_h * (1.75 if critical else 1.35)
        parts_base = _to_float(profile.get("parts_cost_base"), 0.0) * (1.35 if critical else 1.0)
        labor_rate = _to_float(profile.get("labor_cost_per_hour"), 0.0)
        down_rate = _to_float(profile.get("downtime_cost_per_hour"), 0.0)
        qual_rate = _to_float(profile.get("quality_cost_per_hour"), 0.0)
        energy_rate = _to_float(profile.get("energy_cost_per_hour"), 0.0)
        dispatch = _to_float(profile.get("dispatch_cost"), 0.0)

        avoided_downtime_cost = anomaly * unplanned_h * down_rate
        avoided_quality_cost = anomaly * unplanned_h * qual_rate
        avoided_energy_cost = anomaly * unplanned_h * energy_rate
        expected_failure_cost = (
            avoided_downtime_cost
            + avoided_quality_cost
            + avoided_energy_cost
            + parts_base * _to_float(profile.get("expedite_parts_multiplier"), 1.35)
            + (labor_rate * unplanned_h * _to_float(profile.get("emergency_labor_multiplier"), 1.4))
        )
        intervention_cost = (
            planned_h * labor_rate
            + parts_base * _to_float(profile.get("planned_parts_ratio"), 0.45)
            + dispatch
        )
        net_ebit_impact = max(0.0, expected_failure_cost - intervention_cost)

        orders.append(
            {
                "wo_id": f"{prefix}-WO-{1200 + idx}",
                "equipment_id": str(asset.get("id") or ""),
                "priority": "P1" if critical else "P2",
                "status": "RECOMMENDED",
                "work_center": work_centers[(idx - 1) % max(1, len(work_centers))] if work_centers else "Operations",
                "cost_center": center_list[(idx - 1) % max(1, len(center_list))] if center_list else "OPS-000",
                "planner_group": profile.get("planner_group", "PLAN-DEFAULT"),
                "failure_probability": round(anomaly, 3),
                "expected_failure_cost": round(expected_failure_cost, 2),
                "intervention_cost": round(intervention_cost, 2),
                "net_ebit_impact": round(net_ebit_impact, 2),
                "avoided_downtime_cost": round(avoided_downtime_cost, 2),
                "avoided_quality_cost": round(avoided_quality_cost, 2),
                "avoided_energy_cost": round(avoided_energy_cost, 2),
                "rul_hours": _to_float(asset.get("rul_hours"), 0.0),
            }
        )
    return orders


def _executive_value(industry: str, assets: list[dict[str, Any]]) -> dict[str, Any]:
    cfg = _industry_cfg(industry)
    accounts = cfg.get("accounts", {}) or {}
    profile = _executive_profile(industry, cfg)
    currency = str(profile.get("currency", "USD"))
    work_orders = _executive_work_orders(industry, assets, profile)

    avoided_downtime = sum(_to_float(w.get("avoided_downtime_cost"), 0.0) for w in work_orders)
    avoided_quality = sum(_to_float(w.get("avoided_quality_cost"), 0.0) for w in work_orders)
    avoided_energy = sum(_to_float(w.get("avoided_energy_cost"), 0.0) for w in work_orders)
    intervention_cost = sum(_to_float(w.get("intervention_cost"), 0.0) for w in work_orders)
    platform_cost = _to_float(profile.get("platform_cost_monthly_alloc"), 0.0)
    net_benefit = avoided_downtime + avoided_quality + avoided_energy - intervention_cost - platform_cost
    ebit_saved = max(0.0, net_benefit)
    invested = max(1.0, intervention_cost + platform_cost)
    roi_pct = (ebit_saved / invested) * 100.0
    payback_days = 999.0
    if ebit_saved > 0:
        daily = ebit_saved / 30.0
        payback_days = invested / max(1.0, daily)

    pipeline_monthly = _to_float(accounts.get("pipeline_monthly"), 1.0)
    ebit_margin_bps = (ebit_saved / max(1.0, pipeline_monthly)) * 10000.0

    for w in work_orders:
        w["expected_failure_cost_fmt"] = _fmt_money(_to_float(w["expected_failure_cost"]), currency)
        w["intervention_cost_fmt"] = _fmt_money(_to_float(w["intervention_cost"]), currency)
        w["net_ebit_impact_fmt"] = _fmt_money(_to_float(w["net_ebit_impact"]), currency)

    value_bridge = [
        {"label": "Avoided unplanned downtime", "kind": "positive", "amount": round(avoided_downtime, 2), "amount_fmt": _fmt_money(avoided_downtime, currency)},
        {"label": "Avoided quality and scrap loss", "kind": "positive", "amount": round(avoided_quality, 2), "amount_fmt": _fmt_money(avoided_quality, currency)},
        {"label": "Avoided energy waste", "kind": "positive", "amount": round(avoided_energy, 2), "amount_fmt": _fmt_money(avoided_energy, currency)},
        {"label": "Planned intervention cost", "kind": "negative", "amount": round(-intervention_cost, 2), "amount_fmt": _fmt_money(-intervention_cost, currency)},
        {"label": "Platform and operations allocation", "kind": "negative", "amount": round(-platform_cost, 2), "amount_fmt": _fmt_money(-platform_cost, currency)},
    ]

    return {
        "audience": "finance_executive",
        "window": "last_30_days_simulated",
        "currency": currency,
        "value_statement": f"Impact on EBIT saved through prescriptive maintenance: {_fmt_money(ebit_saved, currency)}",
        "ebit_saved": round(ebit_saved, 2),
        "ebit_saved_fmt": _fmt_money(ebit_saved, currency),
        "net_benefit": round(net_benefit, 2),
        "net_benefit_fmt": _fmt_money(net_benefit, currency),
        "roi_pct": round(roi_pct, 1),
        "payback_days": round(payback_days, 1),
        "ebit_margin_bps": round(ebit_margin_bps, 1),
        "kpis": {
            "avoided_downtime_cost": round(avoided_downtime, 2),
            "avoided_downtime_cost_fmt": _fmt_money(avoided_downtime, currency),
            "avoided_quality_cost": round(avoided_quality, 2),
            "avoided_quality_cost_fmt": _fmt_money(avoided_quality, currency),
            "avoided_energy_cost": round(avoided_energy, 2),
            "avoided_energy_cost_fmt": _fmt_money(avoided_energy, currency),
            "intervention_cost": round(intervention_cost, 2),
            "intervention_cost_fmt": _fmt_money(intervention_cost, currency),
            "platform_cost": round(platform_cost, 2),
            "platform_cost_fmt": _fmt_money(platform_cost, currency),
        },
        "erp": {
            "plant_code": profile.get("plant_code"),
            "fiscal_period": profile.get("fiscal_period"),
            "cost_centers": profile.get("cost_centers", []),
            "work_centers": profile.get("work_centers", []),
            "planner_group": profile.get("planner_group"),
            "reference_account": accounts.get("primary", ""),
        },
        "value_bridge": value_bridge,
        "work_orders": work_orders,
    }


def _asset_snapshot(industry: str, asset_def: dict[str, Any], pred: dict[str, Any] | None = None) -> dict[str, Any]:
    aid = asset_def["id"]
    rng = _asset_rng(industry, aid)
    sev = float(asset_def.get("fault_severity", 0.0))
    if pred and pred.get("anomaly_score") is not None:
        anomaly = round(float(pred.get("anomaly_score")), 2)
    elif sev > 0:
        anomaly = round(max(0.02, min(0.99, sev + rng.uniform(-0.08, 0.06))), 2)
    else:
        anomaly = round(max(0.02, min(0.45, rng.uniform(0.08, 0.28))), 2)
    health = max(5, int((1 - anomaly) * 100))
    status = "critical" if anomaly >= 0.8 else "warning" if anomaly >= 0.5 else "healthy"
    if pred and pred.get("rul_hours") is not None:
        rul = round(float(pred.get("rul_hours")), 1)
    else:
        rul = round(max(4.0, (220.0 * (1 - anomaly)) + rng.uniform(-16, 12)), 1)
    inject_fault = asset_def.get("inject_fault")
    fm = _industry_cfg(industry).get("failure_modes", {}).get(inject_fault or "", {})
    base_cost = int(fm.get("cost_per_event", rng.uniform(8_000, 95_000)))
    exposure = int(base_cost * max(0.3, anomaly))
    currency = _industry_cfg(industry).get("agent", {}).get("terminology", {}).get("cost_currency", "USD")
    return {
        "id": aid,
        "equipment_id": aid,
        "type": _normalize_text(asset_def.get("type", "asset")),
        "model": asset_def.get("model", ""),
        "site": _normalize_text(asset_def.get("site", "site_1")),
        "area": _normalize_text(asset_def.get("area", "area_1")),
        "unit": _normalize_text(asset_def.get("unit", "unit_1")),
        "crumb": f"{_normalize_text(asset_def.get('site', 'site_1'))} > {_normalize_text(asset_def.get('area', 'area_1'))} > {_normalize_text(asset_def.get('unit', 'unit_1'))} > {aid}",
        "status": status,
        "anomaly_score": anomaly,
        "health_score_pct": health,
        "rul_hours": rul,
        "fault_mode": inject_fault or "none",
        "cost_exposure": f"{currency} {exposure:,}",
        "model_version_anomaly": (pred or {}).get("model_version_anomaly"),
        "model_version_rul": (pred or {}).get("model_version_rul"),
        "prediction_timestamp": (pred or {}).get("prediction_timestamp"),
        "top_contributing_sensor": (pred or {}).get("top_contributing_sensor"),
        "top_contributing_score": (pred or {}).get("top_contributing_score"),
    }


def _overview(industry: str) -> dict[str, Any]:
    predictions = _predictions_map(industry)
    rows = [_asset_snapshot(industry, a, predictions.get(a["id"])) for a in _asset_defs(industry)]
    actioned_assets = _recommendation_actioned_assets(industry)
    if not rows:
        return {
            "assets": [],
            "actioned_assets": [],
            "kpis": {"fleet_health_score": 0, "critical_assets": 0, "asset_count": 0},
            "executive": _executive_value(industry, []),
        }
    avg_health = round(sum(a["health_score_pct"] for a in rows) / len(rows), 1)
    critical = [a for a in rows if a["status"] == "critical"]
    warning = [a for a in rows if a["status"] == "warning"]
    return {
        "assets": rows,
        "actioned_assets": sorted(actioned_assets),
        "kpis": {
            "fleet_health_score": avg_health,
            "critical_assets": len(critical),
            "asset_count": len(rows),
            "avoided_cost": sum(int(str(a["cost_exposure"]).split()[-1].replace(",", "")) for a in warning + critical),
        },
        "alerts": [
            {"severity": "critical", "text": f"{a['id']} requires immediate intervention ({a['fault_mode']})", "time": "now"}
            for a in critical[:3]
        ]
        + [
            {"severity": "warning", "text": f"{a['id']} should be scheduled this week ({a['fault_mode']})", "time": "recent"}
            for a in warning[:3]
        ],
        "messages": [
            {
                "role": "agent",
                "label": "Maintenance Supervisor AI",
                "text": "I can triage top-risk equipment, parts readiness, and recommended actions.",
            }
        ],
        "executive": _executive_value(industry, rows),
    }


def _hierarchy(industry: str) -> dict[str, Any]:
    assets = _overview(industry)["assets"]
    cfg = _industry_cfg(industry)
    display_name = cfg.get("display_name", f"{industry.title()} Fleet")
    root: dict[str, Any] = {
        "label": display_name,
        "level": "site",
        "icon": ISA_EMOJI["site"],
        "health": 100,
        "children": [],
    }
    site_map: dict[str, dict[str, Any]] = {}
    for asset in assets:
        site_key = asset["site"]
        area_key = asset["area"]
        unit_key = asset["unit"]
        if site_key not in site_map:
            site_node = {
                "label": site_key,
                "level": "site",
                "icon": ISA_EMOJI["site"],
                "children": [],
                "_scores": [],
            }
            site_map[site_key] = site_node
            root["children"].append(site_node)
        site_node = site_map[site_key]
        area_node = next((n for n in site_node["children"] if n["label"] == area_key), None)
        if area_node is None:
            area_node = {
                "label": area_key,
                "level": "area",
                "icon": ISA_EMOJI["area"],
                "children": [],
                "_scores": [],
            }
            site_node["children"].append(area_node)
        unit_node = next((n for n in area_node["children"] if n["label"] == unit_key), None)
        if unit_node is None:
            unit_node = {
                "label": unit_key,
                "level": "unit",
                "icon": ISA_EMOJI["unit"],
                "children": [],
                "_scores": [],
            }
            area_node["children"].append(unit_node)
        equipment_node = {
            "label": asset["id"],
            "level": "equipment",
            "icon": ISA_EMOJI["equipment"],
            "asset_id": asset["id"],
            "health": asset["health_score_pct"],
            "children": [],
            "_scores": [asset["health_score_pct"]],
        }
        unit_node["children"].append(equipment_node)
        unit_node["_scores"].append(asset["health_score_pct"])
        area_node["_scores"].append(asset["health_score_pct"])
        site_node["_scores"].append(asset["health_score_pct"])
        root.setdefault("_scores", []).append(asset["health_score_pct"])

    def _finalize(node: dict[str, Any]) -> None:
        scores = node.pop("_scores", [])
        node["health"] = int(round(sum(scores) / len(scores))) if scores else 100
        for child in node.get("children", []):
            _finalize(child)

    _finalize(root)
    return root


def _asset_detail(industry: str, asset_id: str) -> dict[str, Any]:
    asset_def = next((a for a in _asset_defs(industry) if a["id"] == asset_id), None)
    if asset_def is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    predictions = _predictions_map(industry)
    snapshot = _asset_snapshot(industry, asset_def, predictions.get(asset_id))
    sensors = _sensor_defs(industry, asset_def.get("type", ""))
    sensor_features = _sensor_features_map(industry)
    srng = _asset_rng(industry, asset_id)
    fault_mode = asset_def.get("inject_fault")
    fault_sev = float(asset_def.get("fault_severity", 0.0))
    sensor_rows = []
    for sensor in sensors[:6]:
        low, high = sensor.get("normal_range", [10, 100])
        warn = sensor.get("warning_threshold")
        crit = sensor.get("critical_threshold")
        feat = sensor_features.get((asset_id, sensor.get("name", "")))
        if feat and feat.get("mean_15m") is not None:
            val = float(feat.get("mean_15m"))
        else:
            val = srng.uniform(low, high)
        if fault_sev > 0 and fault_mode and sensor.get("failure_mode") == fault_mode:
            direction = sensor.get("dir", 1)
            span = abs((high - low) if high is not None and low is not None else max(1.0, abs(val)))
            shift = span * (0.35 + 0.75 * fault_sev)
            val = val + shift if direction >= 0 else val - shift
        val = round(val, 2)
        status = "healthy"
        if crit is not None and ((sensor.get("dir", 1) == 1 and val >= crit) or (sensor.get("dir", 1) == -1 and val <= crit)):
            status = "critical"
        elif warn is not None and ((sensor.get("dir", 1) == 1 and val >= warn) or (sensor.get("dir", 1) == -1 and val <= warn)):
            status = "warning"
        if feat and feat.get("stddev_15m") is not None:
            sigma = max(0.01, float(feat.get("stddev_15m") or 0.0))
            history = [round(max(0.0, val + srng.uniform(-sigma * 1.5, sigma * 1.5)), 2) for _ in range(18)]
        else:
            history = [round(max(0.0, val + srng.uniform(-2.5, 2.5)), 2) for _ in range(18)]
        sensor_rows.append(
            {
                "name": sensor["name"],
                "label": sensor.get("display", sensor["name"]),
                "unit": sensor.get("unit", ""),
                "value": val,
                "status": status,
                "trend": "rising" if history[-1] >= history[0] else "stable",
                "history": history,
            }
        )
    base = snapshot["anomaly_score"]
    anomaly_history = [round(max(0.01, min(0.99, base + srng.uniform(-0.08, 0.08) + ((i - 6) * 0.01))), 2) for i in range(12)]
    return {
        **snapshot,
        "sensors": sensor_rows,
        "anomaly_history": anomaly_history,
        "parts": _parts_rows(industry),
        "maintenance_schedule": _maintenance_rows(industry, asset_id),
    }


def _sim_state(industry: str) -> dict[str, Any]:
    if industry not in SIM_STATE:
        cfg = _industry_cfg(industry)
        assets = cfg.get("simulator", {}).get("assets", [])
        SIM_STATE[industry] = {
            "running": False,
            "reading_count": 0,
            "tick_interval_ms": int(cfg.get("simulator", {}).get("tick_interval_ms", 800)),
            "noise_factor": float(cfg.get("simulator", {}).get("noise_factor", 0.02)),
            "faults": {
                a["id"]: {
                    "enabled": float(a.get("fault_severity", 0.0)) > 0,
                    "severity": int(float(a.get("fault_severity", 0.0)) * 100),
                    "mode": a.get("inject_fault", "degradation"),
                }
                for a in assets
            },
            "recent_rows": [],
        }
    return SIM_STATE[industry]


def _sim_flow_stage(table_fqn: str, stage_name: str, limit: int, tier: str = "bronze") -> dict[str, Any]:
    max_rows = max(5, min(120, int(limit)))
    now_bucket = int(time.time() // 2)
    latest_stmt = f"""
    SELECT timestamp, equipment_id, tag_name, value, unit, quality, source_protocol
    FROM {table_fqn}
    ORDER BY timestamp DESC
    LIMIT {max_rows}
    """
    rows = _run_sql(latest_stmt, cache_key=f"{table_fqn}:{stage_name}:latest:{max_rows}:{now_bucket}")

    count_stmt = f"""
    SELECT
      SUM(CASE WHEN timestamp >= current_timestamp() - INTERVAL 30 MINUTES THEN 1 ELSE 0 END) AS rows_30m,
      SUM(CASE WHEN timestamp >= current_timestamp() - INTERVAL 5 MINUTES THEN 1 ELSE 0 END) AS rows_5m,
      SUM(CASE
            WHEN timestamp >= current_timestamp() - INTERVAL 10 MINUTES
             AND timestamp < current_timestamp() - INTERVAL 5 MINUTES THEN 1
            ELSE 0
          END) AS rows_prev_5m,
      MAX(timestamp) AS latest_ts
    FROM {table_fqn}
    """
    cnt = _run_sql(count_stmt, cache_key=f"{table_fqn}:{stage_name}:count:{now_bucket}")
    rows_30m = 0
    rows_5m = 0
    rows_prev_5m = 0
    latest_ts = None
    if cnt:
        rows_30m = int(cnt[0].get("rows_30m") or 0)
        rows_5m = int(cnt[0].get("rows_5m") or 0)
        rows_prev_5m = int(cnt[0].get("rows_prev_5m") or 0)
        latest_ts = cnt[0].get("latest_ts")
    elif rows:
        latest_ts = rows[0].get("timestamp")
    if rows_prev_5m <= 0:
        rate_change_pct = 100.0 if rows_5m > 0 else 0.0
    else:
        rate_change_pct = ((rows_5m - rows_prev_5m) / rows_prev_5m) * 100.0

    return {
        "stage": stage_name,
        "tier": tier,
        "table": table_fqn,
        "rows_30m": rows_30m,
        "rows_5m": rows_5m,
        "rows_prev_5m": rows_prev_5m,
        "rate_change_pct": round(rate_change_pct, 1),
        "latest_ts": latest_ts,
        "rows": rows,
    }


def _sim_custom_stage(
    table_fqn: str,
    stage_name: str,
    limit: int,
    latest_stmt: str,
    count_stmt: str,
    ts_field: str = "timestamp",
    tier: str = "silver",
) -> dict[str, Any]:
    max_rows = max(5, min(120, int(limit)))
    now_bucket = int(time.time() // 2)
    rows = _run_sql(latest_stmt.format(limit=max_rows), cache_key=f"{table_fqn}:{stage_name}:latest:{max_rows}:{now_bucket}")
    cnt = _run_sql(count_stmt, cache_key=f"{table_fqn}:{stage_name}:count:{now_bucket}")
    rows_30m = 0
    rows_5m = 0
    rows_prev_5m = 0
    latest_ts = None
    if cnt:
        rows_30m = int(cnt[0].get("rows_30m") or 0)
        rows_5m = int(cnt[0].get("rows_5m") or 0)
        rows_prev_5m = int(cnt[0].get("rows_prev_5m") or 0)
        latest_ts = cnt[0].get("latest_ts")
    elif rows:
        latest_ts = rows[0].get(ts_field)
    if rows_prev_5m <= 0:
        rate_change_pct = 100.0 if rows_5m > 0 else 0.0
    else:
        rate_change_pct = ((rows_5m - rows_prev_5m) / rows_prev_5m) * 100.0
    return {
        "stage": stage_name,
        "tier": tier,
        "table": table_fqn,
        "rows_30m": rows_30m,
        "rows_5m": rows_5m,
        "rows_prev_5m": rows_prev_5m,
        "rate_change_pct": round(rate_change_pct, 1),
        "latest_ts": latest_ts,
        "rows": rows,
    }


def _sim_silver_stage(table_fqn: str, limit: int) -> dict[str, Any]:
    latest_stmt = f"""
    SELECT timestamp, equipment_id, tag_name,
           mean_15m AS value,
           quality,
           'SILVER' AS source_protocol
    FROM {table_fqn}
    ORDER BY timestamp DESC
    LIMIT {{limit}}
    """
    count_stmt = f"""
    SELECT
      SUM(CASE WHEN timestamp >= current_timestamp() - INTERVAL 30 MINUTES THEN 1 ELSE 0 END) AS rows_30m,
      SUM(CASE WHEN timestamp >= current_timestamp() - INTERVAL 5 MINUTES THEN 1 ELSE 0 END) AS rows_5m,
      SUM(CASE
            WHEN timestamp >= current_timestamp() - INTERVAL 10 MINUTES
             AND timestamp < current_timestamp() - INTERVAL 5 MINUTES THEN 1
            ELSE 0
          END) AS rows_prev_5m,
      MAX(timestamp) AS latest_ts
    FROM {table_fqn}
    """
    return _sim_custom_stage(table_fqn, "silver_features", limit, latest_stmt, count_stmt, "timestamp", "silver")


def _sim_gold_stage(table_fqn: str, limit: int) -> dict[str, Any]:
    latest_stmt = f"""
    SELECT prediction_timestamp AS timestamp,
           equipment_id,
           'anomaly_score' AS tag_name,
           anomaly_score AS value,
           anomaly_label AS quality,
           'GOLD' AS source_protocol
    FROM {table_fqn}
    ORDER BY prediction_timestamp DESC
    LIMIT {{limit}}
    """
    count_stmt = f"""
    SELECT
      SUM(CASE WHEN prediction_timestamp >= current_timestamp() - INTERVAL 30 MINUTES THEN 1 ELSE 0 END) AS rows_30m,
      SUM(CASE WHEN prediction_timestamp >= current_timestamp() - INTERVAL 5 MINUTES THEN 1 ELSE 0 END) AS rows_5m,
      SUM(CASE
            WHEN prediction_timestamp >= current_timestamp() - INTERVAL 10 MINUTES
             AND prediction_timestamp < current_timestamp() - INTERVAL 5 MINUTES THEN 1
            ELSE 0
          END) AS rows_prev_5m,
      MAX(prediction_timestamp) AS latest_ts
    FROM {table_fqn}
    """
    return _sim_custom_stage(table_fqn, "gold_predictions", limit, latest_stmt, count_stmt, "timestamp", "gold")


def _default_cost_unit(industry: str) -> str:
    return {
        "mining": "AUD per TOR event",
        "energy": "AUD per MWh curtailed",
        "water": "AUD per ML leakage",
        "automotive": "JPY per production hour",
        "semiconductor": "USD per wafer lot at risk",
    }.get(industry, "USD per event")


def _default_tz(industry: str) -> str:
    return {"mining": "AWST", "energy": "AEST", "water": "AEST", "automotive": "JST", "semiconductor": "JST"}.get(industry, "UTC")


def _default_asset_noun(industry: str) -> str:
    return {
        "mining": "haul truck",
        "energy": "grid asset",
        "water": "pump",
        "automotive": "machine",
        "semiconductor": "fab tool",
    }.get(industry, "asset")


def _default_downtime(industry: str) -> str:
    return {
        "mining": "TOR (Truck Off Road)",
        "energy": "generation curtailment",
        "water": "pump outage",
        "automotive": "line stoppage",
        "semiconductor": "lot hold",
    }.get(industry, "downtime")


def _yaml_from_payload(payload: dict[str, Any]) -> str:
    connector = payload.get("connector", {}) or {}
    fqn = str(connector.get("target_fqn", "") or "").split(".")
    if len([p for p in fqn if p.strip()]) == 3:
        target_catalog, target_schema, target_table = [p.strip() for p in fqn]
    else:
        target_catalog = connector.get("target_catalog") or payload.get("catalog", "")
        target_schema = connector.get("target_schema", "bronze")
        target_table = connector.get("target_table", "_zerobus_staging")
    cfg = {
        "industry": payload.get("industry_key", ""),
        "display_name": payload.get("display_name", ""),
        "catalog": target_catalog,
        "isa95_hierarchy": {"levels": [{"key": l.lower(), "display": l} for l in payload.get("isa_levels", []) if l]},
        "simulator": {
            "protocol": payload.get("protocol", "OPC-UA"),
            "assets": [],
        },
        "zerobus": {
            "endpoint": connector.get("zerobus_endpoint", ""),
            "workspace_host": connector.get("workspace_url", connector.get("workspace_host", "")),
            "source": {
                "protocol": connector.get("protocol", "opcua"),
                "endpoint": connector.get("endpoint", ""),
                "oauth": {
                    "client_id": connector.get("oauth_client_id", ""),
                    # Never emit plaintext secrets in generated previews.
                    "client_secret": "${stored_in_secret_manager}",
                },
            },
            "target": {
                "catalog": target_catalog,
                "schema": target_schema,
                "table": target_table,
            },
        },
        "agent": {
            "persona": payload.get("persona", ""),
            "terminology": {
                "asset": payload.get("asset_noun", ""),
                "downtime_event": payload.get("downtime_event", ""),
                "cost_unit": payload.get("cost_unit", ""),
                "site_timezone": payload.get("timezone", ""),
            },
        },
    }
    for asset in payload.get("assets", []):
        entry: dict[str, Any] = {
            "id": asset.get("id", ""),
            "type": (asset.get("type", "equipment") or "equipment").lower().replace(" ", "_"),
        }
        path_parts = [p.strip() for p in (asset.get("path", "") or "").split("/") if p.strip()]
        if len(path_parts) > 0:
            entry["site"] = path_parts[0]
        if len(path_parts) > 1:
            entry["area"] = path_parts[1]
        if len(path_parts) > 2:
            entry["unit"] = path_parts[2]
        sensors = []
        for sensor in asset.get("sensors", []):
            s = {"name": sensor.get("name", "sensor"), "unit": sensor.get("unit", "")}
            if sensor.get("warn"):
                s["warning_threshold"] = sensor["warn"]
            if sensor.get("crit"):
                s["critical_threshold"] = sensor["crit"]
            sensors.append(s)
        if sensors:
            entry["sensors"] = sensors
        cfg["simulator"]["assets"].append(entry)
    return yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/fleet/assets")
def fleet_assets(industry: str = "mining") -> list[dict]:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    return [
        {
            "equipment_id": a["equipment_id"],
            "anomaly_score": a["anomaly_score"],
            "anomaly_label": "anomaly" if a["anomaly_score"] >= 0.5 else "normal",
            "rul_hours": a["rul_hours"],
            "health_score_pct": a["health_score_pct"],
        }
        for a in _overview(industry)["assets"]
    ]


@app.get("/api/fleet/kpis")
def fleet_kpis(industry: str = "mining") -> dict:
    assets = fleet_assets(industry)
    if not assets:
        return {"fleet_health_score": 0.0, "critical_assets": 0, "asset_count": 0}
    avg_health = round(sum(a["health_score_pct"] for a in assets) / len(assets), 1)
    return {
        "fleet_health_score": avg_health,
        "critical_assets": sum(1 for a in assets if a["anomaly_score"] >= 0.8),
        "asset_count": len(assets),
    }


@app.get("/api/asset/{asset_id}/prediction")
def asset_prediction(asset_id: str, industry: str = "mining") -> dict:
    assets = {a["equipment_id"]: a for a in fleet_assets(industry)}
    if asset_id not in assets:
        raise HTTPException(status_code=404, detail="Asset not found")
    return assets[asset_id]


@app.get("/api/asset/{asset_id}/sensors")
def asset_sensors(asset_id: str, hours: int = 8, industry: str = "mining") -> dict:
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(min(hours, 24) * 3):
        rows.append(
            {
                "timestamp": (now - timedelta(minutes=20 * i)).isoformat(),
                "sensor": f"sensor_{(i % 6) + 1}",
                "value": round(random.uniform(1.0, 100.0), 2),
                "quality": random.choice(["good", "good", "good", "uncertain", "bad"]),
            }
        )
    return {"equipment_id": asset_id, "rows": rows}


@app.get("/api/asset/{asset_id}/anomaly_history")
def anomaly_history(asset_id: str, hours: int = 72, industry: str = "mining") -> dict:
    points = max(8, min(72, hours))
    data = [round(random.uniform(0.05, 0.95), 2) for _ in range(points)]
    return {"equipment_id": asset_id, "hours": hours, "scores": data}


@app.get("/api/asset/{asset_id}/feature_importance")
def feature_importance(asset_id: str, industry: str = "mining") -> dict:
    features = [
        {"name": "vibration_rms__zscore_30d", "score": 0.91},
        {"name": "engine_egt__slope_1h", "score": 0.72},
        {"name": "coolant_temp__mean_15m", "score": 0.61},
        {"name": "oil_temp__stddev_15m", "score": 0.43},
    ]
    return {"equipment_id": asset_id, "features": features}


@app.get("/api/parts/{asset_id}")
def parts(asset_id: str, industry: str = "mining") -> dict:
    rows = _parts_rows(industry)
    return {"equipment_id": asset_id, "parts": rows}


@app.get("/api/ui/overview")
def ui_overview(industry: str = "mining") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    return _overview(industry)


@app.post("/api/ui/recommendation/action")
def ui_recommendation_action(payload: dict) -> dict:
    industry = str(payload.get("industry", "mining") or "mining").lower()
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    equipment_id = str(payload.get("equipment_id", "") or "").strip()
    if not equipment_id:
        raise HTTPException(status_code=400, detail="Missing equipment_id")
    decision_raw = str(payload.get("decision", "") or "").strip().lower()
    decision_map = {
        "approve": "approved",
        "approved": "approved",
        "reject": "rejected",
        "rejected": "rejected",
        "defer": "deferred",
        "deferred": "deferred",
    }
    if decision_raw not in decision_map:
        raise HTTPException(status_code=400, detail="decision must be approve, reject, or defer")
    decision = decision_map[decision_raw]
    note = str(payload.get("note", "") or "").strip()
    target_fqn, zcfg = _zerobus_action_target(industry)
    rec_id = f"{industry}:{equipment_id}:{int(time.time())}"
    pred = _predictions_map(industry).get(equipment_id) or {}
    pred_ts = str(pred.get("prediction_timestamp") or "")

    decision_value = {"approved": 1.0, "rejected": -1.0, "deferred": 0.0}.get(decision, 0.0)
    meta = {
        "recommendation_id": rec_id,
        "decision": decision,
        "note": note,
        "industry": industry,
        "prediction_timestamp": pred_ts or None,
        "decided_by": "operator",
        "zerobus_endpoint": str((zcfg or {}).get("zerobus_endpoint") or ""),
    }
    meta_s = json.dumps(meta, separators=(",", ":")).replace("'", "''")
    safe_eq = equipment_id.replace("'", "''")
    statement = f"""
    INSERT INTO {target_fqn}
    (site_id, area_id, unit_id, equipment_id, component_id, tag_name, value, unit, quality, quality_code, source_protocol, timestamp)
    VALUES (
      'ops_console',
      'maintenance',
      'recommendations',
      '{safe_eq}',
      '{meta_s}',
      'operator.recommendation.action',
      {decision_value},
      'decision',
      'good',
      '0x00',
      'ZEROBUS_OPERATOR_ACTION',
      current_timestamp()
    )
    """
    if WorkspaceClient is None or sql_service is None:
        raise HTTPException(status_code=500, detail="Databricks SQL client unavailable for Zerobus action stream")
    try:
        client = WorkspaceClient()
        resp = client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=_WAREHOUSE_ID,
            wait_timeout="20s",
            disposition=sql_service.Disposition.INLINE,
        )
        status = getattr(resp, "status", None)
        if status is None or status.state != sql_service.StatementState.SUCCEEDED:
            raise RuntimeError("stream_insert_failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stream operator decision to Zerobus target: {e}")
    lakebase_written = False
    try:
        lakebase_written = _lakebase_record_action(
            recommendation_id=rec_id,
            industry=industry,
            equipment_id=equipment_id,
            decision=decision,
            note=note,
            decided_by="operator",
            prediction_timestamp=_parse_dt(pred_ts),
            zerobus_target=target_fqn,
        )
    except Exception as e:
        # Operator action must still succeed as long as Zerobus stream succeeds.
        print(f"[lakebase] unable to persist operator action for {industry}/{equipment_id}: {e}")
    _SQL_CACHE.pop(f"{target_fqn}:recommendation_actions_latest", None)
    return {
        "ok": True,
        "industry": industry,
        "equipment_id": equipment_id,
        "decision": decision,
        "target": target_fqn,
        "lakebase_oltp": lakebase_written,
    }


@app.get("/api/ui/hierarchy")
def ui_hierarchy(industry: str = "mining") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    return _hierarchy(industry)


@app.get("/api/ui/asset/{asset_id}")
def ui_asset(asset_id: str, industry: str = "mining") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    return _asset_detail(industry, asset_id)


@app.get("/api/ui/model/{asset_id}")
def ui_model(asset_id: str, industry: str = "mining") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    detail = _asset_detail(industry, asset_id)
    sensor_features = _sensor_features_map(industry)
    feat = []
    for sensor in detail.get("sensors", []):
        row = sensor_features.get((asset_id, sensor.get("name", "")))
        if not row:
            continue
        score = abs(float(row.get("zscore_30d") or 0.0))
        if score <= 0:
            score = abs(float(row.get("slope_1h") or 0.0))
        feat.append({"name": sensor.get("name", "feature"), "score": round(min(0.99, max(0.05, score / 10.0)), 2)})
    if not feat:
        feat = [{"name": s.get("name", "feature"), "score": round(0.4 + (i * 0.1), 2)} for i, s in enumerate(detail.get("sensors", [])[:4])]
    return {
        "asset_id": asset_id,
        "health_score_pct": detail["health_score_pct"],
        "rul_hours": detail["rul_hours"],
        "model_meta": {
            "trained": str(detail.get("prediction_timestamp") or "n/a"),
            "r2": "from_mlflow_tracking",
            "rmse": "from_mlflow_tracking",
            "protocol": _industry_cfg(industry).get("simulator", {}).get("protocol", "OPC-UA"),
            "model_version_anomaly": detail.get("model_version_anomaly"),
            "model_version_rul": detail.get("model_version_rul"),
        },
        "rul_curve": {
            "labels": ["T-7d", "T-3d", "T-1d", "Now"],
            "values": [round(detail["rul_hours"] * m, 1) for m in (1.8, 1.4, 1.15, 1.0)],
        },
        "feature_importance": sorted(feat, key=lambda x: x["score"], reverse=True)[:4],
        "anomaly_decomposition": [
            {"name": f["name"], "score": round(f["score"] / 3, 2)}
            for f in sorted(feat, key=lambda x: x["score"], reverse=True)[:4]
        ],
    }


@app.get("/api/ui/simulator/state")
def ui_simulator_state(industry: str = "mining") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    st = _sim_state(industry)
    asset_sensors: dict[str, list[dict[str, Any]]] = {}
    for a in _asset_defs(industry):
        asset_sensors[a["id"]] = [
            {
                "name": s.get("name", "sensor"),
                "failure_mode": s.get("failure_mode"),
                "warn": s.get("warning_threshold"),
                "crit": s.get("critical_threshold"),
            }
            for s in _sensor_defs(industry, a.get("type", ""))[:10]
        ]
    return {
        "running": st["running"],
        "reading_count": st["reading_count"],
        "tick_interval_ms": st["tick_interval_ms"],
        "noise_factor": st["noise_factor"],
        "faults": st["faults"],
        "rows": st["recent_rows"],
        "assets": _asset_defs(industry),
        "asset_sensors": asset_sensors,
        "catalog": _industry_cfg(industry).get("catalog", f"pdm_{industry}"),
    }


@app.get("/api/ui/simulator/flow")
def ui_simulator_flow(industry: str = "mining", limit: int = 30) -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    bronze_fqn = f"{catalog}.bronze.sensor_readings"
    features_fqn = f"{catalog}.bronze.sensor_features"
    predictions_fqn = f"{catalog}.bronze.pdm_predictions"
    bronze = _sim_flow_stage(bronze_fqn, "bronze_curated", limit, "bronze")
    silver = _sim_silver_stage(features_fqn, limit)
    gold = _sim_gold_stage(predictions_fqn, limit)
    return {
        "industry": industry,
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "stages": [bronze, silver, gold],
    }


@app.post("/api/ui/simulator/control")
def ui_simulator_control(payload: dict) -> dict:
    industry = payload.get("industry", "mining")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    action = payload.get("action", "start")
    st = _sim_state(industry)
    st["running"] = action == "start"
    if "tick_interval_ms" in payload:
        st["tick_interval_ms"] = int(payload["tick_interval_ms"])
    if "noise_factor" in payload:
        st["noise_factor"] = float(payload["noise_factor"])
    return {"running": st["running"], "tick_interval_ms": st["tick_interval_ms"], "noise_factor": st["noise_factor"]}


@app.post("/api/ui/simulator/tick")
def ui_simulator_tick(payload: dict) -> dict:
    industry = payload.get("industry", "mining")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    st = _sim_state(industry)
    if not st["running"]:
        return {"rows": st["recent_rows"], "reading_count": st["reading_count"], "running": False}
    cfg = _industry_cfg(industry)
    protocol = cfg.get("simulator", {}).get("protocol", "OPC-UA")
    rows: list[dict[str, Any]] = []
    for asset in _asset_defs(industry):
        aid = asset["id"]
        fault_cfg = st.get("faults", {}).get(aid, {"enabled": False, "severity": 0, "mode": "degradation"})
        sensors = _sensor_defs(industry, asset.get("type", ""))[:3]
        for sensor in sensors:
            low, high = sensor.get("normal_range", [10, 100])
            v = random.uniform(low, high)
            if fault_cfg.get("enabled"):
                mode = fault_cfg.get("mode")
                sev = max(0, min(100, int(fault_cfg.get("severity", 0)))) / 100.0
                if mode == sensor.get("failure_mode") or mode in ("degradation", "", None):
                    direction = sensor.get("dir", 1)
                    v = v * (1 + (0.25 * sev * direction))
                    if direction < 0:
                        v = v * (1 - 0.25 * sev)
            rows.append(
                {
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "site_id": asset.get("site", "site_1"),
                    "area_id": asset.get("area", "area_1"),
                    "unit_id": asset.get("unit", "unit_1"),
                    "equipment_id": aid,
                    "tag_name": sensor.get("name", "sensor"),
                    "value": round(v, 2),
                    "unit": sensor.get("unit", "u"),
                    "quality": (
                        "bad"
                        if fault_cfg.get("enabled") and random.random() < 0.15 + (fault_cfg.get("severity", 0) / 500)
                        else random.choice(["good", "good", "uncertain"])
                    ),
                    "source_protocol": protocol,
                }
            )
    st["reading_count"] += len(rows)
    st["recent_rows"] = (rows + st["recent_rows"])[:120]
    # Persist simulator ticks to configured Zerobus target table (Zerobus-only ingest path).
    try:
        _persist_simulator_rows(industry, rows)
    except Exception as e:
        print(f"[simulator] failed to persist rows for {industry}: {e}")
    return {"rows": st["recent_rows"], "reading_count": st["reading_count"], "running": True}


@app.post("/api/ui/simulator/fault")
def ui_simulator_fault(payload: dict) -> dict:
    industry = payload.get("industry", "mining")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    asset_id = payload.get("asset_id")
    if not asset_id:
        raise HTTPException(status_code=400, detail="Missing asset_id")
    st = _sim_state(industry)
    current = st["faults"].get(asset_id, {"enabled": False, "severity": 0, "mode": "degradation"})
    current["enabled"] = bool(payload.get("enabled", current["enabled"]))
    if "severity" in payload:
        current["severity"] = int(payload["severity"])
    if "mode" in payload:
        current["mode"] = str(payload["mode"])
    st["faults"][asset_id] = current
    return {"asset_id": asset_id, "fault": current, "faults": st["faults"]}


@app.get("/api/ui/config/template")
def ui_config_template(industry: str = "mining") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    cfg = _industry_cfg(industry)
    assets = []
    for a in cfg.get("simulator", {}).get("assets", []):
        sensors = _sensor_defs(industry, a.get("type", ""))
        assets.append(
            {
                "id": a.get("id", ""),
                "type": _normalize_text(a.get("type", "equipment")),
                "path": f"{_normalize_text(a.get('site', 'Site'))} / {_normalize_text(a.get('area', 'Area'))} / {_normalize_text(a.get('unit', 'Unit'))}",
                "sensors": [
                    {
                        "name": s.get("name", "sensor"),
                        "unit": s.get("unit", ""),
                        "warn": s.get("warning_threshold") or "",
                        "crit": s.get("critical_threshold") or "",
                    }
                    for s in sensors[:6]
                ],
            }
        )
    zerobus_defaults_path = ROOT.parent / "core" / "zerobus_ingest" / "defaults.yaml"
    zerobus = {}
    if zerobus_defaults_path.exists():
        try:
            zerobus = (yaml.safe_load(zerobus_defaults_path.read_text(encoding="utf-8")) or {}).get("zerobus", {})
        except Exception:
            zerobus = {}
    saved_cfg = _load_zerobus_config("opcua") or {}
    saved_target = (saved_cfg.get("target", {}) or {}) if isinstance(saved_cfg, dict) else {}
    default_target_catalog = str(saved_target.get("catalog") or cfg.get("catalog", f"pdm_{industry}"))
    default_target_schema = str(saved_target.get("schema") or "bronze")
    default_target_table = str(saved_target.get("table") or _DEFAULT_ZEROBUS_TARGET_TABLE)

    return {
        "industry_key": industry,
        "display_name": cfg.get("display_name", industry.title()),
        "catalog": cfg.get("catalog", f"pdm_{industry}"),
        "protocol": cfg.get("simulator", {}).get("protocol", "OPC-UA"),
        "cost_unit": _default_cost_unit(industry),
        "timezone": _default_tz(industry),
        "persona": cfg.get("agent", {}).get("persona", "Maintenance Supervisor AI"),
        "asset_noun": _default_asset_noun(industry),
        "downtime_event": _default_downtime(industry),
        "isa_levels": [l.get("display", "").strip() for l in cfg.get("isa95_hierarchy", {}).get("levels", [])] or ["Site", "Area", "Unit", "Equipment", "Component"],
        "assets": assets,
        "connector": {
            "protocol": str(saved_cfg.get("protocol") or "opcua"),
            "endpoint": str(saved_cfg.get("endpoint") or "opc.tcp://192.168.1.100:4840"),
            "oauth_client_id": str((saved_cfg.get("auth", {}) or {}).get("client_id") or _DEFAULT_ZEROBUS_CLIENT_ID),
            "oauth_client_secret": "",
            "zerobus_endpoint": str(saved_cfg.get("zerobus_endpoint") or _DEFAULT_ZEROBUS_ENDPOINT or f"http://{zerobus.get('host', 'localhost')}:{zerobus.get('web_port', 8080)}"),
            "workspace_url": str(saved_cfg.get("workspace_host") or _DEFAULT_ZEROBUS_WORKSPACE_URL),
            "target_catalog": default_target_catalog,
            "target_schema": default_target_schema,
            "target_table": default_target_table,
            "target_fqn": f"{default_target_catalog}.{default_target_schema}.{default_target_table}",
        },
    }


@app.post("/api/ui/config/preview")
def ui_config_preview(payload: dict) -> dict:
    return {"yaml": _yaml_from_payload(payload)}


@app.get("/api/ui/sdt/report")
def ui_sdt_report(industry: str = "mining", ticks: int = 300, live: bool = True) -> dict[str, Any]:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")

    available_ticks = _available_sdt_ticks()
    selected_ticks = ticks
    report_dir = _report_dir_for_ticks(selected_ticks)
    if not report_dir.exists():
        if available_ticks:
            selected_ticks = available_ticks[0]
            report_dir = _report_dir_for_ticks(selected_ticks)
        else:
            return {
                "industry": industry,
                "summary": [],
                "tags": [],
                "source": str(SDT_REPORT_DIR),
                "generated_at": "",
                "ticks": selected_ticks,
                "available_ticks": [],
            }

    overall_rows = _read_csv_rows(report_dir / "overall_summary.csv")
    tag_rows = _read_csv_rows(report_dir / "tag_level_summary.csv")

    def _to_int(v: Any) -> int:
        try:
            return int(float(v))
        except Exception:
            return 0

    def _to_float(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    overall = [
        {
            "industry": r.get("industry", ""),
            "raw_total": _to_int(r.get("raw_total")),
            "sdt_total": _to_int(r.get("sdt_total")),
            "kept_pct": _to_float(r.get("kept_pct")),
            "drop_pct": _to_float(r.get("drop_pct")),
        }
        for r in overall_rows
    ]
    overall.sort(key=lambda x: x["industry"])

    tags = [
        {
            "industry": r.get("industry", ""),
            "tag_name": r.get("tag_name", ""),
            "raw_count": _to_int(r.get("raw_count")),
            "sdt_count": _to_int(r.get("sdt_count")),
            "kept_pct": _to_float(r.get("kept_pct")),
            "drop_pct": _to_float(r.get("drop_pct")),
        }
        for r in tag_rows
        if r.get("industry", "") == industry
    ]
    tags.sort(key=lambda x: x["drop_pct"], reverse=True)

    generated_at = ""
    overall_csv = report_dir / "overall_summary.csv"
    if overall_csv.exists():
        generated_at = datetime.fromtimestamp(overall_csv.stat().st_mtime, tz=timezone.utc).isoformat()

    trend_by_industry: dict[str, list[dict[str, Any]]] = {}
    trend_ticks = sorted(set((available_ticks or []) + [selected_ticks]))
    for t in trend_ticks:
        t_dir = _report_dir_for_ticks(t)
        t_csv = t_dir / "overall_summary.csv"
        if not t_csv.exists():
            continue
        t_rows = _read_csv_rows(t_csv)
        t_generated_at = datetime.fromtimestamp(t_csv.stat().st_mtime, tz=timezone.utc).isoformat()
        for r in t_rows:
            ind = r.get("industry", "")
            if not ind:
                continue
            trend_by_industry.setdefault(ind, []).append(
                {
                    "ticks": int(t),
                    "raw_total": _to_int(r.get("raw_total")),
                    "sdt_total": _to_int(r.get("sdt_total")),
                    "kept_pct": _to_float(r.get("kept_pct")),
                    "drop_pct": _to_float(r.get("drop_pct")),
                    "generated_at": t_generated_at,
                }
            )
    for ind in trend_by_industry:
        trend_by_industry[ind].sort(key=lambda x: int(x.get("ticks", 0)))

    # Live-live mode: recompute SDT from incoming Zerobus stream windows.
    if live:
        live_windows = sorted(set((available_ticks or []) + [selected_ticks]))
        live_trend_by_industry: dict[str, list[dict[str, Any]]] = {}
        live_summary_by_industry: dict[str, dict[str, Any]] = {}
        live_tags_for_industry: list[dict[str, Any]] = []
        max_window = max(live_windows) if live_windows else selected_ticks
        for ind in INDUSTRIES:
            points = _live_stream_points(ind, max_window)
            if not points:
                continue
            snapshots: list[dict[str, Any]] = []
            chosen_tags: list[dict[str, Any]] = []
            for w in live_windows:
                ov, tag_metrics = _live_sdt_window_metrics(ind, points, w)
                snapshot = {
                    "ticks": int(w),
                    "raw_total": ov["raw_total"],
                    "sdt_total": ov["sdt_total"],
                    "kept_pct": ov["kept_pct"],
                    "drop_pct": ov["drop_pct"],
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
                snapshots.append(snapshot)
                if int(w) == int(selected_ticks):
                    live_summary_by_industry[ind] = ov
                    if ind == industry:
                        chosen_tags = tag_metrics
            if snapshots:
                live_trend_by_industry[ind] = snapshots
            if ind == industry and chosen_tags:
                live_tags_for_industry = chosen_tags
        if live_summary_by_industry:
            overall = sorted(live_summary_by_industry.values(), key=lambda x: x["industry"])
            if live_tags_for_industry:
                tags = live_tags_for_industry
            trend_by_industry = live_trend_by_industry
            generated_at = datetime.now(timezone.utc).isoformat()

    return {
        "industry": industry,
        "summary": overall,
        "tags": tags,
        "industry_window_snapshots": trend_by_industry.get(industry, []),
        "trend_by_industry": trend_by_industry,
        "source": str(report_dir),
        "generated_at": generated_at,
        "ticks": selected_ticks,
        "available_ticks": available_ticks or [selected_ticks],
    }


@app.get("/api/zerobus/status")
def zerobus_status() -> dict[str, Any]:
    for protocol in ZEROBUS_PROTOCOLS:
        ZEROBUS_STATUS[protocol]["has_config"] = _zerobus_config_path(protocol).exists()
    return {"status": ZEROBUS_STATUS}


@app.post("/api/zerobus/config/load")
def zerobus_config_load(payload: dict) -> dict[str, Any]:
    protocol = _zerobus_protocol(payload)
    cfg = _load_zerobus_config(protocol)
    if not cfg:
        return {"success": False, "message": "No saved config", "config": {}}
    ZEROBUS_STATUS[protocol]["has_config"] = True
    return {
        "success": True,
        "message": "Loaded saved config",
        "config": _sanitize_zerobus_config_for_response(cfg),
    }


@app.post("/api/zerobus/config")
def zerobus_config_save(payload: dict) -> dict[str, Any]:
    protocol = _zerobus_protocol(payload)
    cfg = payload.get("config", {}) or {}
    existing = _load_zerobus_config(protocol) or {}
    auth = cfg.get("auth", {}) or {}
    existing_auth = (existing.get("auth", {}) or {})
    client_secret = str(auth.get("client_secret", "") or "").strip()
    if client_secret:
        auth["client_secret_encrypted"] = _encrypt_secret(client_secret)
    elif existing_auth.get("client_secret_encrypted"):
        auth["client_secret_encrypted"] = existing_auth["client_secret_encrypted"]
    elif existing_auth.get("client_secret"):
        auth["client_secret_encrypted"] = _encrypt_secret(str(existing_auth["client_secret"]))
    auth.pop("client_secret", None)
    auth["has_client_secret"] = bool(auth.get("client_secret_encrypted"))
    cfg["auth"] = auth
    _zerobus_config_path(protocol).write_text(
        json.dumps(cfg, indent=2), encoding="utf-8"
    )
    ZEROBUS_STATUS[protocol]["has_config"] = True
    return {
        "success": True,
        "message": "Saved",
        "protocol": protocol,
        "has_client_secret": auth["has_client_secret"],
    }


@app.post("/api/zerobus/test")
def zerobus_test(payload: dict) -> dict[str, Any]:
    protocol = _zerobus_protocol(payload)
    cfg = payload.get("config", {}) or {}
    missing: list[str] = []
    if not cfg.get("workspace_host"):
        missing.append("workspace_host")
    if not cfg.get("zerobus_endpoint"):
        missing.append("zerobus_endpoint")
    if not cfg.get("endpoint"):
        missing.append("endpoint")
    auth = cfg.get("auth", {}) or {}
    if not auth.get("client_secret"):
        saved = _load_zerobus_config(protocol) or {}
        saved_auth = (saved.get("auth", {}) or {})
        encrypted = saved_auth.get("client_secret_encrypted")
        if encrypted:
            auth["client_secret"] = _decrypt_secret(str(encrypted))
        elif saved_auth.get("client_secret"):
            auth["client_secret"] = str(saved_auth.get("client_secret"))
    if not auth.get("client_id"):
        missing.append("auth.client_id")
    if not auth.get("client_secret"):
        missing.append("auth.client_secret")
    target = cfg.get("target", {}) or {}
    if not target.get("catalog"):
        missing.append("target.catalog")
    if not target.get("schema"):
        missing.append("target.schema")
    if not target.get("table"):
        missing.append("target.table")
    if missing:
        return {
            "success": False,
            "protocol": protocol,
            "message": f"Missing required fields: {', '.join(missing)}",
            "missing": missing,
        }
    return {"success": True, "protocol": protocol, "message": "Test complete"}


@app.post("/api/zerobus/start")
def zerobus_start(payload: dict) -> dict[str, Any]:
    protocol = _zerobus_protocol(payload)
    if not _zerobus_config_path(protocol).exists():
        return {"success": False, "message": "No saved config for protocol", "protocol": protocol}
    ZEROBUS_STATUS[protocol]["active"] = True
    ZEROBUS_STATUS[protocol]["has_config"] = True
    return {"success": True, "message": "Start requested", "protocol": protocol}


@app.post("/api/zerobus/stop")
def zerobus_stop(payload: dict) -> dict[str, Any]:
    protocol = _zerobus_protocol(payload)
    ZEROBUS_STATUS[protocol]["active"] = False
    ZEROBUS_STATUS[protocol]["has_config"] = _zerobus_config_path(protocol).exists()
    return {"success": True, "message": "Stop requested", "protocol": protocol}


@app.post("/api/ui/connector/test")
def ui_connector_test(payload: dict) -> dict:
    protocol = (payload.get("protocol", "opcua") or "opcua").lower()
    seed = f"{protocol}:{payload.get('endpoint', '')}"
    rng = random.Random(seed)
    return {
        "ok": True,
        "latency_ms": int(rng.uniform(8, 38)),
        "message": "Connected",
        "protocol": protocol,
    }


@app.post("/api/ui/connector/discover-tags")
def ui_connector_discover(payload: dict) -> dict:
    protocol = (payload.get("protocol", "opcua") or "opcua").lower()
    query = (payload.get("query", "") or "").lower().strip()
    tags = SIMULATED_TAGS.get(protocol, [])
    if query:
        tags = [t for t in tags if query in t["name"].lower() or query in t["desc"].lower()]
    return {"tags": tags, "count": len(tags)}


@app.get("/api/stream/latest")
def stream_latest(industry: str = "mining", limit: int = 50) -> dict:
    now = datetime.now(timezone.utc)
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    live_rows = _bronze_latest(industry, limit)
    if live_rows:
        return {
            "rows": [
                {
                    "timestamp": str(r.get("timestamp")),
                    "site_id": r.get("site_id"),
                    "area_id": r.get("area_id"),
                    "unit_id": r.get("unit_id"),
                    "equipment_id": r.get("equipment_id"),
                    "tag_name": r.get("tag_name"),
                    "value": r.get("value"),
                    "unit": r.get("unit"),
                    "quality": r.get("quality"),
                    "source_protocol": r.get("source_protocol"),
                }
                for r in live_rows
            ]
        }
    asset_defs = _asset_defs(industry)
    if not asset_defs:
        return {"rows": []}
    protocol = _industry_cfg(industry).get("simulator", {}).get("protocol", "OPC-UA")
    rows = []
    for i in range(min(limit, 200)):
        asset = random.choice(asset_defs)
        sensors = _sensor_defs(industry, asset.get("type", ""))
        sensor = random.choice(sensors) if sensors else {"name": "sensor_1", "unit": "u", "normal_range": [1.0, 100.0], "warning_threshold": None, "critical_threshold": None, "dir": 1}
        low, high = sensor.get("normal_range", [1.0, 100.0])
        v = random.uniform(low, high)
        q = "good"
        warn = sensor.get("warning_threshold")
        crit = sensor.get("critical_threshold")
        dirn = sensor.get("dir", 1)
        if crit is not None and ((dirn == 1 and v >= crit) or (dirn == -1 and v <= crit)):
            q = "bad"
        elif warn is not None and ((dirn == 1 and v >= warn) or (dirn == -1 and v <= warn)):
            q = "uncertain"
        rows.append(
            {
                "timestamp": (now - timedelta(seconds=i * 5)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "site_id": asset.get("site", "site_1"),
                "area_id": asset.get("area", "area_1"),
                "unit_id": asset.get("unit", "unit_1"),
                "equipment_id": asset["id"],
                "tag_name": sensor.get("name", f"sensor_{(i % 6) + 1}"),
                "value": round(v, 2),
                "unit": sensor.get("unit", "u"),
                "quality": q,
                "source_protocol": protocol,
            }
        )
    return {"rows": rows}


@app.post("/api/agent/chat")
def agent_chat(payload: dict) -> dict:
    messages = payload.get("messages", [])
    user_text = ""
    if messages:
        user_text = str(messages[-1].get("content", "") or "").strip()
    if not user_text:
        return {"choices": [{"message": {"content": "Please enter a question."}}]}

    industry = str(payload.get("industry", "mining") or "mining").lower()
    if industry not in INDUSTRIES:
        industry = "mining"
    conversation_id = str(payload.get("conversation_id", "") or "").strip()
    resolved_asset, raw_asset = _resolve_asset_alias(industry, user_text)
    effective_user_text = user_text
    if resolved_asset:
        alias = raw_asset or resolved_asset
        effective_user_text = (
            f"{user_text}\n\n"
            f"Resolver note: interpret asset reference '{alias}' as canonical equipment ID '{resolved_asset}'. "
            f"Answer for '{resolved_asset}', and at the end ask a short confirmation question: "
            f"\"I interpreted '{alias}' as '{resolved_asset}' — is that what you meant?\""
        )

    room_map = _load_genie_room_map()
    space_id = room_map.get(industry) or room_map.get("default", "")
    if not space_id or WorkspaceClient is None:
        answer = (
            f"Diagnosis: potential developing fault for the selected asset. "
            f"Action: schedule inspection in the next shift, verify parts, and prepare a maintenance window. "
            f"User message: {effective_user_text}"
        )
        return {"choices": [{"message": {"content": answer}}]}

    try:
        w = WorkspaceClient()
        message_obj: dict[str, Any]
        if conversation_id:
            create_resp = w.api_client.do(
                "POST",
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
                body={"content": effective_user_text},
            )
            message_obj = create_resp if isinstance(create_resp, dict) else {}
        else:
            start_resp = w.api_client.do(
                "POST",
                f"/api/2.0/genie/spaces/{space_id}/start-conversation",
                body={"content": effective_user_text},
            )
            if not isinstance(start_resp, dict):
                start_resp = {}
            conv = start_resp.get("conversation") or {}
            conversation_id = str(conv.get("conversation_id") or conv.get("id") or "")
            message_obj = start_resp.get("message") or {}

        message_id = str(message_obj.get("message_id") or message_obj.get("id") or "")
        if not conversation_id or not message_id:
            raise RuntimeError("Genie did not return conversation_id/message_id")

        terminal_states = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
        status = str(message_obj.get("status") or "SUBMITTED")
        final_message = message_obj
        start_ts = time.time()
        timeout_s = 90.0
        while status not in terminal_states and (time.time() - start_ts) < timeout_s:
            time.sleep(1.5)
            polled = w.api_client.do(
                "GET",
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
            )
            if isinstance(polled, dict):
                final_message = polled
                status = str(polled.get("status") or status)

        if status != "COMPLETED":
            raise RuntimeError(f"Genie message status={status}")

        text = _genie_extract_text(final_message) or "Genie completed your request."
        return {
            "conversation_id": conversation_id,
            "choices": [{"message": {"content": text}}],
        }
    except Exception as e:
        return {
            "conversation_id": conversation_id,
            "choices": [{"message": {"content": f"Genie request failed: {e}"}}],
        }


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    index = DIST / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"status": "frontend-not-built", "path": full_path}
