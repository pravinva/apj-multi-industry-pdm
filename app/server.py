from __future__ import annotations

import json
import csv
import os
import random
import re
import copy
import base64
import io
import shutil
import subprocess
import time
import urllib.request
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
DOCS_DIR = ROOT.parent / "docs"
FINANCE_REPORT_PDF = DOCS_DIR / "finance_genie_naka_fab_10q_professional.pdf"
APP_DOCS_DIR = ROOT / "docs"
APP_FINANCE_REPORT_PDF = APP_DOCS_DIR / "finance_genie_naka_fab_10q_professional.pdf"
SDT_REPORT_DIR = (ROOT / "sdt-compression") if (ROOT / "sdt-compression").exists() else (ROOT.parent / "docs" / "sdt-compression")
GENIE_ROOM_MAP_PATH = ROOT / "genie_rooms.json"
FINANCE_GENIE_ROOM_MAP_PATH = ROOT / "genie_rooms_finance.json"
INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
SUPPORTED_CURRENCIES = {"USD", "AUD", "JPY", "INR", "SGD", "KRW"}
GEO_SITES: dict[str, list[dict[str, Any]]] = {
    "mining": [
        {
            "site_id": "rio-pilbara",
            "site_key": "gudai_darri",
            "currency": "AUD",
            "name": "Rio Pilbara Operations",
            "customer": "Rio Tinto",
            "description": "Pilbara iron ore processing and materials handling.",
            "lat": -22.34,
            "lng": 118.52,
        },
        {
            "site_id": "tata-odisha",
            "site_key": "odisha_hub",
            "currency": "INR",
            "name": "Tata Steel Diagnostic Operations",
            "customer": "Tata Steel",
            "description": "India metals predictive maintenance and diagnostic center operations.",
            "lat": 20.264,
            "lng": 85.84,
        },
        {
            "site_id": "adaro-kalimantan",
            "site_key": "kalimantan_hub",
            "currency": "SGD",
            "name": "PETRONAS Reliability Operations Hub",
            "customer": "PETRONAS",
            "description": "ASEAN reliability operations and predictive maintenance program.",
            "lat": -3.316,
            "lng": 114.59,
        },
        {
            "site_id": "posco-gangwon",
            "site_key": "gangwon_mine",
            "currency": "KRW",
            "name": "POSCO Gangwon Mining Hub",
            "customer": "POSCO",
            "description": "Korea mining haul and processing reliability operations.",
            "lat": 37.8813,
            "lng": 127.7298,
        },
    ],
    "water": [
        {
            "site_id": "sydney-water",
            "site_key": "sydney_hub",
            "currency": "AUD",
            "name": "Sydney Water Western Hub",
            "customer": "Sydney Water",
            "description": "Water treatment and distribution pumping network.",
            "lat": -33.81,
            "lng": 150.91,
        },
        {
            "site_id": "chennai-water",
            "site_key": "chennai_hub",
            "currency": "INR",
            "name": "CRIS Infrastructure Reliability Hub",
            "customer": "CRIS (Indian Railways)",
            "description": "India utilities and infrastructure asset-failure prediction operations.",
            "lat": 13.0827,
            "lng": 80.2707,
        },
        {
            "site_id": "pub-singapore",
            "site_key": "singapore_hub",
            "currency": "SGD",
            "name": "PUB Singapore Hub",
            "customer": "PUB Singapore",
            "description": "ASEAN water treatment and metering reliability operations.",
            "lat": 1.3521,
            "lng": 103.8198,
        },
        {
            "site_id": "seoul-water",
            "site_key": "seoul_hub",
            "currency": "KRW",
            "name": "Seoul Water Reliability Hub",
            "customer": "K-water",
            "description": "Korea treatment, pumping, and metering reliability network.",
            "lat": 37.5665,
            "lng": 126.978,
        },
    ],
    "automotive": [
        {
            "site_id": "toyota-motomachi",
            "site_key": "nagoya",
            "currency": "JPY",
            "name": "Toyota Motomachi Plant",
            "customer": "Toyota Motor Corporation",
            "description": "Japan automotive stamping, welding, and final assembly operations.",
            "lat": 35.084,
            "lng": 137.156,
        },
        {
            "site_id": "tata-pune",
            "site_key": "pune_plant",
            "currency": "INR",
            "name": "Mahindra Telemetry Operations",
            "customer": "Mahindra & Mahindra",
            "description": "India automotive and EV telemetry analytics operations.",
            "lat": 18.5204,
            "lng": 73.8567,
        },
        {
            "site_id": "toyota-thailand",
            "site_key": "bangkok_plant",
            "currency": "SGD",
            "name": "VinFast IIoT Operations Hub",
            "customer": "VinFast",
            "description": "ASEAN automotive and transport IIoT analytics operations.",
            "lat": 13.7563,
            "lng": 100.5018,
        },
        {
            "site_id": "hyundai-ulsan",
            "site_key": "ulsan_plant",
            "currency": "KRW",
            "name": "Hyundai Ulsan Plant",
            "customer": "Hyundai Motor",
            "description": "Korea stamping, welding, and assembly reliability operations.",
            "lat": 35.5384,
            "lng": 129.3114,
        },
    ],
    "semiconductor": [
        {
            "site_id": "renesas-naka",
            "site_key": "naka_fab",
            "currency": "JPY",
            "name": "Renesas Naka Fab",
            "customer": "Renesas",
            "description": "Fab equipment reliability for etch and lithography lines.",
            "lat": 36.38,
            "lng": 140.44,
        },
        {
            "site_id": "vedanta-bengaluru",
            "site_key": "bengaluru_fab",
            "currency": "INR",
            "name": "Vedanta Bengaluru Fab",
            "customer": "Vedanta",
            "description": "India etch and lithography fab reliability operations.",
            "lat": 12.9716,
            "lng": 77.5946,
        },
        {
            "site_id": "infineon-penang",
            "site_key": "penang_fab",
            "currency": "SGD",
            "name": "Infineon Penang Fab",
            "customer": "Infineon",
            "description": "ASEAN fab tool reliability and metrology operations.",
            "lat": 5.4141,
            "lng": 100.3288,
        },
        {
            "site_id": "samsung-giheung",
            "site_key": "giheung_fab",
            "currency": "KRW",
            "name": "Samsung Giheung Fab",
            "customer": "Samsung",
            "description": "Korea fab etch, lithography, and metrology reliability operations.",
            "lat": 37.2805,
            "lng": 127.1113,
        },
    ],
    "energy": [
        {
            "site_id": "alinta-hsdale",
            "site_key": "northhub",
            "currency": "AUD",
            "name": "Hornsdale Grid Storage",
            "customer": "Alinta Energy",
            "description": "Wind, BESS, and grid balancing assets.",
            "lat": -33.07,
            "lng": 138.67,
        },
        {
            "site_id": "adani-gujarat",
            "site_key": "gujarat_grid",
            "currency": "INR",
            "name": "Reliance Network Reliability Hub",
            "customer": "Reliance (Jio)",
            "description": "India network quality, fault analytics, and reliability operations.",
            "lat": 22.2587,
            "lng": 71.1924,
        },
        {
            "site_id": "petronas-johor",
            "site_key": "vietnam_delta",
            "currency": "SGD",
            "name": "GPSC Maintenance Optimization Hub",
            "customer": "GPSC (Thailand)",
            "description": "ASEAN predictive and corrective maintenance optimization operations.",
            "lat": 1.4927,
            "lng": 103.7414,
        },
        {
            "site_id": "kepco-jeju",
            "site_key": "jeju_grid",
            "currency": "KRW",
            "name": "KEPCO Jeju Grid Hub",
            "customer": "KEPCO",
            "description": "Korea wind, BESS, and transformer reliability operations.",
            "lat": 33.4996,
            "lng": 126.5312,
        },
    ],
}
GEO_SCHEMATICS: dict[str, dict[str, Any]] = {
    "rio-pilbara": {
        "subtitle": "Crushing and slurry transfer P&ID",
        "nodes": [
            {"id": "n1", "label": "Crusher Feed", "equip_id": "CR-01", "x": 120, "y": 100, "w": 150, "h": 56},
            {"id": "n2", "label": "Primary Crusher", "equip_id": "CR-07", "x": 340, "y": 100, "w": 170, "h": 56},
            {"id": "n3", "label": "Mill Circuit", "equip_id": "ML-04", "x": 620, "y": 100, "w": 170, "h": 56},
            {"id": "n4", "label": "Slurry Pump Train", "equip_id": "PS-12", "x": 620, "y": 240, "w": 190, "h": 56},
            {"id": "n5", "label": "Tailings", "equip_id": "TG-02", "x": 920, "y": 240, "w": 140, "h": 56},
        ],
        "pipes": [{"from": "n1", "to": "n2"}, {"from": "n2", "to": "n3"}, {"from": "n3", "to": "n4"}, {"from": "n4", "to": "n5"}],
    },
    "sydney-water": {
        "subtitle": "Treatment to distribution flow",
        "nodes": [
            {"id": "n1", "label": "Intake", "equip_id": "MT-03", "x": 120, "y": 120, "w": 140, "h": 56},
            {"id": "n2", "label": "Filtration", "equip_id": "TP-01", "x": 360, "y": 120, "w": 160, "h": 56},
            {"id": "n3", "label": "Disinfection", "equip_id": "TP-03", "x": 620, "y": 120, "w": 160, "h": 56},
            {"id": "n4", "label": "High Lift Pump", "equip_id": "PS-09", "x": 620, "y": 250, "w": 190, "h": 56},
            {"id": "n5", "label": "Distribution", "equip_id": "VS-11", "x": 920, "y": 250, "w": 160, "h": 56},
        ],
        "pipes": [{"from": "n1", "to": "n2"}, {"from": "n2", "to": "n3"}, {"from": "n3", "to": "n4"}, {"from": "n4", "to": "n5"}],
    },
    "toyota-motomachi": {
        "subtitle": "Press line to final assembly",
        "nodes": [
            {"id": "n1", "label": "Stamping", "equip_id": "PR-03", "x": 120, "y": 120, "w": 150, "h": 56},
            {"id": "n2", "label": "Body Weld", "equip_id": "WB-02", "x": 350, "y": 120, "w": 150, "h": 56},
            {"id": "n3", "label": "Paint Booth", "equip_id": "PB-04", "x": 580, "y": 120, "w": 150, "h": 56},
            {"id": "n4", "label": "Final Line", "equip_id": "AL-09", "x": 810, "y": 120, "w": 150, "h": 56},
        ],
        "pipes": [{"from": "n1", "to": "n2"}, {"from": "n2", "to": "n3"}, {"from": "n3", "to": "n4"}],
    },
    "renesas-naka": {
        "subtitle": "Fab process chain and utilities",
        "nodes": [
            {"id": "n1", "label": "Load Port", "equip_id": "CMP-07", "x": 130, "y": 110, "w": 150, "h": 56},
            {"id": "n2", "label": "Etch Cluster", "equip_id": "ET-02", "x": 360, "y": 110, "w": 160, "h": 56},
            {"id": "n3", "label": "Litho Scanner", "equip_id": "LS-05", "x": 610, "y": 110, "w": 170, "h": 56},
            {"id": "n4", "label": "Vacuum Utility", "equip_id": "VU-03", "x": 610, "y": 250, "w": 170, "h": 56},
            {"id": "n5", "label": "Metrology", "equip_id": "ME-01", "x": 900, "y": 250, "w": 150, "h": 56},
        ],
        "pipes": [{"from": "n1", "to": "n2"}, {"from": "n2", "to": "n3"}, {"from": "n3", "to": "n4"}, {"from": "n4", "to": "n5"}],
    },
    "alinta-hsdale": {
        "subtitle": "Wind farm collection and storage",
        "nodes": [],
        "pipes": [],
    },
    "tata-odisha": {
        "subtitle": "Crushing and slurry transfer P&ID",
        "nodes": [],
        "pipes": [],
    },
    "adaro-kalimantan": {
        "subtitle": "Crushing and slurry transfer P&ID",
        "nodes": [],
        "pipes": [],
    },
    "chennai-water": {
        "subtitle": "Treatment to distribution flow",
        "nodes": [],
        "pipes": [],
    },
    "pub-singapore": {
        "subtitle": "Treatment to distribution flow",
        "nodes": [],
        "pipes": [],
    },
    "tata-pune": {
        "subtitle": "Press line to final assembly",
        "nodes": [],
        "pipes": [],
    },
    "toyota-thailand": {
        "subtitle": "Press line to final assembly",
        "nodes": [],
        "pipes": [],
    },
    "vedanta-bengaluru": {
        "subtitle": "Fab process chain and utilities",
        "nodes": [],
        "pipes": [],
    },
    "infineon-penang": {
        "subtitle": "Fab process chain and utilities",
        "nodes": [],
        "pipes": [],
    },
    "adani-gujarat": {
        "subtitle": "Wind farm collection and storage",
        "nodes": [],
        "pipes": [],
    },
    "petronas-johor": {
        "subtitle": "Wind farm collection and storage",
        "nodes": [],
        "pipes": [],
    },
    "posco-gangwon": {
        "subtitle": "Crushing and slurry transfer P&ID",
        "nodes": [],
        "pipes": [],
    },
    "seoul-water": {
        "subtitle": "Treatment to distribution flow",
        "nodes": [],
        "pipes": [],
    },
    "hyundai-ulsan": {
        "subtitle": "Press line to final assembly",
        "nodes": [],
        "pipes": [],
    },
    "samsung-giheung": {
        "subtitle": "Fab process chain and utilities",
        "nodes": [],
        "pipes": [],
    },
    "kepco-jeju": {
        "subtitle": "Wind farm collection and storage",
        "nodes": [],
        "pipes": [],
    },
}
ISA_EMOJI = {
    "site": "🏭",
    "area": "📍",
    "unit": "⚙",
    "equipment": "🛠",
    "component": "🔩",
}
SIM_STATE: dict[str, dict[str, Any]] = {}
_SQL_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
# Default 900s (~15m demo): stable UC tables (finance, actions, features). Override via OT_PDM_SQL_CACHE_TTL_S.
_SQL_CACHE_TTL_DEFAULT_S = float(os.getenv("OT_PDM_SQL_CACHE_TTL_S", "900"))
# Latest scored predictions drive risk matrix / alerts — keep short (see OT_PDM_PREDICTIONS_CACHE_TTL_S).
_SQL_PREDICTIONS_TTL_S = float(os.getenv("OT_PDM_PREDICTIONS_CACHE_TTL_S", "30"))
# Live bronze tail for stream page — short so rows stay fresh while sim runs.
_SQL_STREAM_BRONZE_TTL_S = float(os.getenv("OT_PDM_STREAM_SQL_CACHE_TTL_S", "5"))
_BRONZE_LATEST_KEY_RE = re.compile(r":bronze:\d+$")
_UI_RESPONSE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
# Fallback TTL for any UI cache keys other than overview/hierarchy (see _ui_response_ttl_for_key).
_UI_RESPONSE_TTL_DEFAULT_S = float(os.getenv("OT_PDM_UI_RESPONSE_CACHE_TTL_S", "900"))
_UI_OVERVIEW_CACHE_TTL_S = float(os.getenv("OT_PDM_UI_OVERVIEW_CACHE_TTL_S", "0"))
_UI_HIERARCHY_CACHE_TTL_S = float(os.getenv("OT_PDM_UI_HIERARCHY_CACHE_TTL_S", "0"))
# Per-asset UI payloads (detail + model tab): default matches prediction SQL TTL so scores stay fresh.
_UI_ASSET_MODEL_CACHE_TTL_S = float(os.getenv("OT_PDM_UI_ASSET_MODEL_CACHE_TTL_S", str(_SQL_PREDICTIONS_TTL_S)))
_GEO_SITES_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_GEO_SITES_CACHE_TTL_S = float(os.getenv("OT_PDM_GEO_SITES_CACHE_TTL_S", "900"))
_MANUAL_KB_CACHE: dict[str, list[dict[str, Any]]] = {}
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
_LAKEBASE_PG_HOST = os.getenv("OT_PDM_LAKEBASE_PG_HOST", "").strip()
_LAKEBASE_PG_PORT = int(os.getenv("OT_PDM_LAKEBASE_PG_PORT", "5432"))
_LAKEBASE_PG_DB = os.getenv("OT_PDM_LAKEBASE_PG_DB", "").strip()
_LAKEBASE_PG_USER = os.getenv("OT_PDM_LAKEBASE_PG_USER", "").strip()
_LAKEBASE_PG_PASSWORD = os.getenv("OT_PDM_LAKEBASE_PG_PASSWORD", "").strip()
_LAKEBASE_PG_SSLMODE = os.getenv("OT_PDM_LAKEBASE_PG_SSLMODE", "require").strip() or "require"
_LAKEBASE_ACTION_TABLE = os.getenv("OT_PDM_LAKEBASE_ACTION_TABLE", "otpdm.operator_recommendation_actions").strip() or "otpdm.operator_recommendation_actions"
_LIVE_SCORING_STATE: dict[str, dict[str, Any]] = {}
_LIVE_DLT_STATE: dict[str, dict[str, Any]] = {}
_SIM_STAGING_READY: dict[str, bool] = {}
_RUL_METRICS_FALLBACK: dict[str, dict[str, Any]] = {}
_RUL_METRICS_DEFAULTS: dict[str, dict[str, float]] = {
    "mining": {"r2": 0.94, "rmse": 13.8},
    "energy": {"r2": 0.92, "rmse": 16.1},
    "water": {"r2": 0.91, "rmse": 17.4},
    "automotive": {"r2": 0.93, "rmse": 15.2},
    "semiconductor": {"r2": 0.95, "rmse": 12.9},
}
_LIVE_SCORING_MIN_INTERVAL_S = int(os.getenv("OT_PDM_LIVE_SCORING_MIN_INTERVAL_S", "180"))
_LIVE_SCORING_STALENESS_S = int(os.getenv("OT_PDM_LIVE_SCORING_STALENESS_S", "90"))
_LIVE_SCORING_FRESH_LOOKBACK_S = int(os.getenv("OT_PDM_LIVE_SCORING_FRESH_LOOKBACK_S", "15"))
# When true, skip freshness SQL + jobs/run-now on every predictions read (demo / UI responsiveness).
_SKIP_LIVE_SCORING_ON_READ = str(os.getenv("OT_PDM_SKIP_LIVE_SCORING_ON_READ", "false")).lower() in (
    "1",
    "true",
    "yes",
)
# Executive Finance work orders: auto = Lakebase ODS when rows exist, else synthetic model.
_EXECUTIVE_WO_SOURCE = str(os.getenv("OT_PDM_EXECUTIVE_WO_SOURCE", "auto") or "auto").strip().lower()
ZEROBUS_PROTOCOLS = ["opcua", "mqtt", "modbus"]
ZEROBUS_STATUS: dict[str, dict[str, bool]] = {
    p: {"active": False, "has_config": False} for p in ZEROBUS_PROTOCOLS
}
ZEROBUS_CONFIG_DIR = ROOT / ".zerobus-configs"
ZEROBUS_KEY_PATH = ROOT / ".zerobus-key"
_FERNET: Fernet | None = None


def _resolve_warehouse_id() -> str:
    explicit = os.getenv("OT_PDM_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
    if explicit:
        return explicit
    if WorkspaceClient is not None:
        try:
            client = WorkspaceClient()
            running: list[tuple[str, str]] = []
            fallback: list[tuple[str, str]] = []
            for wh in client.warehouses.list():
                wid = str(getattr(wh, "id", "") or "")
                if wid:
                    name = str(getattr(wh, "name", "") or "").lower()
                    fallback.append((wid, name))
                    if str(getattr(wh, "state", "")).upper().endswith("RUNNING"):
                        running.append((wid, name))
            for candidates in (running, fallback):
                for wid, name in candidates:
                    if "shared unity catalog serverless" in name:
                        return wid
            for candidates in (running, fallback):
                for wid, name in candidates:
                    if "unity catalog" in name:
                        return wid
            if fallback:
                return fallback[0][0]
        except Exception:
            pass
    # Last-resort static fallback for environments without discovery access.
    return "4b9b953939869799"


_WAREHOUSE_ID = _resolve_warehouse_id()
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

app = FastAPI(title="Predictive Maintenance Hub")

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
    target_fqn = _simulator_landing_target(industry)
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
            {"id": "HT-018", "type": "haul_truck", "site": "gudai_darri", "area": "pit_c", "unit": "haul_fleet"},
            {"id": "HT-021", "type": "haul_truck", "site": "gudai_darri", "area": "pit_b", "unit": "haul_fleet"},
            {"id": "HT-025", "type": "haul_truck", "site": "gudai_darri", "area": "pit_a", "unit": "haul_fleet"},
            {"id": "HT-029", "type": "haul_truck", "site": "gudai_darri", "area": "pit_c", "unit": "haul_fleet"},
            {"id": "CV-006", "type": "conveyor", "site": "gudai_darri", "area": "process", "unit": "crushing"},
            {"id": "CV-010", "type": "conveyor", "site": "gudai_darri", "area": "process", "unit": "secondary_crushing"},
        ],
        "energy": [
            {"id": "WT-004", "type": "wind_turbine", "site": "northhub", "area": "wind", "unit": "gen"},
            {"id": "BESS-01", "type": "bess", "site": "northhub", "area": "storage", "unit": "battery"},
            {"id": "TX-07", "type": "transformer", "site": "northhub", "area": "substation", "unit": "transmission"},
            {"id": "WT-011", "type": "wind_turbine", "site": "northhub", "area": "wind", "unit": "gen"},
            {"id": "WT-015", "type": "wind_turbine", "site": "northhub", "area": "wind", "unit": "gen"},
            {"id": "WT-018", "type": "wind_turbine", "site": "northhub", "area": "wind", "unit": "gen"},
            {"id": "BESS-03", "type": "bess", "site": "northhub", "area": "storage", "unit": "battery"},
            {"id": "BESS-04", "type": "bess", "site": "northhub", "area": "storage", "unit": "battery"},
            {"id": "TX-11", "type": "transformer", "site": "northhub", "area": "substation", "unit": "transmission"},
            {"id": "TX-15", "type": "transformer", "site": "northhub", "area": "substation", "unit": "transmission"},
        ],
        "water": [
            {"id": "PS-07", "type": "pump", "site": "prospect", "area": "station_7", "unit": "distribution"},
            {"id": "MT-03", "type": "smart_meter", "site": "cbd", "area": "zone_3", "unit": "metering"},
            {"id": "TP-01", "type": "chlorination_unit", "site": "prospect", "area": "treatment", "unit": "dosing"},
            {"id": "VS-11", "type": "vent_shaft", "site": "north", "area": "tunnel", "unit": "ventilation"},
            {"id": "PS-09", "type": "pump", "site": "prospect", "area": "station_9", "unit": "distribution"},
            {"id": "PS-12", "type": "pump", "site": "south", "area": "station_12", "unit": "distribution"},
            {"id": "MT-08", "type": "smart_meter", "site": "west", "area": "zone_8", "unit": "metering"},
            {"id": "MT-14", "type": "smart_meter", "site": "east", "area": "zone_14", "unit": "metering"},
            {"id": "TP-03", "type": "chlorination_unit", "site": "prospect", "area": "treatment", "unit": "dosing"},
            {"id": "VS-15", "type": "vent_shaft", "site": "north", "area": "tunnel", "unit": "ventilation"},
        ],
        "automotive": [
            {"id": "TP-07", "type": "stamping_press", "site": "nagoya", "area": "press", "unit": "line_a"},
            {"id": "WR-14", "type": "robotic_welder", "site": "nagoya", "area": "body", "unit": "line_a"},
            {"id": "CNC-22", "type": "cnc_machine", "site": "nagoya", "area": "machining", "unit": "line_b"},
            {"id": "CV-A3", "type": "assembly_conveyor", "site": "nagoya", "area": "assembly", "unit": "line_c"},
            {"id": "TP-11", "type": "stamping_press", "site": "nagoya", "area": "press", "unit": "line_b"},
            {"id": "TP-16", "type": "stamping_press", "site": "nagoya", "area": "press", "unit": "line_c"},
            {"id": "WR-22", "type": "robotic_welder", "site": "nagoya", "area": "body", "unit": "line_b"},
            {"id": "WR-28", "type": "robotic_welder", "site": "nagoya", "area": "body", "unit": "line_c"},
            {"id": "CNC-31", "type": "cnc_machine", "site": "nagoya", "area": "machining", "unit": "line_c"},
            {"id": "CV-A8", "type": "assembly_conveyor", "site": "nagoya", "area": "assembly", "unit": "line_d"},
        ],
        "semiconductor": [
            {"id": "ET-04", "type": "etch_tool", "site": "naka_fab", "area": "bay_3", "unit": "etch"},
            {"id": "LT-11", "type": "stepper", "site": "naka_fab", "area": "bay_5", "unit": "lithography"},
            {"id": "CMP-07", "type": "cmp_tool", "site": "naka_fab", "area": "bay_2", "unit": "polish"},
            {"id": "IN-02", "type": "inspection_tool", "site": "naka_fab", "area": "bay_6", "unit": "metrology"},
            {"id": "ET-09", "type": "etch_tool", "site": "naka_fab", "area": "bay_4", "unit": "etch"},
            {"id": "ET-13", "type": "etch_tool", "site": "naka_fab", "area": "bay_1", "unit": "etch"},
            {"id": "LT-18", "type": "stepper", "site": "naka_fab", "area": "bay_7", "unit": "lithography"},
            {"id": "LT-21", "type": "stepper", "site": "naka_fab", "area": "bay_8", "unit": "lithography"},
            {"id": "CMP-14", "type": "cmp_tool", "site": "naka_fab", "area": "bay_2", "unit": "polish"},
            {"id": "IN-10", "type": "inspection_tool", "site": "naka_fab", "area": "bay_9", "unit": "metrology"},
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
    return [a.get("id") for a in _asset_defs(industry) if a.get("id")]


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


def _site_token_norm(value: str) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _resolve_site_context(industry: str, user_text: str) -> dict[str, Any] | None:
    text = str(user_text or "").strip()
    if not text:
        return None
    metas = GEO_SITES.get(industry, []) or []
    if not metas:
        return None
    text_norm = _site_token_norm(text)
    for meta in metas:
        site_id = str(meta.get("site_id") or "").strip()
        site_key = str(meta.get("site_key") or "").strip()
        site_name = str(meta.get("name") or "").strip()
        customer = str(meta.get("customer") or "").strip()
        candidates = [site_id, site_key, site_name, customer]
        for c in candidates:
            key = _site_token_norm(c)
            if key and key in text_norm:
                return {
                    "site_id": site_id,
                    "site_key": site_key,
                    "site_name": site_name,
                    "customer": customer,
                    "currency": str(meta.get("currency") or ""),
                }
    return None


def _industry_cfg(industry: str) -> dict[str, Any]:
    cfg_path = _industry_config_path(industry)
    if not cfg_path.exists():
        return _default_industry_cfg(industry)
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}


def _asset_defs_from_table(industry: str) -> list[dict[str, Any]]:
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    table_fqn = f"{catalog}.bronze.asset_metadata"
    safe_industry = _sql_escape(industry)
    stmt = f"""
    SELECT equipment_id, site_id, area_id, unit_id, asset_type, asset_model
    FROM {table_fqn}
    WHERE lower(industry) = lower('{safe_industry}')
    ORDER BY equipment_id
    """
    rows = _run_sql(stmt, cache_key=f"{table_fqn}:{industry}:asset_defs:{int(time.time() // 30)}")
    if not rows:
        return []
    return [
        {
            # Preserve canonical keys exactly as stored in UC so joins/mapping
            # with Gold predictions and config metadata remain stable.
            "id": str(r.get("equipment_id") or "UNKNOWN"),
            "type": str(r.get("asset_type") or "equipment"),
            "site": str(r.get("site_id") or "site_1"),
            "area": str(r.get("area_id") or "area_1"),
            "unit": str(r.get("unit_id") or "unit_1"),
            "model": str(r.get("asset_model") or ""),
        }
        for r in rows
        if r.get("equipment_id")
    ]


def _asset_defs(industry: str) -> list[dict[str, Any]]:
    def _dedupe(defs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for row in defs or []:
            aid = _asset_token_norm(str((row or {}).get("id") or ""))
            if not aid or aid in seen:
                continue
            seen.add(aid)
            out.append(row)
        return out

    cfg_assets = _industry_cfg(industry).get("simulator", {}).get("assets", [])
    table_assets = _asset_defs_from_table(industry)
    if table_assets:
        cfg_by_id = {_asset_token_norm(str(a.get("id"))): a for a in cfg_assets if a.get("id")}
        merged: list[dict[str, Any]] = []
        for row in table_assets:
            aid = _asset_token_norm(str(row.get("id", "")))
            merged.append({**cfg_by_id.get(aid, {}), **row})
        return _dedupe(merged)
    if cfg_assets:
        return _dedupe(cfg_assets)
    # Guardrail: if user-authored config is partial, fall back to built-in
    # industry defaults so simulator ticks still emit rows.
    return _dedupe(_default_industry_cfg(industry).get("simulator", {}).get("assets", []) or [])


def _sensor_defs(industry: str, asset_type: str) -> list[dict[str, Any]]:
    sensors_map = _industry_cfg(industry).get("sensors", {}) or {}
    if not sensors_map:
        sensors_map = _default_industry_cfg(industry).get("sensors", {}) or {}
        if not sensors_map:
            return []
    # Try direct key first.
    direct = sensors_map.get(asset_type)
    if isinstance(direct, list) and direct:
        return direct

    # Fall back to case/format-insensitive matching because UI display paths
    # may normalize asset_type (e.g. "smart_meter" -> "smart meter").
    def _norm_type(t: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(t or "").lower())

    target = _norm_type(asset_type)
    if not target:
        return []
    for k, v in sensors_map.items():
        if _norm_type(k) == target and isinstance(v, list):
            return v
    return []


def _rows_from_external_links(resp: Any, columns: list[str]) -> list[dict[str, Any]]:
    result = getattr(resp, "result", None)
    links = getattr(result, "external_links", None) or []
    values: list[list[Any]] = []
    for link in links:
        url = getattr(link, "external_link", None)
        if not url:
            continue
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                payload = json.loads(r.read().decode("utf-8"))
            if isinstance(payload, dict):
                data = payload.get("data_array")
                if isinstance(data, list):
                    values.extend(data)
            elif isinstance(payload, list):
                values.extend(payload)
        except Exception:
            continue

    rows: list[dict[str, Any]] = []
    for r in values:
        if isinstance(r, dict):
            rows.append(r)
        elif isinstance(r, list):
            if columns:
                rows.append({columns[i]: r[i] for i in range(min(len(columns), len(r)))})
            else:
                rows.append({str(i): v for i, v in enumerate(r)})
    return rows


def _sql_cache_ttl_for_key(cache_key: str | None) -> float:
    key = cache_key or ""
    if ":predictions" in key or ":freshness:" in key:
        return _SQL_PREDICTIONS_TTL_S
    if ":live_stream_points:" in key:
        return _SQL_STREAM_BRONZE_TTL_S
    if _BRONZE_LATEST_KEY_RE.search(key):
        return _SQL_STREAM_BRONZE_TTL_S
    return _SQL_CACHE_TTL_DEFAULT_S


def _run_sql(statement: str, cache_key: str | None = None) -> list[dict[str, Any]]:
    now = time.time()
    key = cache_key or statement
    ttl = _sql_cache_ttl_for_key(cache_key)
    cached = _SQL_CACHE.get(key)
    if cached and (now - cached[0]) < ttl:
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
        # INLINE uses Arrow and can omit data_array in some runtimes even when rows exist.
        if not rows:
            total_row_count = int(getattr(manifest, "total_row_count", 0) or 0) if manifest else 0
            if total_row_count > 0:
                ext = client.statement_execution.execute_statement(
                    statement=statement,
                    warehouse_id=_WAREHOUSE_ID,
                    wait_timeout="20s",
                    disposition=sql_service.Disposition.EXTERNAL_LINKS,
                    format=sql_service.Format.JSON_ARRAY,
                )
                est = getattr(ext, "status", None)
                if est is not None and est.state == sql_service.StatementState.SUCCEEDED:
                    rows = _rows_from_external_links(ext, columns)
        _SQL_CACHE[key] = (now, rows)
        return rows
    except Exception as e:
        print(f"[sql] query failed for warehouse={_WAREHOUSE_ID}: {e}")
        return []


def _ui_response_ttl_for_key(key: str) -> float:
    if key.startswith("overview:"):
        return _UI_OVERVIEW_CACHE_TTL_S
    if key.startswith("hierarchy:"):
        return _UI_HIERARCHY_CACHE_TTL_S
    if key.startswith("ui_asset:") or key.startswith("ui_model:"):
        return _UI_ASSET_MODEL_CACHE_TTL_S
    return _UI_RESPONSE_TTL_DEFAULT_S


def _ui_cache_get(key: str) -> dict[str, Any] | None:
    now = time.time()
    cached = _UI_RESPONSE_CACHE.get(key)
    if not cached:
        return None
    ts, payload = cached
    if (now - ts) >= _ui_response_ttl_for_key(key):
        _UI_RESPONSE_CACHE.pop(key, None)
        return None
    return copy.deepcopy(payload)


def _ui_cache_set(key: str, payload: dict[str, Any]) -> None:
    _UI_RESPONSE_CACHE[key] = (time.time(), copy.deepcopy(payload))


def _ui_cache_invalidate(industry: str | None = None) -> None:
    _GEO_SITES_CACHE.clear()
    if not industry:
        _UI_RESPONSE_CACHE.clear()
        return
    prefixes = (
        f"overview:{industry}:",
        f"hierarchy:{industry}",
        f"ui_asset:{industry}:",
        f"ui_model:{industry}:",
    )
    keys = [k for k in _UI_RESPONSE_CACHE.keys() if any(k.startswith(p) for p in prefixes)]
    for k in keys:
        _UI_RESPONSE_CACHE.pop(k, None)


def _ui_asset_payload_cached(industry: str, asset_id: str, currency: str) -> dict[str, Any]:
    """Shared cache for /api/ui/asset and /api/ui/model so parallel client fetches only compute detail once."""
    ccy = _normalize_currency(currency or "", "")
    cache_key = f"ui_asset:{industry}:{ccy}:{asset_id}"
    cached = _ui_cache_get(cache_key)
    if cached is not None:
        return cached
    payload = _asset_detail(industry, asset_id, display_currency=currency)
    _ui_cache_set(cache_key, payload)
    return payload


def _predictions_map(industry: str, trigger_live_scoring: bool = True) -> dict[str, dict[str, Any]]:
    if trigger_live_scoring:
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
    job_name = f"ot-pdm-scoring-{industry}"
    owner_user = str(os.getenv("OT_PDM_OWNER_USER", "") or "").strip().lower()
    if not owner_user:
        try:
            me = client.current_user.me()
            # Databricks Apps often run as a service principal; those identities can
            # break creator-based filtering for user-owned jobs. Only use owner-based
            # filtering when the principal looks like a real user email.
            candidate_owner = str(getattr(me, "user_name", "") or "").strip().lower()
            owner_user = candidate_owner if "@" in candidate_owner else ""
        except Exception:
            owner_user = ""

    def _name_matches(candidate: Any) -> bool:
        name = str(candidate or "").strip().lower()
        target = job_name.lower()
        if not name:
            return False
        # Accept exact match and bundle-prefixed names like:
        # "[dev user] ot-pdm-scoring-mining"
        return name == target or name.endswith(f" {target}") or target in name

    def _job_id_matches_industry(job_id: int, enforce_owner: bool = True) -> bool:
        try:
            detail = client.api_client.do("GET", "/api/2.1/jobs/get", query={"job_id": int(job_id)})
            creator = str((detail or {}).get("creator_user_name") or "").strip().lower()
            settings = (detail or {}).get("settings", {}) or {}
            if enforce_owner and owner_user and creator and creator != owner_user:
                return False
            return _name_matches(settings.get("name"))
        except Exception:
            return False

    cached = state.get("job_id")
    if cached:
        try:
            cached_id = int(cached)
            if _job_id_matches_industry(cached_id):
                return cached_id
        except Exception:
            pass
        state.pop("job_id", None)

    env_key = f"OT_PDM_SCORING_JOB_ID_{industry.upper()}"
    # Keep env override strict per industry.
    # In Databricks Apps runtimes, service principals may not have enough
    # permission to resolve job metadata via jobs/get or jobs/list, even when
    # they can run a known job ID. Treat explicit env IDs as authoritative.
    env_val = os.getenv(env_key)
    if env_val:
        try:
            job_id = int(env_val)
            state["job_id"] = job_id
            return job_id
        except Exception:
            pass
    try:
        listing = client.api_client.do("GET", "/api/2.1/jobs/list", query={"name": job_name, "limit": "100"})
        jobs = listing.get("jobs", []) if isinstance(listing, dict) else []
        for j in jobs:
            creator = str(j.get("creator_user_name") or "").strip().lower()
            settings = j.get("settings", {}) or {}
            if owner_user and creator and creator != owner_user:
                continue
            if _name_matches(settings.get("name")) and j.get("job_id"):
                state["job_id"] = int(j["job_id"])
                return int(j["job_id"])
    except Exception:
        pass

    # Fallback in case `name` server-side filtering is unavailable.
    try:
        page_token = None
        for _ in range(0, 50):
            query = {"limit": "100"}
            if page_token:
                query["page_token"] = page_token
            listing = client.api_client.do("GET", "/api/2.1/jobs/list", query=query)
            jobs = listing.get("jobs", []) if isinstance(listing, dict) else []
            for j in jobs:
                creator = str(j.get("creator_user_name") or "").strip().lower()
                settings = j.get("settings", {}) or {}
                if owner_user and creator and creator != owner_user:
                    continue
                if _name_matches(settings.get("name")) and j.get("job_id"):
                    state["job_id"] = int(j["job_id"])
                    return int(j["job_id"])
            page_token = listing.get("next_page_token") if isinstance(listing, dict) else None
            if not page_token:
                break
    except Exception:
        pass
    return None


def _resolve_dlt_pipeline_id(client: WorkspaceClient, industry: str) -> str | None:
    state = _LIVE_DLT_STATE.setdefault(industry, {})
    pipeline_name = f"ot-pdm-dlt-{industry}"

    def _name_matches(candidate: Any) -> bool:
        name = str(candidate or "").strip().lower()
        target = pipeline_name.lower()
        if not name:
            return False
        return name == target or name.endswith(f" {target}") or target in name

    cached = state.get("pipeline_id")
    if cached:
        return str(cached)

    env_key = f"OT_PDM_DLT_PIPELINE_ID_{industry.upper()}"
    env_val = str(os.getenv(env_key, "") or "").strip()
    if env_val:
        state["pipeline_id"] = env_val
        return env_val

    try:
        page_token = None
        for _ in range(0, 50):
            query = {"max_results": "100"}
            if page_token:
                query["page_token"] = page_token
            listing = client.api_client.do("GET", "/api/2.0/pipelines", query=query)
            items = listing.get("statuses", []) if isinstance(listing, dict) else []
            for p in items:
                pid = str(p.get("pipeline_id") or "").strip()
                if pid and _name_matches(p.get("name")):
                    state["pipeline_id"] = pid
                    return pid
            page_token = listing.get("next_page_token") if isinstance(listing, dict) else None
            if not page_token:
                break
    except Exception:
        pass

    return None


def _trigger_dlt_update(client: WorkspaceClient, industry: str) -> dict[str, Any]:
    pipeline_id = _resolve_dlt_pipeline_id(client, industry)
    if not pipeline_id:
        return {"ok": False, "error": f"DLT pipeline not found for industry={industry}"}
    try:
        resp = client.api_client.do("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", body={})
        update_id = (resp or {}).get("update_id") if isinstance(resp, dict) else None
        return {"ok": True, "pipeline_id": pipeline_id, "update_id": update_id}
    except Exception as e:
        return {"ok": False, "pipeline_id": pipeline_id, "error": str(e)}


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
    if _SKIP_LIVE_SCORING_ON_READ:
        return
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


def _simulator_landing_targets(industry: str) -> list[str]:
    # Single canonical per-industry landing table for both simulator and
    # connector ingest paths.
    target_fqn, _ = _zerobus_action_target(industry)
    return [target_fqn]


def _simulator_landing_target(industry: str) -> str:
    # Backward-compatible single target used by UI helpers.
    return _simulator_landing_targets(industry)[0]


def _pi_simulated_landing_target(industry: str) -> str:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    return f"{catalog}.bronze.pi_simulated_tags"


def _persist_simulator_rows(industry: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    if WorkspaceClient is None or sql_service is None:
        return
    target_fqns = _simulator_landing_targets(industry)
    client = WorkspaceClient()

    def _exec_checked(statement: str) -> None:
        resp = client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=_WAREHOUSE_ID,
            wait_timeout="50s",
            disposition=sql_service.Disposition.INLINE,
        )
        status = getattr(resp, "status", None)
        if status is None or status.state != sql_service.StatementState.SUCCEEDED:
            state = getattr(status, "state", None) if status is not None else None
            err = getattr(status, "error", None) if status is not None else None
            msg = getattr(err, "message", None) if err is not None else None
            raise RuntimeError(f"SQL statement failed state={state} message={msg or 'unknown error'}")

    def _ensure_schema_for_fqn(target_fqn: str) -> None:
        parts = [p.strip() for p in str(target_fqn).split(".") if p.strip()]
        if len(parts) != 3:
            return
        catalog, schema, _ = parts
        try:
            _exec_checked(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            _exec_checked(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        except Exception as e:
            # If caller can already write to existing table but lacks CREATE on
            # catalog/schema, continue and let downstream INSERT decide.
            if "PERMISSION_DENIED" in str(e):
                return
            raise

    def _landing_table_ddl(target_fqn: str) -> str:
        return f"""
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

    values_sql: list[str] = []
    values_sql_pi: list[str] = []
    for r in rows:
        ts_raw = str(r.get("timestamp", "") or "")
        ts = ts_raw[:19] if len(ts_raw) >= 19 else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        try:
            ts_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            pi_ts = (ts_dt - timedelta(seconds=12)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pi_ts = ts
        base_value = float(r.get("value", 0.0))
        # PI-sim stream: slightly smoothed/lagged variant of OT value.
        pi_value = round(base_value * (1.0 + random.uniform(-0.01, 0.01)), 6)
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
        values_sql_pi.append(
            "("
            + ", ".join(
                [
                    f"'{_sql_escape(str(r.get('site_id', '')))}'",
                    f"'{_sql_escape(str(r.get('area_id', '')))}'",
                    f"'{_sql_escape(str(r.get('unit_id', '')))}'",
                    f"'{_sql_escape(str(r.get('equipment_id', '')))}'",
                    "NULL",
                    f"'{_sql_escape(str(r.get('tag_name', '')))}'",
                    str(pi_value),
                    f"'{_sql_escape(str(r.get('unit', '')))}'",
                    f"'{_sql_escape(str(r.get('quality', 'good')))}'",
                    "'0x00'",
                    "'PI-SIM'",
                    f"TIMESTAMP '{_sql_escape(pi_ts)}'",
                ]
            )
            + ")"
        )

    for target_fqn in target_fqns:
        _ensure_schema_for_fqn(target_fqn)
        if not _SIM_STAGING_READY.get(target_fqn):
            ddl = _landing_table_ddl(target_fqn)
            try:
                _exec_checked(ddl)
                _SIM_STAGING_READY[target_fqn] = True
            except Exception as e:
                # Continue if CREATE TABLE is not allowed; INSERT may still work
                # when table already exists.
                if "PERMISSION_DENIED" not in str(e):
                    raise

        chunk = 200
        for i in range(0, len(values_sql), chunk):
            stmt = (
                f"INSERT INTO {target_fqn} "
                "(site_id, area_id, unit_id, equipment_id, component_id, tag_name, value, unit, quality, quality_code, source_protocol, timestamp) VALUES "
                + ", ".join(values_sql[i : i + chunk])
            )
            try:
                _exec_checked(stmt)
            except Exception as e:
                # Table may be dropped/reset while process cache still marks it ready.
                # Self-heal once by recreating and retrying the same INSERT.
                if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
                    raise
                _SIM_STAGING_READY[target_fqn] = False
                try:
                    _exec_checked(_landing_table_ddl(target_fqn))
                except Exception as ce:
                    if "PERMISSION_DENIED" in str(ce):
                        raise RuntimeError(
                            f"Missing CREATE TABLE permission for {target_fqn}; "
                            "pre-create the table or grant CREATE TABLE on schema."
                        ) from ce
                    raise
                _SIM_STAGING_READY[target_fqn] = True
                _exec_checked(stmt)

    pi_target_fqn = _pi_simulated_landing_target(industry)
    try:
        _ensure_schema_for_fqn(pi_target_fqn)
        if not _SIM_STAGING_READY.get(pi_target_fqn):
            ddl = _landing_table_ddl(pi_target_fqn)
            try:
                _exec_checked(ddl)
                _SIM_STAGING_READY[pi_target_fqn] = True
            except Exception as e:
                # PI stream is additive; do not fail simulator tick if PI table
                # cannot be created due to grants.
                if "PERMISSION_DENIED" in str(e):
                    print(f"[simulator] PI create skipped for {pi_target_fqn}: {e}")
                    return
                raise
        chunk = 200
        for i in range(0, len(values_sql_pi), chunk):
            stmt = (
                f"INSERT INTO {pi_target_fqn} "
                "(site_id, area_id, unit_id, equipment_id, component_id, tag_name, value, unit, quality, quality_code, source_protocol, timestamp) VALUES "
                + ", ".join(values_sql_pi[i : i + chunk])
            )
            try:
                _exec_checked(stmt)
            except Exception as e:
                # Keep OT path healthy even if PI insert fails.
                if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "PERMISSION_DENIED" in str(e):
                    print(f"[simulator] PI insert skipped for {pi_target_fqn}: {e}")
                    return
                raise
    except Exception as e:
        # PI is supplemental context only; never block OT simulator tick on PI.
        if "PERMISSION_DENIED" in str(e) or "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"[simulator] PI path non-fatal for {pi_target_fqn}: {e}")
            return
        raise


def _sensor_features_map(industry: str) -> dict[tuple[str, str], dict[str, Any]]:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    statement = f"""
    SELECT equipment_id, tag_name, mean_15m, stddev_15m, slope_1h, zscore_30d,
           CAST(NULL AS BIGINT) AS reading_count,
           timestamp AS window_end
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY equipment_id, tag_name ORDER BY timestamp DESC) AS rn
      FROM {catalog}.bronze.sensor_features
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


def _asset_data_source(industry: str, asset_id: str) -> str:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    stmt = f"""
    SELECT data_source
    FROM {catalog}.silver.ot_pi_aligned
    WHERE equipment_id = '{_sql_escape(asset_id)}'
    ORDER BY ot_timestamp DESC
    LIMIT 1
    """
    try:
        rows = _run_sql(stmt, cache_key=f"{catalog}:ot_pi_aligned:{asset_id}")
    except Exception:
        return "UNKNOWN"
    if not rows:
        return "UNKNOWN"
    src = str(rows[0].get("data_source") or "").strip().upper()
    if src in {"BOTH", "OT_ONLY"}:
        return src
    return "UNKNOWN"


def _rul_model_metrics(industry: str, asset_id: str, model_version_rul: str | None) -> dict[str, Any]:
    """
    Best-effort lookup for RUL quality metrics from the model version run.
    Returns null metrics when model registry or run metadata is unavailable.
    """
    version = str(model_version_rul or "").strip()
    if not version:
        return _industry_rul_metrics_fallback(industry)
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    model_name = f"{catalog}.models.ot_pdm_rul_{str(asset_id or '').lower()}"
    try:
        import mlflow  # type: ignore

        mlflow.set_registry_uri("databricks-uc")
        client = mlflow.MlflowClient()
        mv = client.get_model_version(model_name, version)
        run_id = str(getattr(mv, "run_id", "") or "").strip()
        if not run_id:
            return {"r2": None, "rmse": None}
        run = client.get_run(run_id)
        metrics = dict(getattr(getattr(run, "data", None), "metrics", {}) or {})
        r2 = metrics.get("r2")
        rmse = metrics.get("rmse")
        resolved = {
            "r2": round(_to_float(r2), 3) if r2 is not None else None,
            "rmse": round(_to_float(rmse), 3) if rmse is not None else None,
        }
        if resolved["r2"] is None or resolved["rmse"] is None:
            fb = _industry_rul_metrics_fallback(industry)
            return {
                "r2": resolved["r2"] if resolved["r2"] is not None else fb.get("r2"),
                "rmse": resolved["rmse"] if resolved["rmse"] is not None else fb.get("rmse"),
            }
        return resolved
    except Exception:
        return _industry_rul_metrics_fallback(industry)


def _industry_rul_metrics_fallback(industry: str) -> dict[str, Any]:
    cached = _RUL_METRICS_FALLBACK.get(industry)
    if cached is not None:
        return cached
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    try:
        import mlflow  # type: ignore

        mlflow.set_registry_uri("databricks-uc")
        client = mlflow.MlflowClient()
        r2_vals: list[float] = []
        rmse_vals: list[float] = []
        # Sample across known assets and stop early once we have enough signal.
        for a in _asset_defs(industry):
            aid = str(a.get("id", "") or "").lower()
            if not aid:
                continue
            model_name = f"{catalog}.models.ot_pdm_rul_{aid}"
            try:
                versions = client.search_model_versions(f"name = '{model_name}'")
            except Exception:
                continue
            if not versions:
                continue
            latest = max(versions, key=lambda v: int(v.version))
            run_id = str(getattr(latest, "run_id", "") or "").strip()
            if not run_id:
                continue
            run = client.get_run(run_id)
            metrics = dict(getattr(getattr(run, "data", None), "metrics", {}) or {})
            r2 = metrics.get("r2")
            rmse = metrics.get("rmse")
            if r2 is not None:
                r2_vals.append(_to_float(r2))
            if rmse is not None:
                rmse_vals.append(_to_float(rmse))
            if len(r2_vals) >= 8 and len(rmse_vals) >= 8:
                break
        if r2_vals and rmse_vals:
            out = {
                "r2": round(sum(r2_vals) / len(r2_vals), 3),
                "rmse": round(sum(rmse_vals) / len(rmse_vals), 3),
            }
            _RUL_METRICS_FALLBACK[industry] = out
            return out
    except Exception:
        pass
    out = dict(_RUL_METRICS_DEFAULTS.get(industry, {"r2": 0.92, "rmse": 16.0}))
    _RUL_METRICS_FALLBACK[industry] = out
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
    target = (zcfg.get("target", {}) or {}) if isinstance(zcfg, dict) else {}
    # Always keep simulator landing writes scoped to the active industry catalog.
    # This prevents cross-industry leakage when a saved connector config points to
    # a different catalog (for example mining while viewing automotive).
    target_catalog = str(catalog).strip()
    # Keep writes pinned to canonical Bronze schema to avoid config drift and
    # invalid saved target schema values causing simulator write failures.
    target_schema = "bronze"
    raw_table = str(target.get("table") or _DEFAULT_ZEROBUS_TARGET_TABLE).strip().strip("`")
    # Accept either plain table name or accidentally saved FQN/table path.
    if "." in raw_table:
        raw_table = raw_table.split(".")[-1]
    target_table = re.sub(r"[^a-zA-Z0-9_]", "", raw_table) or _DEFAULT_ZEROBUS_TARGET_TABLE
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

def _normalize_text(value: Any, fallback: str = "") -> str:
    text = str(value or fallback or "")
    return text.replace("_", " ").title()


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


def _load_genie_room_map(room_type: str = "ops") -> dict[str, str]:
    normalized = str(room_type or "ops").strip().lower()
    is_finance = normalized == "finance"
    env_var = "OT_PDM_FINANCE_GENIE_ROOM_MAP" if is_finance else "OT_PDM_GENIE_ROOM_MAP"
    map_path = FINANCE_GENIE_ROOM_MAP_PATH if is_finance else GENIE_ROOM_MAP_PATH
    env_map = os.getenv(env_var, "").strip()
    if env_map:
        try:
            parsed = json.loads(env_map)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v}
        except Exception:
            pass
    if map_path.exists():
        try:
            parsed = json.loads(map_path.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items() if v}
        except Exception:
            pass
    return {}


def _geo_sites_for_industry(industry: str) -> list[dict[str, Any]]:
    return list(GEO_SITES.get(str(industry or "").lower()) or [])


def _geo_site_for_industry(industry: str) -> dict[str, Any] | None:
    sites = _geo_sites_for_industry(industry)
    return sites[0] if sites else None


def _geo_site_meta(site_id: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    target = str(site_id or "").strip().lower()
    if not target:
        return None, None
    for ind, metas in GEO_SITES.items():
        for meta in metas:
            if str(meta.get("site_id", "")).lower() == target:
                return ind, meta
    return None, None


def _geo_industry_for_site(site_id: str) -> str | None:
    industry, _meta = _geo_site_meta(site_id)
    return industry


def _geo_currency(industry: str, site_id: str = "") -> str:
    if site_id:
        _ind, site_meta = _geo_site_meta(site_id)
        if site_meta:
            return _normalize_currency(str(site_meta.get("currency") or ""), "USD")
    sites = _geo_sites_for_industry(industry)
    if sites:
        return _normalize_currency(str(sites[0].get("currency") or ""), "USD")
    return "USD"


def _geo_assets_for_site(industry: str, site_id: str) -> list[dict[str, Any]]:
    defs = _geo_asset_defs_from_predictions(industry)
    _ind, meta = _geo_site_meta(site_id)
    if not meta:
        return defs
    site_key = _asset_token_norm(str(meta.get("site_key") or ""))
    if not site_key:
        return defs
    filtered = [
        a
        for a in defs
        if _asset_token_norm(str(a.get("site") or "")) == site_key
    ]
    return filtered


def _geo_asset_defs_from_predictions(industry: str) -> list[dict[str, Any]]:
    """Use Gold predictions as Geo equipment source of truth."""
    predictions = _predictions_map(industry)
    defs = _asset_defs(industry)
    if not predictions:
        # Keep Geo fully populated even if Gold is temporarily empty.
        return defs
    defs_by_norm = {_asset_token_norm(str(a.get("id") or "")): a for a in defs if a.get("id")}
    out: list[dict[str, Any]] = []
    seen_norm: set[str] = set()
    for equipment_id in sorted(predictions.keys()):
        key = _asset_token_norm(str(equipment_id))
        seen_norm.add(key)
        base = defs_by_norm.get(key)
        if base:
            merged = dict(base)
            merged["id"] = str(equipment_id)
            out.append(merged)
        else:
            out.append(
                {
                    "id": str(equipment_id),
                    "type": "equipment",
                    "site": "site_1",
                    "area": "area_1",
                    "unit": "unit_1",
                    "model": "",
                }
            )
    # Preserve full ISA view even when only a subset has fresh predictions.
    for d in defs:
        key = _asset_token_norm(str(d.get("id") or ""))
        if key and key not in seen_norm:
            out.append(d)
    return out


def _geo_suggestions(
    alert_message: str,
    severity: str,
    avoided_cost: float,
    intervention_cost: float,
    currency: str = "",
) -> list[str]:
    sev = str(severity or "").lower()
    ccy = str(currency or "").strip().upper()
    jpy_mode = ccy == "JPY"
    krw_mode = ccy == "KRW"
    if sev == "critical":
        s1 = (
            "近い将来の故障を避けるため、現在シフトで保全チームを手配してください。"
            if jpy_mode
            else "근시일 내 고장을 방지하기 위해 현재 교대에 정비팀을 배치하세요."
            if krw_mode
            else "Dispatch maintenance crew in current shift to avoid near-term failure."
        )
    elif sev == "warning":
        s1 = (
            "次の保全ウィンドウで作業を計画し、ドリフトを監視してください。"
            if jpy_mode
            else "다음 정비 윈도우에 작업을 계획하고 드리프트를 모니터링하세요."
            if krw_mode
            else "Schedule maintenance in the next available window and monitor drift."
        )
    else:
        s1 = (
            "監視を継続し、この資産のベースライントレンドを確認してください。"
            if jpy_mode
            else "모니터링을 계속하고 이 자산의 기준 추세를 확인하세요."
            if krw_mode
            else "Continue monitoring and validate baseline trend for this asset."
        )
    if avoided_cost > intervention_cost and avoided_cost > 0:
        s2 = (
            f"この対応を優先してください: 回避コストが介入コストを {int(round(avoided_cost - intervention_cost))} 上回っています。"
            if jpy_mode
            else f"이 조치를 우선하세요: 회피 비용이 개입 비용보다 {int(round(avoided_cost - intervention_cost))} 만큼 큽니다."
            if krw_mode
            else f"Prioritize this action: avoided cost exceeds intervention by {int(round(avoided_cost - intervention_cost))}."
        )
    else:
        s2 = (
            "計画前に介入スコープと部材在庫を再確認してください。"
            if jpy_mode
            else "일정을 확정하기 전에 작업 범위와 부품 재고를 다시 확인하세요."
            if krw_mode
            else "Recheck intervention scope and parts inventory before scheduling."
        )
    s3 = str(
        alert_message
        or (
            "オペレーター推奨を確認し、根本原因シグナルを検証してください。"
            if jpy_mode
            else "운영자 권고를 검토하고 근본 원인 신호를 검증하세요."
            if krw_mode
            else "Review operator recommendation and validate root-cause signals."
        )
    )
    return [s1, s2, s3]


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


def _industry_manual_dir(industry: str) -> Path:
    candidates = [
        ROOT.parent / "industries" / industry / "manuals",
        ROOT / "industries" / industry / "manuals",
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def _manual_text_from_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md"}:
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""
    if suffix == ".pdf":
        try:
            from pypdf import PdfReader  # type: ignore

            reader = PdfReader(str(path))
            out: list[str] = []
            for page in reader.pages:
                txt = page.extract_text() or ""
                if txt.strip():
                    out.append(txt)
            return "\n".join(out)
        except Exception:
            return ""
    return ""


def _manual_text_from_bytes(filename: str, raw: bytes) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix in {".txt", ".md"}:
        try:
            return raw.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    if suffix == ".pdf":
        try:
            from pypdf import PdfReader  # type: ignore

            reader = PdfReader(io.BytesIO(raw))
            out: list[str] = []
            for page in reader.pages:
                txt = page.extract_text() or ""
                if txt.strip():
                    out.append(txt)
            return "\n".join(out)
        except Exception:
            return ""
    return ""


def _manual_tokenize(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9_]{3,}", str(text).lower())}


def _manual_index_table(industry: str) -> str:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    return f"{catalog}.bronze.manual_reference_chunks"


def _manual_chunk_rows(source: str, text: str, chunk_chars: int = 900, overlap: int = 140) -> list[dict[str, Any]]:
    norm = " ".join(str(text or "").split())
    if not norm:
        return []
    rows: list[dict[str, Any]] = []
    i = 0
    chunk_id = 1
    n = len(norm)
    while i < n:
        chunk = norm[i : i + chunk_chars].strip()
        if chunk:
            rows.append(
                {
                    "source": source,
                    "chunk_id": chunk_id,
                    "chunk_text": chunk,
                    "token_count": len(_manual_tokenize(chunk)),
                }
            )
            chunk_id += 1
        if i + chunk_chars >= n:
            break
        i += max(80, chunk_chars - overlap)
    return rows


def _ensure_manual_index_table(industry: str) -> None:
    if WorkspaceClient is None or sql_service is None:
        return
    table_name = _manual_index_table(industry)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      industry STRING,
      source STRING,
      chunk_id INT,
      chunk_text STRING,
      token_count INT,
      updated_at TIMESTAMP
    ) USING DELTA
    """
    try:
        WorkspaceClient().statement_execution.execute_statement(
            statement=ddl,
            warehouse_id=_WAREHOUSE_ID,
            wait_timeout="20s",
            disposition=sql_service.Disposition.INLINE,
        )
    except Exception as e:
        print(f"[manual-index] create table failed: {e}")


def _persist_manual_chunks(industry: str, source: str, text: str) -> int:
    if WorkspaceClient is None or sql_service is None:
        return 0
    rows = _manual_chunk_rows(source, text)
    if not rows:
        return 0
    _ensure_manual_index_table(industry)
    table_name = _manual_index_table(industry)
    try:
        client = WorkspaceClient()
        delete_stmt = (
            f"DELETE FROM {table_name} "
            f"WHERE industry = '{_sql_escape(industry)}' AND source = '{_sql_escape(source)}'"
        )
        client.statement_execution.execute_statement(
            statement=delete_stmt,
            warehouse_id=_WAREHOUSE_ID,
            wait_timeout="20s",
            disposition=sql_service.Disposition.INLINE,
        )
        values_sql: list[str] = []
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for r in rows:
            values_sql.append(
                "("
                + ", ".join(
                    [
                        f"'{_sql_escape(industry)}'",
                        f"'{_sql_escape(str(r['source']))}'",
                        str(int(r["chunk_id"])),
                        f"'{_sql_escape(str(r['chunk_text']))}'",
                        str(int(r["token_count"])),
                        f"TIMESTAMP '{_sql_escape(now_ts)}'",
                    ]
                )
                + ")"
            )
        chunk = 150
        for i in range(0, len(values_sql), chunk):
            insert_stmt = (
                f"INSERT INTO {table_name} (industry, source, chunk_id, chunk_text, token_count, updated_at) VALUES "
                + ", ".join(values_sql[i : i + chunk])
            )
            client.statement_execution.execute_statement(
                statement=insert_stmt,
                warehouse_id=_WAREHOUSE_ID,
                wait_timeout="20s",
                disposition=sql_service.Disposition.INLINE,
            )
    except Exception as e:
        print(f"[manual-index] persist failed for {source}: {e}")
        return 0
    return len(rows)


def _load_manual_chunks(industry: str) -> list[dict[str, Any]]:
    table_name = _manual_index_table(industry)
    stmt = f"""
    SELECT source, chunk_id, chunk_text, token_count
    FROM {table_name}
    WHERE industry = '{_sql_escape(industry)}'
    ORDER BY source, chunk_id
    LIMIT 5000
    """
    return _run_sql(stmt, cache_key=f"{table_name}:{industry}:{int(time.time() // 60)}")


def _manual_kb(industry: str) -> list[dict[str, Any]]:
    if industry in _MANUAL_KB_CACHE:
        return _MANUAL_KB_CACHE[industry]
    docs: list[dict[str, Any]] = []
    chunk_rows = _load_manual_chunks(industry)
    if chunk_rows:
        grouped: dict[str, list[str]] = {}
        for r in chunk_rows:
            src = str(r.get("source") or "")
            if not src:
                continue
            grouped.setdefault(src, []).append(str(r.get("chunk_text") or ""))
        for src, chunks in grouped.items():
            txt = "\n".join(chunks).strip()
            if not txt:
                continue
            compact = " ".join(txt.split())
            docs.append(
                {
                    "source": src,
                    "text": txt,
                    "excerpt": compact[:1200],
                    "tokens": _manual_tokenize(compact),
                }
            )
    else:
        manual_dir = _industry_manual_dir(industry)
        if manual_dir.exists():
            for p in sorted(manual_dir.glob("*")):
                if not p.is_file():
                    continue
                txt = _manual_text_from_file(p).strip()
                if not txt:
                    continue
                _persist_manual_chunks(industry, p.name, txt)
                compact = " ".join(txt.split())
                docs.append(
                    {
                        "source": p.name,
                        "text": txt,
                        "excerpt": compact[:1200],
                        "tokens": _manual_tokenize(compact),
                    }
                )
    _MANUAL_KB_CACHE[industry] = docs
    return docs


def _manual_references(industry: str, query: str, limit: int = 3) -> list[dict[str, Any]]:
    q_tokens = _manual_tokenize(query)
    if not q_tokens:
        return []
    scored: list[tuple[float, dict[str, Any]]] = []
    for d in _manual_kb(industry):
        overlap = len(q_tokens & set(d.get("tokens", set())))
        if overlap <= 0:
            continue
        score = overlap / max(1, len(q_tokens))
        scored.append((score, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    out: list[dict[str, Any]] = []
    for score, d in scored[: max(1, int(limit))]:
        out.append(
            {
                "source": d.get("source", ""),
                "score": round(float(score), 3),
                "excerpt": str(d.get("excerpt", ""))[:420],
            }
        )
    return out


def _advanced_pdm_payload(industry: str, asset_id: str, display_currency: str | None = None) -> dict[str, Any]:
    detail = _asset_detail(industry, asset_id, display_currency=display_currency)
    anomaly = _to_float(detail.get("anomaly_score"), 0.2)
    rul_h = max(1.0, _to_float(detail.get("rul_hours"), 24.0))
    fault_mode = str(detail.get("fault_mode") or "degradation")
    asset_type = str(detail.get("type") or "")

    sensors = _sensor_defs(industry, asset_type)
    by_mode: dict[str, int] = {}
    for s in sensors:
        m = str(s.get("failure_mode") or "degradation")
        by_mode[m] = by_mode.get(m, 0) + 1
    failure_modes: list[dict[str, Any]] = []
    for mode, sensor_cnt in sorted(by_mode.items(), key=lambda x: x[1], reverse=True):
        base = 0.32 + 0.45 * anomaly
        if mode == fault_mode:
            base += 0.22
        likelihood = max(0.05, min(0.99, base))
        confidence = max(0.35, min(0.98, 0.5 + sensor_cnt * 0.06))
        failure_modes.append(
            {
                "mode": mode,
                "likelihood": round(likelihood, 3),
                "confidence": round(confidence, 3),
                "sensor_count": sensor_cnt,
                "priority": "high" if likelihood >= 0.75 else "medium" if likelihood >= 0.5 else "low",
            }
        )

    currency = _effective_demo_currency(display_currency, _industry_cfg(industry).get("agent", {}).get("terminology", {}).get("cost_currency", "USD"))
    profile = _executive_profile(industry, _industry_cfg(industry))
    impact_source_table, impact_row = _financial_impact_latest(industry, asset_id)
    labor_rate = _to_float(profile.get("labor_cost_per_hour"), 0.0)
    dispatch = _to_float(profile.get("dispatch_cost"), 0.0)
    planned_h = max(2.0, min(10.0, rul_h * 0.25))
    planned_cost_native = planned_h * labor_rate + dispatch
    expected_unplanned_native = anomaly * _to_float(profile.get("downtime_cost_per_hour"), 0.0) * max(2.0, min(12.0, rul_h * 0.4))
    native_currency = str(profile.get("currency", currency))
    planned_cost = _fx_convert(planned_cost_native, native_currency, currency)
    expected_unplanned = _fx_convert(expected_unplanned_native, native_currency, currency)

    recommended_window = "within_8h" if anomaly >= 0.8 else "within_24h" if anomaly >= 0.55 else "within_72h"
    fi_event_type = "model_estimate"
    if impact_row:
        fi_event_type = str(impact_row.get("event_type") or "unplanned_failure")
        planned_cost = _fx_convert(_to_float(impact_row.get("maintenance_cost"), planned_cost_native), native_currency, currency)
        expected_unplanned = _fx_convert(_to_float(impact_row.get("expected_failure_cost"), expected_unplanned_native), native_currency, currency)
        if impact_row.get("maintenance_window_start"):
            recommended_window = "scheduled_window"
        elif fi_event_type == "caught_early":
            recommended_window = "within_24h"
        else:
            recommended_window = "expedite_next_shift"
        avoided_native = _to_float(impact_row.get("avoided_cost"), expected_unplanned_native - planned_cost_native)
    else:
        avoided_native = max(0.0, expected_unplanned_native - planned_cost_native)
    avoided_display = _fx_convert(avoided_native, native_currency, currency)

    actions = [
        "Create planned work order aligned to next production window.",
        "Reserve technician and lock spare parts before dispatch.",
        "Run confirmation inspection and close-loop outcome for retraining.",
    ]
    if impact_row:
        if impact_row.get("has_maintenance_window") and impact_row.get("crew_available"):
            actions = [
                "Execute intervention in the scheduled maintenance window.",
                "Pre-stage required parts and assign crew to the selected shift.",
                "Capture post-maintenance result to improve cost-impact calibration.",
            ]
        else:
            actions = [
                "Escalate planner to secure an earlier maintenance window.",
                "Assign available crew and expedite critical spare parts.",
                "Trigger temporary operating limits until intervention is complete.",
            ]

    prescriptive = {
        "recommended_window": recommended_window,
        "expected_avoided_loss": round(max(0.0, avoided_display), 2),
        "expected_avoided_loss_fmt": _fmt_money(max(0.0, avoided_display), currency),
        "planned_intervention_cost": round(planned_cost, 2),
        "planned_intervention_cost_fmt": _fmt_money(planned_cost, currency),
        "expected_failure_cost": round(expected_unplanned, 2),
        "expected_failure_cost_fmt": _fmt_money(expected_unplanned, currency),
        "event_type": fi_event_type,
        "data_source": str((impact_row or {}).get("data_source") or "UNKNOWN"),
        "source_table": impact_source_table,
        "actions": actions,
    }

    parts_rows = _parts_rows(industry)
    parts_plan: list[dict[str, Any]] = []
    for p in parts_rows[:6]:
        qty = int(_to_float(p.get("quantity"), 0.0))
        reorder = int(_to_float(p.get("reorder_point"), 0.0))
        needed = max(1, int(round(1 + anomaly * 4)))
        shortage = max(0, needed - qty)
        parts_plan.append(
            {
                "part_number": p.get("part_number"),
                "description": p.get("description"),
                "quantity_on_hand": qty,
                "reorder_point": reorder,
                "lead_time_days": int(_to_float(p.get("lead_time_days"), 0.0)),
                "required_qty_risk_adjusted": needed,
                "recommended_reorder_qty": max(0, shortage + max(0, reorder - qty)),
                "supply_risk": "high" if shortage > 0 else "medium" if qty <= reorder else "low",
            }
        )

    rng = _asset_rng(industry, asset_id)
    drift = max(0.0, min(0.35, 0.08 + anomaly * 0.14 + rng.uniform(-0.03, 0.04)))
    mlops = {
        "anomaly_model_version": detail.get("model_version_anomaly") or "iforest_v3",
        "rul_model_version": detail.get("model_version_rul") or "xgb_rul_v2",
        "prediction_timestamp": detail.get("prediction_timestamp") or datetime.now(timezone.utc).isoformat(),
        "data_drift_index": round(drift, 3),
        "label_feedback_coverage_pct": round(max(8.0, min(92.0, 24 + (1 - anomaly) * 60)), 1),
        "champion_challenger_gap_pct": round(max(0.5, min(8.0, 1.8 + drift * 10)), 2),
        "retrain_recommended": drift >= 0.18,
    }

    manual_refs = _manual_references(industry, f"{asset_id} {fault_mode} {asset_type}".strip(), limit=3)
    if not manual_refs:
        manual_refs = _manual_references(industry, f"{fault_mode} {asset_type}", limit=3)

    return {
        "asset_id": asset_id,
        "industry": industry,
        "currency": currency,
        "failure_mode_centric": failure_modes,
        "prescriptive_optimizer": prescriptive,
        "spare_parts_risk_planning": parts_plan,
        "mlops_industrial_ai": mlops,
        "manual_references": manual_refs,
    }

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


def _normalize_currency(code: str | None, fallback: str = "USD") -> str:
    c = str(code or "").strip().upper()
    if c in SUPPORTED_CURRENCIES:
        return c
    return str(fallback or "USD").upper()


def _fx_convert(amount: float, from_currency: str, to_currency: str) -> float:
    src = _normalize_currency(from_currency, "USD")
    dst = _normalize_currency(to_currency, "USD")
    if src == dst:
        return _to_float(amount, 0.0)
    per_unit_to_usd = {
        "USD": 1.0,
        "AUD": 0.66,
        "JPY": 0.0067,
        "INR": 0.012,
        "SGD": 0.74,
        "KRW": 0.00074,
    }
    src_to_usd = per_unit_to_usd.get(src, 1.0)
    dst_to_usd = per_unit_to_usd.get(dst, 1.0)
    usd_amount = _to_float(amount, 0.0) * src_to_usd
    return usd_amount / max(1e-9, dst_to_usd)


def _effective_demo_currency(requested_currency: str | None, native_currency: str) -> str:
    env_override = os.getenv("OT_PDM_DEMO_CURRENCY", "").strip().upper()
    if env_override in SUPPORTED_CURRENCIES:
        return env_override
    req = str(requested_currency or "").strip().upper()
    if req in SUPPORTED_CURRENCIES:
        return req
    return _normalize_currency(native_currency, "USD")


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
            "baseline_monthly_ebit": 42000000.0,
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
            "baseline_monthly_ebit": 28000000.0,
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
            "baseline_monthly_ebit": 9000000.0,
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
            "baseline_monthly_ebit": 620000000.0,
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
            "baseline_monthly_ebit": 55000000.0,
        },
    }
    profile = dict(profiles.get(industry, profiles["mining"]))
    profile["currency"] = str(profile.get("currency") or default_currency)
    profile["expedite_parts_multiplier"] = 1.35
    profile["emergency_labor_multiplier"] = 1.4
    profile["planned_parts_ratio"] = 0.45
    return profile


def _sql_row_get(row: dict[str, Any], *candidates: str) -> Any:
    lower_map = {str(k).lower(): k for k in row}
    for c in candidates:
        if c in row:
            return row[c]
        lk = c.lower()
        if lk in lower_map:
            return row[lower_map[lk]]
    return None


def _executive_work_orders_from_lakebase(
    industry: str, assets: list[dict[str, Any]], profile: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Load prescriptive work orders from Lakebase ODS (hydrated from SAP/BDC-shaped bronze)."""
    if _EXECUTIVE_WO_SOURCE == "synthetic":
        return None
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    safe_ind = _sql_escape(industry)
    stmt = f"""
    SELECT
      wo.work_order_id,
      wo.equipment_id,
      wo.priority,
      wo.status,
      wo.work_center,
      wo.cost_center,
      wo.plant_code,
      wo.site_id,
      wo.expected_failure_cost,
      wo.intervention_cost,
      wo.net_ebit_impact,
      wo.avoided_downtime_cost,
      wo.avoided_quality_cost,
      wo.avoided_energy_cost,
      wo.failure_probability,
      wo.rul_hours,
      wo.source_system,
      wo.bdc_session_id,
      wo.amount_currency
    FROM {catalog}.lakebase.work_orders wo
    INNER JOIN {catalog}.bronze.asset_metadata am
      ON wo.equipment_id = am.equipment_id
     AND LOWER(am.industry) = LOWER('{safe_ind}')
    WHERE LOWER(wo.status) IN ('open', 'scheduled', 'submitted')
      AND (wo.source_system IS NULL OR wo.source_system != 'IGNORE_LEGACY')
    ORDER BY
      CASE WHEN LOWER(wo.priority) IN ('p1', 'critical', '1') THEN 0 ELSE 1 END,
      COALESCE(wo.net_ebit_impact, 0) DESC
    LIMIT 12
    """
    try:
        rows = _run_sql(stmt, cache_key=f"{catalog}:lakebase_exec_wo:{industry}:{int(time.time() // 20)}")
    except Exception:
        return None
    if not rows:
        return []
    asset_by_id = {str(a.get("id") or a.get("equipment_id") or ""): a for a in assets}
    orders: list[dict[str, Any]] = []
    for r in rows:
        eid = str(_sql_row_get(r, "equipment_id") or "")
        wo_id = str(_sql_row_get(r, "work_order_id") or "")
        if not wo_id or not eid:
            continue
        ast = asset_by_id.get(eid, {})
        rul = _to_float(_sql_row_get(r, "rul_hours"), _to_float(ast.get("rul_hours"), 72.0))
        fp = _to_float(_sql_row_get(r, "failure_probability"), max(0.22, min(0.98, _to_float(ast.get("anomaly_score"), 0.35))))
        efc = _to_float(_sql_row_get(r, "expected_failure_cost"), 0.0)
        ic = _to_float(_sql_row_get(r, "intervention_cost"), 0.0)
        net = _to_float(_sql_row_get(r, "net_ebit_impact"), max(0.0, efc - ic))
        ad = _to_float(_sql_row_get(r, "avoided_downtime_cost"), 0.0)
        aq = _to_float(_sql_row_get(r, "avoided_quality_cost"), 0.0)
        ae = _to_float(_sql_row_get(r, "avoided_energy_cost"), 0.0)
        pri = str(_sql_row_get(r, "priority") or "P2")
        st = str(_sql_row_get(r, "status") or "open")
        wc = str(_sql_row_get(r, "work_center") or "Operations")
        cc = str(_sql_row_get(r, "cost_center") or (profile.get("cost_centers") or ["OPS-000"])[0])
        src = str(_sql_row_get(r, "source_system") or "lakebase")
        bdc = str(_sql_row_get(r, "bdc_session_id") or "")
        pl = str(_sql_row_get(r, "plant_code") or profile.get("plant_code") or "")
        orders.append(
            {
                "wo_id": wo_id,
                "equipment_id": eid,
                "priority": pri,
                "status": st.upper() if st else "OPEN",
                "work_center": wc,
                "cost_center": cc,
                "plant_code": pl,
                "planner_group": str(profile.get("planner_group", "PLAN-DEFAULT")),
                "failure_probability": round(fp, 3),
                "expected_failure_cost": round(efc, 2),
                "intervention_cost": round(ic, 2),
                "net_ebit_impact": round(net, 2),
                "avoided_downtime_cost": round(ad, 2),
                "avoided_quality_cost": round(aq, 2),
                "avoided_energy_cost": round(ae, 2),
                "rul_hours": rul,
                "source_system": src,
                "bdc_session_id": bdc,
            }
        )
    return orders if orders else []


def _executive_work_orders(
    industry: str, assets: list[dict[str, Any]], profile: dict[str, Any]
) -> list[dict[str, Any]]:
    scoped = [a for a in assets if a.get("status") != "healthy"]
    # Keep executive work-order panel populated even when current telemetry has no active alerts.
    if not scoped:
        scoped = sorted(
            assets,
            key=lambda a: _to_float(a.get("anomaly_score"), 0.0),
            reverse=True,
        )[:6]
    scoped.sort(key=lambda a: _to_float(a.get("anomaly_score"), 0.0), reverse=True)
    center_list = profile.get("cost_centers", [])
    work_centers = profile.get("work_centers", [])
    prefix = industry[:3].upper()
    orders: list[dict[str, Any]] = []
    for idx, asset in enumerate(scoped[:8], start=1):
        anomaly = max(0.22, min(0.98, _to_float(asset.get("anomaly_score"), 0.25)))
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


def _financial_daily_rows(industry: str, days: int = 760) -> tuple[str, list[dict[str, Any]]]:
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    table_fqn = f"{catalog}.finance.pm_financial_daily"
    stmt = f"""
    SELECT
      ds,
      industry,
      currency,
      avoided_downtime_cost,
      avoided_quality_cost,
      avoided_energy_cost,
      intervention_cost,
      platform_cost,
      ebit_saved,
      net_benefit,
      baseline_monthly_ebit
    FROM {table_fqn}
    WHERE industry = '{industry}'
      AND ds >= date_sub(current_date(), {max(30, int(days))})
    ORDER BY ds
    """
    rows = _run_sql(stmt, cache_key=f"{table_fqn}:{industry}:{int(time.time() // 30)}")
    return table_fqn, rows


def _financial_impact_latest(industry: str, asset_id: str) -> tuple[str, dict[str, Any] | None]:
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    table_fqn = f"{catalog}.gold.financial_impact_events"
    stmt = f"""
    SELECT
      equipment_id,
      prediction_timestamp,
      severity,
      anomaly_score,
      rul_hours,
      event_type,
      shift_label,
      maintenance_window_start,
      maintenance_window_end,
      has_maintenance_window,
      crew_available,
      downtime_hours,
      maintenance_cost,
      production_loss,
      expected_failure_cost,
      avoided_cost,
      total_event_cost,
      data_source,
      source_table
    FROM {table_fqn}
    WHERE equipment_id = '{_sql_escape(asset_id)}'
    ORDER BY prediction_timestamp DESC
    LIMIT 1
    """
    try:
        rows = _run_sql(stmt, cache_key=f"{table_fqn}:{asset_id}:{int(time.time() // 30)}")
    except Exception:
        return table_fqn, None
    return table_fqn, (rows[0] if rows else None)


def _month_key(ds: str) -> str:
    s = str(ds or "")
    return s[:7] if len(s) >= 7 else ""


def _adoption_insights(
    industry: str,
    assets: list[dict[str, Any]],
    work_orders: list[dict[str, Any]],
    currency: str,
    ebit_saved: float,
    intervention_cost: float,
    platform_cost: float,
) -> dict[str, Any]:
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")

    # Build per-site asset inventory from configured ISA definitions (site keys),
    # then fall back to any runtime-only asset rows.
    assets_by_site: dict[str, set[str]] = {}
    asset_to_site: dict[str, str] = {}
    for a in _asset_defs(industry):
        aid = str(a.get("id") or a.get("equipment_id") or "")
        site = str(a.get("site") or a.get("site_id") or "")
        if not aid or not site:
            continue
        assets_by_site.setdefault(site, set()).add(aid)
        asset_to_site[aid] = site
    for a in assets:
        aid = str(a.get("id") or a.get("equipment_id") or "")
        site = str(a.get("site_id") or "")
        if aid and site and aid not in asset_to_site:
            assets_by_site.setdefault(site, set()).add(aid)
            asset_to_site[aid] = site

    live_alerted_by_site: dict[str, int] = {}
    for a in assets:
        aid = str(a.get("id") or a.get("equipment_id") or "")
        sid = asset_to_site.get(aid, "")
        if not sid:
            continue
        status = str(a.get("status") or "").lower()
        if status in {"critical", "warning"}:
            live_alerted_by_site[sid] = live_alerted_by_site.get(sid, 0) + 1

    # Site metadata (all configured sites for the industry, even if sparse telemetry).
    site_meta_map: dict[str, dict[str, Any]] = {
        str(s.get("site_key") or ""): s for s in GEO_SITES.get(industry, []) if s.get("site_key")
    }

    site_events: dict[str, dict[str, Any]] = {}
    site_finance: dict[str, dict[str, Any]] = {}
    site_work_orders: dict[str, dict[str, Any]] = {}

    try:
        rows = _run_sql(
            f"""
            SELECT
              site_id,
              COUNT(*) AS events_30d,
              COUNT(DISTINCT equipment_id) AS alerted_assets_30d,
              SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) AS critical_30d,
              SUM(CASE WHEN severity='warning' THEN 1 ELSE 0 END) AS warning_30d,
              SUM(COALESCE(avoided_cost, 0)) AS avoided_cost_30d,
              SUM(COALESCE(maintenance_cost, 0)) AS intervention_cost_30d
            FROM {catalog}.gold.financial_impact_events
            WHERE prediction_timestamp >= current_timestamp() - INTERVAL 30 DAYS
            GROUP BY site_id
            """,
            cache_key=f"{catalog}:adoption:site_events:{int(time.time() // 45)}",
        )
        for r in rows:
            sid = str(r.get("site_id") or "")
            if sid:
                site_events[sid] = r
    except Exception:
        site_events = {}

    try:
        rows = _run_sql(
            f"""
            SELECT site_id, avoided_cost, intervention_cost, net_benefit, critical_assets, warning_assets
            FROM (
              SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY ds DESC) AS rn
              FROM {catalog}.finance.pm_site_financial_daily
            ) t
            WHERE rn = 1
            """,
            cache_key=f"{catalog}:adoption:site_finance:{int(time.time() // 60)}",
        )
        for r in rows:
            sid = str(r.get("site_id") or "")
            if sid:
                site_finance[sid] = r
    except Exception:
        site_finance = {}

    try:
        rows = _run_sql(
            f"""
            SELECT
              site_id,
              SUM(CASE WHEN lower(status) = 'open' THEN 1 ELSE 0 END) AS open_work_orders,
              SUM(CASE WHEN lower(status) = 'scheduled' THEN 1 ELSE 0 END) AS scheduled_work_orders,
              COUNT(*) AS total_work_orders
            FROM {catalog}.lakebase.work_orders
            GROUP BY site_id
            """,
            cache_key=f"{catalog}:adoption:work_orders:{int(time.time() // 60)}",
        )
        for r in rows:
            sid = str(r.get("site_id") or "")
            if sid:
                site_work_orders[sid] = r
    except Exception:
        site_work_orders = {}

    try:
        actioned_assets = _recommendation_actioned_assets(industry)
    except Exception:
        actioned_assets = set()
    actioned_by_site: dict[str, int] = {}
    for aid in actioned_assets:
        sid = asset_to_site.get(str(aid))
        if sid:
            actioned_by_site[sid] = actioned_by_site.get(sid, 0) + 1
    # Fallback proxy: if explicit action logs are unavailable, infer operational
    # adoption from recommendation/work-order execution intent in the current window.
    proxy_actioned_by_site: dict[str, int] = {}
    for w in work_orders:
        aid = str(w.get("equipment_id") or "")
        sid = asset_to_site.get(aid, "")
        if not sid:
            continue
        status = str(w.get("status") or "").strip().lower()
        if status in {"approved", "scheduled", "recommended", "open", "submitted", "in_progress", "in progress"}:
            proxy_actioned_by_site[sid] = proxy_actioned_by_site.get(sid, 0) + 1

    maturity_rows: list[dict[str, Any]] = []
    all_site_keys = sorted(set(site_meta_map.keys()) | set(assets_by_site.keys()) | set(site_events.keys()) | set(site_finance.keys()) | set(site_work_orders.keys()))
    for sid in all_site_keys:
        meta = site_meta_map.get(sid, {})
        total_assets = len(assets_by_site.get(sid, set()))
        ev = site_events.get(sid, {})
        fin = site_finance.get(sid, {})
        wo = site_work_orders.get(sid, {})

        alerted_assets = int(_to_float(ev.get("alerted_assets_30d"), 0))
        if alerted_assets <= 0:
            alerted_assets = int(_to_float(fin.get("critical_assets"), 0)) + int(_to_float(fin.get("warning_assets"), 0))
        if alerted_assets <= 0:
            alerted_assets = int(live_alerted_by_site.get(sid, 0))
        alerted_assets = max(0, alerted_assets)

        actioned = int(actioned_by_site.get(sid, 0))
        if actioned <= 0:
            actioned = int(proxy_actioned_by_site.get(sid, 0))
        action_rate = (actioned / max(1, alerted_assets)) * 100.0 if alerted_assets > 0 else 0.0
        prediction_coverage = (alerted_assets / max(1, total_assets)) * 100.0 if total_assets > 0 else 0.0

        open_wo = int(_to_float(wo.get("open_work_orders"), 0))
        sched_wo = int(_to_float(wo.get("scheduled_work_orders"), 0))
        total_wo = int(_to_float(wo.get("total_work_orders"), open_wo + sched_wo))

        # Proxy for site engagement when direct audit query volume is unavailable.
        genie_queries_30d = max(0, int((alerted_assets * 8) + (actioned * 12) + (total_wo * 2)))

        maturity_score = (
            (action_rate * 0.55)
            + (prediction_coverage * 0.25)
            + (min(100.0, (genie_queries_30d / 120.0) * 100.0) * 0.20)
        )
        maturity_score = round(max(0.0, min(100.0, maturity_score)), 1)
        if maturity_score >= 75:
            tier = "Leader"
        elif maturity_score >= 50:
            tier = "Advancing"
        else:
            tier = "Emerging"

        maturity_rows.append(
            {
                "site_id": sid,
                "site_name": str(meta.get("name") or sid),
                "customer": str(meta.get("customer") or ""),
                "action_rate_pct": round(action_rate, 1),
                "prediction_coverage_pct": round(prediction_coverage, 1),
                "genie_queries_30d": genie_queries_30d,
                "alerted_assets_30d": alerted_assets,
                "actioned_assets_30d": actioned,
                "open_work_orders": open_wo,
                "scheduled_work_orders": sched_wo,
                "maturity_score": maturity_score,
                "tier": tier,
            }
        )

    maturity_rows.sort(key=lambda r: (r.get("maturity_score", 0), r.get("action_rate_pct", 0)), reverse=True)

    total_alerted_assets = sum(int(r.get("alerted_assets_30d", 0)) for r in maturity_rows)
    total_actioned_assets = sum(int(r.get("actioned_assets_30d", 0)) for r in maturity_rows)
    total_genie_queries = sum(int(r.get("genie_queries_30d", 0)) for r in maturity_rows)
    sites_with_predictions = sum(1 for r in maturity_rows if int(r.get("alerted_assets_30d", 0)) > 0)
    total_sites = max(1, len(maturity_rows))

    action_rate_global = (total_actioned_assets / max(1, total_alerted_assets)) * 100.0 if total_alerted_assets > 0 else 0.0
    event_count_30d = sum(int(_to_float(v.get("events_30d"), 0)) for v in site_events.values())
    if event_count_30d <= 0:
        event_count_30d = max(0, total_alerted_assets)
    avoided_30d = sum(_to_float(v.get("avoided_cost_30d"), 0.0) for v in site_events.values())
    intervention_30d = sum(_to_float(v.get("intervention_cost_30d"), 0.0) for v in site_events.values())
    if avoided_30d <= 0:
        avoided_30d = sum(_to_float(v.get("avoided_cost"), 0.0) for v in site_finance.values())
    if intervention_30d <= 0:
        intervention_30d = sum(_to_float(v.get("intervention_cost"), 0.0) for v in site_finance.values())

    cost_per_prediction = (intervention_30d / max(1, event_count_30d)) if event_count_30d > 0 else 0.0
    avoided_per_prediction = (avoided_30d / max(1, event_count_30d)) if event_count_30d > 0 else 0.0
    invested = max(1.0, intervention_cost + platform_cost)
    platform_roi_x = max(0.0, ebit_saved / invested)
    mttr_improvement_pct = round(min(35.0, 6.0 + (action_rate_global * 0.12)), 1)
    recurrence_reduction_pct = round(min(35.0, 8.0 + (action_rate_global * 0.15)), 1)

    return {
        "model_utilization_rate_pct": round(action_rate_global, 1),
        "genie_queries_30d": int(total_genie_queries),
        "predictions_consumed_sites": int(sites_with_predictions),
        "total_sites": int(total_sites),
        "cost_per_prediction": round(cost_per_prediction, 2),
        "cost_per_prediction_fmt": _fmt_money(cost_per_prediction, currency),
        "avoided_cost_per_prediction": round(avoided_per_prediction, 2),
        "avoided_cost_per_prediction_fmt": _fmt_money(avoided_per_prediction, currency),
        "platform_roi_x": round(platform_roi_x, 1),
        "mttr_improvement_pct": mttr_improvement_pct,
        "failure_recurrence_reduction_pct": recurrence_reduction_pct,
        "site_maturity": maturity_rows,
        "data_mode": "observed_plus_proxy",
        "summary_text": (
            f"{sites_with_predictions}/{total_sites} sites are actively consuming predictions. "
            f"Model utilization is {action_rate_global:.1f}% with {total_genie_queries} Genie finance interactions in 30d."
        ),
    }


def _executive_value(industry: str, assets: list[dict[str, Any]], display_currency: str | None = None) -> dict[str, Any]:
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    accounts = cfg.get("accounts", {}) or {}
    profile = _executive_profile(industry, cfg)
    native_currency = str(profile.get("currency", "USD"))
    currency = _effective_demo_currency(display_currency, native_currency)
    wo_lake = _executive_work_orders_from_lakebase(industry, assets, profile)
    if _EXECUTIVE_WO_SOURCE == "synthetic":
        work_orders = _executive_work_orders(industry, assets, profile)
        work_order_source = "synthetic_model"
    elif _EXECUTIVE_WO_SOURCE == "lakebase":
        work_orders = list(wo_lake or [])
        work_order_source = "lakebase_ods"
    else:
        if wo_lake:
            work_orders = wo_lake
            work_order_source = "lakebase_ods"
        else:
            work_orders = _executive_work_orders(industry, assets, profile)
            work_order_source = "synthetic_model"

    avoided_downtime_native = sum(_to_float(w.get("avoided_downtime_cost"), 0.0) for w in work_orders)
    avoided_quality_native = sum(_to_float(w.get("avoided_quality_cost"), 0.0) for w in work_orders)
    avoided_energy_native = sum(_to_float(w.get("avoided_energy_cost"), 0.0) for w in work_orders)
    intervention_cost_native = sum(_to_float(w.get("intervention_cost"), 0.0) for w in work_orders)
    platform_cost_native = _to_float(profile.get("platform_cost_monthly_alloc"), 0.0)
    net_benefit_native = (
        avoided_downtime_native
        + avoided_quality_native
        + avoided_energy_native
        - intervention_cost_native
        - platform_cost_native
    )
    ebit_saved_native = max(0.0, net_benefit_native)
    invested_native = max(1.0, intervention_cost_native + platform_cost_native)
    roi_pct = (ebit_saved_native / invested_native) * 100.0
    payback_days = 999.0
    if ebit_saved_native > 0:
        daily = ebit_saved_native / 30.0
        payback_days = invested_native / max(1.0, daily)

    baseline_monthly_ebit_native = _to_float(profile.get("baseline_monthly_ebit"), 1.0)
    ebit_margin_bps = (ebit_saved_native / max(1.0, baseline_monthly_ebit_native)) * 10000.0

    def _cv(amount: float) -> float:
        return _fx_convert(amount, native_currency, currency)

    avoided_downtime = _cv(avoided_downtime_native)
    avoided_quality = _cv(avoided_quality_native)
    avoided_energy = _cv(avoided_energy_native)
    intervention_cost = _cv(intervention_cost_native)
    platform_cost = _cv(platform_cost_native)
    net_benefit = _cv(net_benefit_native)
    ebit_saved = _cv(ebit_saved_native)

    for w in work_orders:
        w["expected_failure_cost"] = round(_cv(_to_float(w["expected_failure_cost"])), 2)
        w["intervention_cost"] = round(_cv(_to_float(w["intervention_cost"])), 2)
        w["net_ebit_impact"] = round(_cv(_to_float(w["net_ebit_impact"])), 2)
        w["expected_failure_cost_fmt"] = _fmt_money(_to_float(w["expected_failure_cost"]), currency)
        w["intervention_cost_fmt"] = _fmt_money(_to_float(w["intervention_cost"]), currency)
        w["net_ebit_impact_fmt"] = _fmt_money(_to_float(w["net_ebit_impact"]), currency)

    source_table, financial_rows = _financial_daily_rows(industry)
    data_mode = "simulated_work_order_model"
    source_window = "last_30_days"
    mom_ebit_pct = 0.0
    yoy_ebit_pct = 0.0
    if financial_rows:
        data_mode = "financial_daily_table"
        source_window = "SUM(ds >= date_sub(current_date(), 30))"
        last = financial_rows[-1]
        recent_rows = financial_rows[-30:] if len(financial_rows) >= 30 else financial_rows
        native_from_table = str(last.get("currency") or native_currency)
        avoided_downtime_native = sum(_to_float(r.get("avoided_downtime_cost"), 0.0) for r in recent_rows)
        avoided_quality_native = sum(_to_float(r.get("avoided_quality_cost"), 0.0) for r in recent_rows)
        avoided_energy_native = sum(_to_float(r.get("avoided_energy_cost"), 0.0) for r in recent_rows)
        intervention_cost_native = sum(_to_float(r.get("intervention_cost"), 0.0) for r in recent_rows)
        platform_cost_native = sum(_to_float(r.get("platform_cost"), 0.0) for r in recent_rows)
        net_benefit_native = sum(_to_float(r.get("net_benefit"), 0.0) for r in recent_rows)
        ebit_saved_native = sum(_to_float(r.get("ebit_saved"), 0.0) for r in recent_rows)
        avoided_downtime = _fx_convert(avoided_downtime_native, native_from_table, currency)
        avoided_quality = _fx_convert(avoided_quality_native, native_from_table, currency)
        avoided_energy = _fx_convert(avoided_energy_native, native_from_table, currency)
        intervention_cost = _fx_convert(intervention_cost_native, native_from_table, currency)
        platform_cost = _fx_convert(platform_cost_native, native_from_table, currency)
        net_benefit = _fx_convert(net_benefit_native, native_from_table, currency)
        ebit_saved = _fx_convert(ebit_saved_native, native_from_table, currency)
        baseline_monthly_ebit_native = _to_float(last.get("baseline_monthly_ebit"), baseline_monthly_ebit_native)
        ebit_margin_bps = (ebit_saved_native / max(1.0, baseline_monthly_ebit_native)) * 10000.0
        roi_pct = (max(0.0, ebit_saved_native) / max(1.0, intervention_cost_native + platform_cost_native)) * 100.0

        month_totals: dict[str, float] = {}
        for r in financial_rows:
            mk = _month_key(str(r.get("ds") or ""))
            if not mk:
                continue
            month_totals[mk] = month_totals.get(mk, 0.0) + _to_float(r.get("ebit_saved"), 0.0)
        month_keys = sorted(month_totals.keys())
        if len(month_keys) >= 2:
            cur = month_totals[month_keys[-1]]
            prev = month_totals[month_keys[-2]]
            mom_ebit_pct = ((cur - prev) / abs(prev) * 100.0) if abs(prev) > 1e-9 else 0.0

        if len(financial_rows) >= 370:
            recent_30 = sum(_to_float(r.get("ebit_saved"), 0.0) for r in financial_rows[-30:])
            prev_year_30 = sum(_to_float(r.get("ebit_saved"), 0.0) for r in financial_rows[-395:-365])
            yoy_ebit_pct = ((recent_30 - prev_year_30) / abs(prev_year_30) * 100.0) if abs(prev_year_30) > 1e-9 else 0.0

        trend_months = month_keys[-6:]
        ebit_trend = []
        for mk in trend_months:
            v_native = month_totals.get(mk, 0.0)
            v = _fx_convert(v_native, native_from_table, currency)
            label = mk[5:] + "/" + mk[2:4] if len(mk) == 7 else mk
            ebit_trend.append({"label": label, "value": round(v, 2), "value_fmt": _fmt_money(v, currency)})

    if not work_orders:
        # Fallback: derive lightweight recommended work orders from top assets so
        # executive "Financial impact by work order" is never empty.
        synthetic_assets = sorted(
            assets,
            key=lambda a: _to_float(a.get("anomaly_score"), 0.0),
            reverse=True,
        )[:6]
        if synthetic_assets:
            weight_sum = sum(max(0.08, _to_float(a.get("anomaly_score"), 0.2)) for a in synthetic_assets)
            centers = profile.get("cost_centers", []) or ["OPS-000"]
            works = profile.get("work_centers", []) or ["Operations"]
            prefix = industry[:3].upper()
            for idx, a in enumerate(synthetic_assets, start=1):
                w = max(0.08, _to_float(a.get("anomaly_score"), 0.2)) / max(1e-9, weight_sum)
                wo_intervention = intervention_cost * w
                wo_impact = max(0.0, net_benefit * w)
                work_orders.append(
                    {
                        "wo_id": f"{prefix}-WO-{2200 + idx}",
                        "equipment_id": str(a.get("id") or a.get("equipment_id") or ""),
                        "priority": "P2",
                        "status": "RECOMMENDED",
                        "work_center": works[(idx - 1) % max(1, len(works))],
                        "cost_center": centers[(idx - 1) % max(1, len(centers))],
                        "planner_group": profile.get("planner_group", "PLAN-DEFAULT"),
                        "failure_probability": round(max(0.2, _to_float(a.get("anomaly_score"), 0.2)), 3),
                        "expected_failure_cost": round(max(wo_impact + wo_intervention, wo_intervention * 1.5), 2),
                        "intervention_cost": round(wo_intervention, 2),
                        "net_ebit_impact": round(wo_impact, 2),
                        "avoided_downtime_cost": round(avoided_downtime * w, 2),
                        "avoided_quality_cost": round(avoided_quality * w, 2),
                        "avoided_energy_cost": round(avoided_energy * w, 2),
                        "rul_hours": round(max(8.0, _to_float(a.get("rul_hours"), 72.0)), 1),
                    }
                )

    for w in work_orders:
        w["expected_failure_cost_fmt"] = _fmt_money(_to_float(w.get("expected_failure_cost"), 0.0), currency)
        w["intervention_cost_fmt"] = _fmt_money(_to_float(w.get("intervention_cost"), 0.0), currency)
        w["net_ebit_impact_fmt"] = _fmt_money(_to_float(w.get("net_ebit_impact"), 0.0), currency)

    value_bridge = [
        {
            "label": "Avoided unplanned downtime",
            "kind": "positive",
            "amount": round(avoided_downtime, 2),
            "amount_fmt": _fmt_money(avoided_downtime, currency),
            "tooltip": (
                f"Source: {source_table}.avoided_downtime_cost | Aggregation: {source_window} | "
                f"Metric: SUM(avoided_downtime_cost) | Mode: {data_mode}"
            ),
        },
        {
            "label": "Avoided quality and scrap loss",
            "kind": "positive",
            "amount": round(avoided_quality, 2),
            "amount_fmt": _fmt_money(avoided_quality, currency),
            "tooltip": (
                f"Source: {source_table}.avoided_quality_cost | Aggregation: {source_window} | "
                f"Metric: SUM(avoided_quality_cost) | Mode: {data_mode}"
            ),
        },
        {
            "label": "Avoided energy waste",
            "kind": "positive",
            "amount": round(avoided_energy, 2),
            "amount_fmt": _fmt_money(avoided_energy, currency),
            "tooltip": (
                f"Source: {source_table}.avoided_energy_cost | Aggregation: {source_window} | "
                f"Metric: SUM(avoided_energy_cost) | Mode: {data_mode}"
            ),
        },
        {
            "label": "Planned intervention cost",
            "kind": "negative",
            "amount": round(-intervention_cost, 2),
            "amount_fmt": _fmt_money(-intervention_cost, currency),
            "tooltip": (
                f"Source: {source_table}.intervention_cost | Aggregation: {source_window} | "
                f"Metric: -SUM(intervention_cost) | Mode: {data_mode}"
            ),
        },
        {
            "label": "Platform and operations allocation",
            "kind": "negative",
            "amount": round(-platform_cost, 2),
            "amount_fmt": _fmt_money(-platform_cost, currency),
            "tooltip": (
                f"Source: {source_table}.platform_cost | Aggregation: {source_window} | "
                f"Metric: -SUM(platform_cost) | Mode: {data_mode}"
            ),
        },
    ]

    if not financial_rows:
        trend_scale = [0.72, 0.81, 0.88, 0.94, 1.0, 1.06]
        trend_labels = ["M-5", "M-4", "M-3", "M-2", "M-1", "Current"]
        ebit_trend = []
        for lbl, mult in zip(trend_labels, trend_scale):
            v = ebit_saved * mult
            ebit_trend.append({"label": lbl, "value": round(v, 2), "value_fmt": _fmt_money(v, currency)})

    ranked_work_orders = sorted(work_orders, key=lambda w: _to_float(w.get("net_ebit_impact"), 0.0), reverse=True)
    top_decisions = ranked_work_orders[:3]
    decision_value_30 = sum(_to_float(w.get("net_ebit_impact"), 0.0) for w in top_decisions)
    decision_value_90 = decision_value_30 * 2.6
    protected_no_action_30 = max(0.0, ebit_saved - decision_value_30)
    protected_no_action_90 = max(0.0, (ebit_saved * 2.6) - decision_value_90)
    confidence_pct = 68.0 + (12.0 if data_mode == "financial_daily_table" else 0.0) + min(14.0, len(financial_rows) / 55.0)
    confidence_pct = max(55.0, min(96.0, confidence_pct))

    annualized_ebit_saved = ebit_saved * 12.0
    annual_target = max(1.0, _cv(baseline_monthly_ebit_native) * 12.0 * 0.02)
    run_rate_to_target_pct = (annualized_ebit_saved / annual_target) * 100.0

    portfolio_rows = sorted(
        [
            {
                "asset_id": str(a.get("id") or a.get("equipment_id") or ""),
                "status": str(a.get("status") or "healthy"),
                "anomaly_score": round(_to_float(a.get("anomaly_score"), 0.0), 2),
                "exposure_value": round(_to_float(a.get("cost_exposure_value"), 0.0), 2),
                "exposure_fmt": _fmt_money(_to_float(a.get("cost_exposure_value"), 0.0), currency),
            }
            for a in assets
        ],
        key=lambda r: _to_float(r.get("exposure_value"), 0.0),
        reverse=True,
    )
    top_exposure = sum(_to_float(r.get("exposure_value"), 0.0) for r in portfolio_rows[:5])
    all_exposure = sum(_to_float(r.get("exposure_value"), 0.0) for r in portfolio_rows)
    concentration_top5_pct = ((top_exposure / all_exposure) * 100.0) if all_exposure > 1e-9 else 0.0

    decision_cards = []
    for w in top_decisions:
        v = _to_float(w.get("net_ebit_impact"), 0.0)
        c = max(1.0, _to_float(w.get("intervention_cost"), 0.0))
        payback_local = c / max(1.0, v / 30.0) if v > 0 else 999.0
        disruption = 7 if str(w.get("priority")) == "P1" else 4
        decision_cards.append(
            {
                "title": f"Approve {w.get('wo_id', 'WO')} intervention",
                "equipment_id": w.get("equipment_id"),
                "value_uplift": round(v, 2),
                "value_uplift_fmt": _fmt_money(v, currency),
                "cost_fmt": _fmt_money(c, currency),
                "payback_days": round(payback_local, 1),
                "disruption_score": disruption,
                "confidence_pct": round(min(98.0, 72.0 + (_to_float(w.get('failure_probability'), 0.5) * 20.0)), 1),
            }
        )

    adoption_insights = _adoption_insights(
        industry=industry,
        assets=assets,
        work_orders=work_orders,
        currency=currency,
        ebit_saved=ebit_saved,
        intervention_cost=intervention_cost,
        platform_cost=platform_cost,
    )

    if currency == "JPY":
        value_statement = f"処方保全により、EBIT上振れ効果は{_fmt_money(ebit_saved, currency)}です。"
    elif currency == "KRW":
        value_statement = f"처방 정비를 통해 EBIT 상향 효과 {_fmt_money(ebit_saved, currency)}를 확보했습니다."
    else:
        value_statement = f"Prescriptive maintenance unlocked EBIT upside of {_fmt_money(ebit_saved, currency)}."

    return {
        "audience": "finance_executive",
        "window": "last_30_days",
        "currency": currency,
        "value_statement": value_statement,
        "ebit_saved": round(ebit_saved, 2),
        "ebit_saved_fmt": _fmt_money(ebit_saved, currency),
        "net_benefit": round(net_benefit, 2),
        "net_benefit_fmt": _fmt_money(net_benefit, currency),
        "roi_pct": round(roi_pct, 1),
        "payback_days": round(payback_days, 1),
        "ebit_margin_bps": round(ebit_margin_bps, 1),
        "explainability": {
            "ebit_saved": "EBIT Saved = (avoided downtime + avoided quality/scrap + avoided energy) - (planned intervention + platform allocation).",
            "roi_pct": "ROI = EBIT Saved / (planned intervention + platform allocation).",
            "payback_days": "Payback days = (planned intervention + platform allocation) / (EBIT Saved / 30).",
            "ebit_margin_bps": "EBIT Margin Lift (bps) = EBIT Saved / baseline monthly EBIT * 10,000.",
            "mom_ebit_pct": "MoM compares current month EBIT Saved vs prior month from daily finance rows.",
            "yoy_ebit_pct": "YoY compares last 30 days EBIT Saved vs the same 30-day period one year earlier.",
            "baseline_monthly_ebit": f"Baseline monthly EBIT reference for {industry} scenario: {_fmt_money(_cv(baseline_monthly_ebit_native), currency)}.",
            "source_table": f"Primary source table: {source_table}. Aggregation window: {source_window}. Data mode: {data_mode}.",
            "work_orders": (
                "Work orders loaded from Lakebase ODS (SAP BDC → bronze → hydrate) when available; "
                "otherwise ranked from anomaly risk. Values convert to display currency."
                if work_order_source == "lakebase_ods"
                else "Work orders are ranked by anomaly risk and mapped to plant work centers/cost centers; values are converted to display currency."
            ),
            "work_order_net_ebit_impact": "Net EBIT impact per work order is expected failure cost avoided minus planned intervention cost.",
            "confidence_pct": "Confidence score reflects data recency, data mode, and sample depth from daily financial rows.",
            "run_rate_to_target_pct": "Run-rate to target = annualized current EBIT saved / annual EBIT target.",
            "concentration_top5_pct": "Portfolio concentration = share of exposure represented by top 5 assets.",
            "protected_30_with_actions": "Projected EBIT protected over 30 days if top recommended interventions are executed.",
            "protected_30_without_actions": "Projected EBIT protected over 30 days if top interventions are deferred.",
            "model_utilization_rate_pct": "Model utilization rate = actioned assets / alerted assets in the last 30 days.",
            "site_maturity": "Site maturity score blends action rate, prediction coverage, and engagement volume proxies.",
            "platform_roi_x": "Platform ROI (x) = EBIT saved / (intervention cost + platform cost).",
            "cost_per_prediction": "Cost per prediction = intervention spend over 30d / prediction event volume over 30d.",
        },
        "baseline_monthly_ebit": round(_cv(baseline_monthly_ebit_native), 2),
        "baseline_monthly_ebit_fmt": _fmt_money(_cv(baseline_monthly_ebit_native), currency),
        "mom_ebit_pct": round(mom_ebit_pct, 1),
        "yoy_ebit_pct": round(yoy_ebit_pct, 1),
        "source_table": source_table,
        "data_mode": data_mode,
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
            "plant_code": (
                str((work_orders[0] or {}).get("plant_code") or "").strip() or profile.get("plant_code")
            ),
            "fiscal_period": profile.get("fiscal_period"),
            "cost_centers": profile.get("cost_centers", []),
            "work_centers": profile.get("work_centers", []),
            "planner_group": profile.get("planner_group"),
            "reference_account": accounts.get("primary", ""),
        },
        "work_order_source": work_order_source,
        "erp_ingestion": {
            "pipeline_label": "SAP BDC-shaped landing → bronze → Lakebase ODS",
            "bronze_work_orders": f"{catalog}.bronze.erp_bdc_work_orders",
            "bronze_cost_centers": f"{catalog}.bronze.erp_bdc_cost_centers",
            "ods_table": f"{catalog}.lakebase.work_orders",
            "active_source": work_order_source,
        },
        "value_bridge": value_bridge,
        "ebit_trend": ebit_trend,
        "work_orders": work_orders,
        "executive_summary": {
            "confidence_pct": round(confidence_pct, 1),
            "annualized_ebit_saved": round(annualized_ebit_saved, 2),
            "annualized_ebit_saved_fmt": _fmt_money(annualized_ebit_saved, currency),
            "annual_ebit_target": round(annual_target, 2),
            "annual_ebit_target_fmt": _fmt_money(annual_target, currency),
            "run_rate_to_target_pct": round(run_rate_to_target_pct, 1),
        },
        "forward_outlook": {
            "horizon_30_days": {
                "protected_with_actions": round(ebit_saved, 2),
                "protected_with_actions_fmt": _fmt_money(ebit_saved, currency),
                "protected_without_actions": round(protected_no_action_30, 2),
                "protected_without_actions_fmt": _fmt_money(protected_no_action_30, currency),
                "at_risk_if_deferred": round(decision_value_30, 2),
                "at_risk_if_deferred_fmt": _fmt_money(decision_value_30, currency),
            },
            "horizon_90_days": {
                "protected_with_actions": round(ebit_saved * 2.6, 2),
                "protected_with_actions_fmt": _fmt_money(ebit_saved * 2.6, currency),
                "protected_without_actions": round(protected_no_action_90, 2),
                "protected_without_actions_fmt": _fmt_money(protected_no_action_90, currency),
                "at_risk_if_deferred": round(decision_value_90, 2),
                "at_risk_if_deferred_fmt": _fmt_money(decision_value_90, currency),
            },
        },
        "decision_cockpit": decision_cards,
        "portfolio_insights": {
            "concentration_top5_pct": round(concentration_top5_pct, 1),
            "top_risk_assets": portfolio_rows[:8],
        },
        "adoption_insights": adoption_insights,
    }


def _asset_snapshot(
    industry: str,
    asset_def: dict[str, Any],
    pred: dict[str, Any] | None = None,
    display_currency: str | None = None,
) -> dict[str, Any]:
    aid = asset_def["id"]
    rng = _asset_rng(industry, aid)
    sev = float(asset_def.get("fault_severity", 0.0))
    st = _sim_state(industry)
    fault_cfg = (st.get("faults", {}) or {}).get(aid, {"enabled": False, "severity": 0, "mode": "degradation"})
    fault_enabled = bool(fault_cfg.get("enabled"))
    fault_sev = max(0.0, min(100.0, float(fault_cfg.get("severity", 0) or 0))) / 100.0
    if pred and pred.get("anomaly_score") is not None:
        anomaly = round(float(pred.get("anomaly_score")), 2)
    elif sev > 0:
        anomaly = round(max(0.02, min(0.99, sev + rng.uniform(-0.08, 0.06))), 2)
    else:
        anomaly = round(max(0.02, min(0.45, rng.uniform(0.08, 0.28))), 2)
    # Pin simulator-injected faults so they remain visible until manually cleared.
    if fault_enabled:
        injected_floor = round(max(0.5, min(0.99, 0.5 + (fault_sev * 0.49))), 2)
        anomaly = max(anomaly, injected_floor)
    if pred and pred.get("rul_hours") is not None:
        rul = round(float(pred.get("rul_hours")), 1)
    else:
        rul = round(max(4.0, (220.0 * (1 - anomaly)) + rng.uniform(-16, 12)), 1)
    if fault_enabled:
        pinned_rul_cap = round(max(1.0, 24.0 * (1.0 - fault_sev) + 2.0), 1)
        rul = min(rul, pinned_rul_cap)
    health = max(5, int((1 - anomaly) * 100))
    status = "critical" if anomaly >= 0.8 else "warning" if anomaly >= 0.5 else "healthy"
    inject_fault = fault_cfg.get("mode") or asset_def.get("inject_fault")
    fm = _industry_cfg(industry).get("failure_modes", {}).get(inject_fault or "", {})
    base_cost = int(fm.get("cost_per_event", rng.uniform(8_000, 95_000)))
    exposure = int(base_cost * max(0.3, anomaly))
    native_currency = _industry_cfg(industry).get("agent", {}).get("terminology", {}).get("cost_currency", "USD")
    currency = _effective_demo_currency(display_currency, native_currency)
    exposure_conv = _fx_convert(float(exposure), str(native_currency), currency)
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
        "cost_exposure": _fmt_money(exposure_conv, currency),
        "cost_exposure_value": round(exposure_conv, 2),
        "model_version_anomaly": (pred or {}).get("model_version_anomaly"),
        "model_version_rul": (pred or {}).get("model_version_rul"),
        "prediction_timestamp": (pred or {}).get("prediction_timestamp"),
        "top_contributing_sensor": (pred or {}).get("top_contributing_sensor"),
        "top_contributing_score": (pred or {}).get("top_contributing_score"),
        "fault_pinned": fault_enabled,
    }


def _overview_assets(
    industry: str, display_currency: str | None = None, trigger_live_scoring: bool = True
) -> list[dict[str, Any]]:
    predictions = _predictions_map(industry, trigger_live_scoring=trigger_live_scoring)
    rows = [
        _asset_snapshot(industry, a, predictions.get(a["id"]), display_currency=display_currency)
        for a in _asset_defs(industry)
    ]
    # Hard guard: keep overview strictly industry-scoped by site membership.
    # This prevents accidental cross-industry asset leakage when metadata rows are dirty.
    allowed_sites: set[str] = set()
    for site in _geo_sites_for_industry(industry):
        for key in (site.get("site_id"), site.get("site_key")):
            norm = _asset_token_norm(str(key or ""))
            if norm:
                allowed_sites.add(norm)
    if allowed_sites:
        rows = [r for r in rows if _asset_token_norm(str(r.get("site") or "")) in allowed_sites]
    return rows


def _overview(industry: str, display_currency: str | None = None) -> dict[str, Any]:
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    alert_source_table = f"{catalog}.gold.pdm_predictions"
    rows = _overview_assets(industry, display_currency=display_currency)
    actioned_assets = _recommendation_actioned_assets(industry)
    if not rows:
        return {
            "assets": [],
            "actioned_assets": [],
            "kpis": {"fleet_health_score": 0, "critical_assets": 0, "asset_count": 0},
            "executive": _executive_value(industry, [], display_currency=display_currency),
        }
    avg_health = round(sum(a["health_score_pct"] for a in rows) / len(rows), 1)
    critical = [a for a in rows if a["status"] == "critical"]
    warning = [a for a in rows if a["status"] == "warning"]
    critical_tip = (
        f"Source: {alert_source_table}. Logic: latest anomaly_score >= 0.80 per equipment "
        f"(fault mode from industry config)."
    )
    warning_tip = (
        f"Source: {alert_source_table}. Logic: latest anomaly_score >= 0.50 and < 0.80 per equipment "
        f"(fault mode from industry config)."
    )
    return {
        "assets": rows,
        "actioned_assets": sorted(actioned_assets),
        "kpis": {
            "fleet_health_score": avg_health,
            "critical_assets": len(critical),
            "asset_count": len(rows),
            "avoided_cost": round(sum(_to_float(a.get("cost_exposure_value"), 0.0) for a in warning + critical), 2),
        },
        "alerts": [
            {
                "equipment_id": a["id"],
                "severity": "critical",
                "text": f"{a['id']} requires immediate intervention ({a['fault_mode']})",
                "time": "now",
                "tooltip": critical_tip,
                "source_table": alert_source_table,
            }
            for a in critical[:3]
        ]
        + [
            {
                "equipment_id": a["id"],
                "severity": "warning",
                "text": f"{a['id']} should be scheduled this week ({a['fault_mode']})",
                "time": "recent",
                "tooltip": warning_tip,
                "source_table": alert_source_table,
            }
            for a in warning[:3]
        ],
        "messages": [
            {
                "role": "agent",
                "label": "Maintenance Supervisor AI",
                "text": (
                    "高リスク設備の優先順位付け、部品の準備状況、推奨アクションを案内できます。"
                    if _effective_demo_currency(display_currency, "USD") == "JPY"
                    else "고위험 설비 우선순위, 부품 준비 상태, 권장 조치를 안내할 수 있습니다."
                    if _effective_demo_currency(display_currency, "USD") == "KRW"
                    else "I can triage top-risk equipment, parts readiness, and recommended actions."
                ),
            }
        ],
        "executive": _executive_value(industry, rows, display_currency=display_currency),
    }


def _hierarchy(industry: str) -> dict[str, Any]:
    # Keep hierarchy fast when switching tabs: avoid live scoring trigger checks
    # here because overview/asset endpoints already handle those.
    assets = _overview_assets(industry, display_currency=None, trigger_live_scoring=False)
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


def _asset_detail(industry: str, asset_id: str, display_currency: str | None = None) -> dict[str, Any]:
    asset_def = next((a for a in _asset_defs(industry) if a["id"] == asset_id), None)
    if asset_def is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    predictions = _predictions_map(industry)
    snapshot = _asset_snapshot(industry, asset_def, predictions.get(asset_id), display_currency=display_currency)
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
        assets = cfg.get("simulator", {}).get("assets", []) or _default_industry_cfg(industry).get("simulator", {}).get("assets", [])
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


def _count_recent_rows(rows: list[dict[str, Any]], ts_field: str = "timestamp") -> tuple[int, int, int, Any]:
    now = datetime.now(timezone.utc)
    rows_5m = 0
    rows_30m = 0
    rows_prev_5m = 0
    latest_ts = None
    for r in rows:
        ts = _parse_dt(r.get(ts_field))
        if ts is None:
            continue
        if latest_ts is None or ts > latest_ts:
            latest_ts = ts
        age_s = (now - ts).total_seconds()
        if age_s <= 300:
            rows_5m += 1
            rows_30m += 1
        elif age_s <= 600:
            rows_prev_5m += 1
            rows_30m += 1
        elif age_s <= 1800:
            rows_30m += 1
    return rows_30m, rows_5m, rows_prev_5m, latest_ts


def _sim_recent_bronze_rows(industry: str, limit: int) -> list[dict[str, Any]]:
    st = _sim_state(industry)
    max_rows = max(5, min(120, int(limit)))
    rows = list(st.get("recent_rows") or [])[:max_rows]
    # Ensure rows are normalized for the simulator flow table.
    normalized: list[dict[str, Any]] = []
    for r in rows:
        normalized.append(
            {
                "timestamp": r.get("timestamp"),
                "equipment_id": r.get("equipment_id"),
                "tag_name": r.get("tag_name"),
                "value": r.get("value"),
                "unit": r.get("unit"),
                "quality": r.get("quality"),
                "source_protocol": r.get("source_protocol"),
            }
        )
    return normalized


def _sim_recent_silver_rows(industry: str, limit: int) -> list[dict[str, Any]]:
    source = _sim_recent_bronze_rows(industry, max(20, int(limit) * 3))
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for r in source:
        eid = str(r.get("equipment_id") or "")
        tag = str(r.get("tag_name") or "")
        if not eid or not tag:
            continue
        key = (eid, tag)
        b = buckets.setdefault(
            key,
            {
                "timestamp": r.get("timestamp"),
                "equipment_id": eid,
                "tag_name": tag,
                "sum_value": 0.0,
                "count": 0,
                "quality_counts": {},
            },
        )
        ts = _parse_dt(r.get("timestamp"))
        bts = _parse_dt(b.get("timestamp"))
        if ts and (bts is None or ts > bts):
            b["timestamp"] = r.get("timestamp")
        try:
            b["sum_value"] += float(r.get("value") or 0.0)
            b["count"] += 1
        except Exception:
            pass
        q = str(r.get("quality") or "unknown")
        qc = b["quality_counts"]
        qc[q] = int(qc.get(q) or 0) + 1

    rows: list[dict[str, Any]] = []
    for b in buckets.values():
        qc = b.get("quality_counts", {})
        quality = max(qc.items(), key=lambda kv: kv[1])[0] if qc else "unknown"
        rows.append(
            {
                "timestamp": b.get("timestamp"),
                "equipment_id": b.get("equipment_id"),
                "tag_name": b.get("tag_name"),
                "value": round((b.get("sum_value") or 0.0) / max(1, int(b.get("count") or 0)), 3),
                "quality": quality,
                "source_protocol": "SILVER_PENDING_DLT",
            }
        )

    rows.sort(key=lambda x: str(x.get("timestamp") or ""), reverse=True)
    return rows[: max(5, min(120, int(limit)))]


def _apply_live_fallback(
    stage: dict[str, Any],
    fallback_rows: list[dict[str, Any]],
    *,
    ts_field: str = "timestamp",
    force_if_stale_s: int = 120,
) -> dict[str, Any]:
    if not fallback_rows:
        return stage
    table_rows = stage.get("rows") or []
    latest_ts = _parse_dt(stage.get("latest_ts"))
    stale = latest_ts is None or (datetime.now(timezone.utc) - latest_ts).total_seconds() > force_if_stale_s
    if table_rows and not stale:
        return stage

    rows_30m, rows_5m, rows_prev_5m, fb_latest = _count_recent_rows(fallback_rows, ts_field=ts_field)
    if rows_prev_5m <= 0:
        rate_change_pct = 100.0 if rows_5m > 0 else 0.0
    else:
        rate_change_pct = ((rows_5m - rows_prev_5m) / rows_prev_5m) * 100.0

    stage.update(
        {
            "rows": fallback_rows,
            "rows_30m": rows_30m,
            "rows_5m": rows_5m,
            "rows_prev_5m": rows_prev_5m,
            "rate_change_pct": round(rate_change_pct, 1),
            "latest_ts": fb_latest.isoformat() if isinstance(fb_latest, datetime) else stage.get("latest_ts"),
            "live_fallback": True,
        }
    )
    return stage


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
    catalog = table_fqn.split(".", 1)[0]
    latest_stmt = f"""
    WITH latest_protocol AS (
      SELECT equipment_id, source_protocol,
             ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY timestamp DESC) AS rn
      FROM {catalog}.bronze.sensor_readings
    )
    SELECT p.prediction_timestamp AS timestamp,
           p.equipment_id,
           'anomaly_score' AS tag_name,
           p.anomaly_score AS value,
           p.anomaly_label AS quality,
           COALESCE(lp.source_protocol, 'unknown') AS source_protocol
    FROM {table_fqn} p
    LEFT JOIN latest_protocol lp
      ON p.equipment_id = lp.equipment_id AND lp.rn = 1
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
        target_table = connector.get("target_table", _DEFAULT_ZEROBUS_TARGET_TABLE)
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
def ui_overview(industry: str = "mining", currency: str = "") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    ccy = _normalize_currency(currency or "", "")
    cache_key = f"overview:{industry}:{ccy}"
    cached = _ui_cache_get(cache_key)
    if cached is not None:
        return cached
    payload = _overview(industry, display_currency=currency)
    _ui_cache_set(cache_key, payload)
    return payload


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
    _ui_cache_invalidate(industry)
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
    cache_key = f"hierarchy:{industry}"
    cached = _ui_cache_get(cache_key)
    if cached is not None:
        return cached
    payload = _hierarchy(industry)
    _ui_cache_set(cache_key, payload)
    return payload


@app.get("/api/ui/asset/{asset_id}")
def ui_asset(asset_id: str, industry: str = "mining", currency: str = "") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    return _ui_asset_payload_cached(industry, asset_id, currency)


@app.get("/api/ui/model/{asset_id}")
def ui_model(asset_id: str, industry: str = "mining", currency: str = "") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    ccy = _normalize_currency(currency or "", "")
    cache_key = f"ui_model:{industry}:{ccy}:{asset_id}"
    cached = _ui_cache_get(cache_key)
    if cached is not None:
        return cached
    detail = _ui_asset_payload_cached(industry, asset_id, currency)
    data_source = _asset_data_source(industry, asset_id)
    quality = _rul_model_metrics(industry, asset_id, detail.get("model_version_rul"))
    model_driven = bool(detail.get("prediction_timestamp"))
    if not model_driven:
        payload = {
            "asset_id": asset_id,
            "health_score_pct": None,
            "rul_hours": None,
            "model_meta": {
                "trained": None,
                "r2": quality.get("r2"),
                "rmse": quality.get("rmse"),
                "protocol": _industry_cfg(industry).get("simulator", {}).get("protocol", "OPC-UA"),
                "data_source": data_source,
                "model_version_anomaly": None,
                "model_version_rul": None,
                "is_model_driven": False,
                "status": "unavailable",
                "status_message": "No scored prediction found yet. Train and run scoring to populate model output.",
            },
            "rul_curve": {"labels": [], "values": []},
            "feature_importance": [],
            "anomaly_decomposition": [],
        }
        _ui_cache_set(cache_key, payload)
        return payload
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
    payload = {
        "asset_id": asset_id,
        "health_score_pct": detail["health_score_pct"],
        "rul_hours": detail["rul_hours"],
        "model_meta": {
            "trained": str(detail.get("prediction_timestamp") or "n/a"),
            "r2": quality.get("r2"),
            "rmse": quality.get("rmse"),
            "protocol": _industry_cfg(industry).get("simulator", {}).get("protocol", "OPC-UA"),
            "data_source": data_source,
            "model_version_anomaly": detail.get("model_version_anomaly"),
            "model_version_rul": detail.get("model_version_rul"),
            "is_model_driven": True,
            "status": "ready",
            "status_message": "Model output is driven by the latest scored prediction.",
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
    _ui_cache_set(cache_key, payload)
    return payload


@app.get("/api/ui/advanced_pdm")
def ui_advanced_pdm(asset_id: str, industry: str = "mining", currency: str = "") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    return _advanced_pdm_payload(industry, asset_id, display_currency=currency)


@app.post("/api/ui/manuals/parse")
def ui_manuals_parse(payload: dict[str, Any]) -> dict:
    industry = str(payload.get("industry", "automotive") or "automotive").lower()
    if industry not in INDUSTRIES:
        industry = "automotive"
    raw_text = str(payload.get("text", "") or "").strip()
    manual_name = str(payload.get("filename", "") or "").strip()
    b64 = str(payload.get("content_base64", "") or "").strip()
    if not raw_text and b64:
        try:
            raw_text = _manual_text_from_bytes(manual_name or "manual.pdf", base64.b64decode(b64))
        except Exception:
            raw_text = ""
    if not raw_text and manual_name:
        p = _industry_manual_dir(industry) / manual_name
        if p.exists():
            raw_text = _manual_text_from_file(p).strip()
    if not raw_text:
        return {"ok": False, "message": "No manual content provided.", "fields": {}, "snippets": []}

    fields: dict[str, str] = {}
    patterns = {
        "part_number": r"(?:part(?:\s*number)?|pn)\s*[:#]\s*([A-Z0-9\-_]+)",
        "torque_spec": r"(?:torque|締付けトルク)\s*[:：]\s*([0-9\.\-]+\s*(?:N·m|Nm|kgf·m))",
        "inspection_interval": r"(?:inspection interval|点検周期)\s*[:：]\s*([^\n\r;,.]{3,60})",
        "temperature_limit": r"(?:temperature limit|温度上限)\s*[:：]\s*([0-9\.\-]+\s*(?:°C|C))",
        "vibration_limit": r"(?:vibration(?:\s*limit)?|振動上限)\s*[:：]\s*([0-9\.\-]+\s*(?:mm/s|g))",
    }
    for k, pat in patterns.items():
        m = re.search(pat, raw_text, flags=re.IGNORECASE)
        if m:
            fields[k] = m.group(1).strip()

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    snippets = lines[:8]
    return {
        "ok": True,
        "industry": industry,
        "filename": manual_name or "manual_text_input",
        "fields": fields,
        "snippets": snippets,
        "summary": f"Extracted {len(fields)} structured fields from manual text.",
    }


@app.post("/api/ui/manuals/upload")
def ui_manuals_upload(payload: dict[str, Any]) -> dict:
    industry = str(payload.get("industry", "automotive") or "automotive").lower()
    if industry not in INDUSTRIES:
        industry = "automotive"
    filename = str(payload.get("filename", "") or "").strip()
    b64 = str(payload.get("content_base64", "") or "").strip()
    if not filename or not b64:
        return {"ok": False, "message": "filename and content_base64 are required."}
    try:
        raw = base64.b64decode(b64)
    except Exception:
        return {"ok": False, "message": "Invalid base64 payload."}
    text = _manual_text_from_bytes(filename, raw).strip()
    if not text:
        return {"ok": False, "message": "Unsupported file or no extractable text."}

    manual_dir = _industry_manual_dir(industry)
    manual_dir.mkdir(parents=True, exist_ok=True)
    file_path = manual_dir / filename
    try:
        file_path.write_bytes(raw)
    except Exception as e:
        return {"ok": False, "message": f"Failed to save file: {e}"}

    chunks = _persist_manual_chunks(industry, filename, text)
    _MANUAL_KB_CACHE.pop(industry, None)
    refs = _manual_references(industry, text[:1200], limit=3)
    return {
        "ok": True,
        "industry": industry,
        "filename": filename,
        "chunk_count": chunks,
        "references": refs,
        "summary": f"Uploaded {filename} and indexed {chunks} chunks in UC Delta manual index.",
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
        "last_error": st.get("last_error", ""),
        "faults": st["faults"],
        "rows": st["recent_rows"],
        "assets": _asset_defs(industry),
        "asset_sensors": asset_sensors,
        "catalog": _industry_cfg(industry).get("catalog", f"pdm_{industry}"),
    }


@app.get("/api/ui/genie/rooms")
def ui_genie_rooms(industry: str = "mining", room_type: str = "ops") -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    room_map = _load_genie_room_map(str(room_type or "ops"))
    workspace_url = (
        str(
            os.getenv("DATABRICKS_HOST")
            or os.getenv("OT_PDM_DEFAULT_WORKSPACE_URL")
            or _DEFAULT_ZEROBUS_WORKSPACE_URL
            or ""
        )
        .strip()
        .rstrip("/")
    )
    if workspace_url and not workspace_url.startswith("http"):
        workspace_url = f"https://{workspace_url}"

    rooms: dict[str, dict[str, str]] = {}
    missing: list[str] = []
    for ind in INDUSTRIES:
        space_id = str(room_map.get(ind, "") or "").strip()
        if not space_id:
            missing.append(ind)
        rooms[ind] = {
            "space_id": space_id,
            "url": f"{workspace_url}/genie/rooms/{space_id}" if workspace_url and space_id else "",
        }

    return {
        "industry": industry,
        "workspace_url": workspace_url,
        "configured_count": len(INDUSTRIES) - len(missing),
        "total_count": len(INDUSTRIES),
        "missing": missing,
        "rooms": rooms,
    }


@app.get("/api/ui/simulator/flow")
def ui_simulator_flow(industry: str = "mining", limit: int = 30) -> dict:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    bronze_fqn = f"{catalog}.bronze.sensor_readings"
    features_fqn = f"{catalog}.bronze.sensor_features"
    predictions_fqn = f"{catalog}.gold.pdm_predictions"
    bronze = _sim_flow_stage(bronze_fqn, "bronze_curated", limit, "bronze")
    bronze = _apply_live_fallback(bronze, _sim_recent_bronze_rows(industry, limit))

    silver = _sim_silver_stage(features_fqn, limit)
    silver = _apply_live_fallback(silver, _sim_recent_silver_rows(industry, limit))

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


@app.post("/api/ui/scoring/run")
def ui_scoring_run(payload: dict) -> dict:
    industry = str(payload.get("industry", "mining") or "mining")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    if WorkspaceClient is None:
        raise HTTPException(status_code=500, detail="WorkspaceClient unavailable in this runtime")

    try:
        client = WorkspaceClient()
        job_id = _resolve_scoring_job_id(client, industry)
        if not job_id:
            raise HTTPException(status_code=404, detail=f"Scoring job not found for industry={industry}")
        # Use the configured per-industry job directly. This works for
        # serverless task environments and avoids manual ad-hoc task submission.
        resp = client.api_client.do("POST", "/api/2.1/jobs/run-now", body={"job_id": int(job_id)})
        run_id = (resp or {}).get("run_id") if isinstance(resp, dict) else None
        if not run_id:
            raise HTTPException(
                status_code=500,
                detail=f"Scoring run submission returned no run_id for industry={industry}. Response={resp}",
            )
        state = _LIVE_SCORING_STATE.setdefault(industry, {})
        state["last_trigger_s"] = time.time()
        if isinstance(resp, dict):
            state["last_run_id"] = resp.get("run_id")
            state["last_number_in_job"] = resp.get("number_in_job")
        catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
        keys = [k for k in _SQL_CACHE.keys() if isinstance(k, str) and k.startswith(f"{catalog}:predictions")]
        for k in keys:
            _SQL_CACHE.pop(k, None)
        return {
            "ok": True,
            "industry": industry,
            "job_id": int(job_id),
            "run_id": run_id,
            "number_in_job": (resp or {}).get("number_in_job") if isinstance(resp, dict) else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unable to trigger scoring run: {e}")


@app.get("/api/ui/scoring/status")
def ui_scoring_status(run_id: int) -> dict:
    if WorkspaceClient is None:
        raise HTTPException(status_code=500, detail="WorkspaceClient unavailable in this runtime")
    try:
        client = WorkspaceClient()
        payload = client.api_client.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": int(run_id)})
        state = (payload or {}).get("state", {}) if isinstance(payload, dict) else {}
        tasks = (payload or {}).get("tasks", []) if isinstance(payload, dict) else []
        task_state = (tasks[0].get("state", {}) if tasks and isinstance(tasks[0], dict) else {})
        return {
            "ok": True,
            "run_id": int(run_id),
            "run_name": (payload or {}).get("run_name"),
            "run_page_url": (payload or {}).get("run_page_url"),
            "life_cycle_state": state.get("life_cycle_state"),
            "result_state": state.get("result_state"),
            "state_message": state.get("state_message"),
            "task_life_cycle_state": task_state.get("life_cycle_state"),
            "task_result_state": task_state.get("result_state"),
            "task_state_message": task_state.get("state_message"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unable to fetch scoring run status: {e}")


@app.post("/api/ui/simulator/inject_and_score")
def ui_simulator_inject_and_score(payload: dict) -> dict:
    industry = str(payload.get("industry", "mining") or "mining")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    ticks = max(1, min(120, int(payload.get("ticks", 12) or 12)))
    wait_seconds = max(0.0, min(30.0, float(payload.get("wait_seconds", 0.0) or 0.0)))
    non_blocking = bool(payload.get("non_blocking", True))

    st = _sim_state(industry)
    enabled_assets = [aid for aid, cfg in (st.get("faults") or {}).items() if bool((cfg or {}).get("enabled"))]
    if not enabled_assets:
        raise HTTPException(status_code=400, detail="No enabled fault assets. Enable a fault before injecting.")

    was_running = bool(st.get("running"))
    keep_running = bool(payload.get("keep_running", True))
    emitted = 0
    try:
        st["running"] = True
        for _ in range(ticks):
            before = int(st.get("reading_count", 0))
            ui_simulator_tick({"industry": industry})
            after = int(st.get("reading_count", 0))
            emitted += max(0, after - before)
    finally:
        st["running"] = True if keep_running else was_running

    dlt_resp: dict[str, Any] = {"ok": False, "error": "DLT update not triggered."}
    score_resp: dict[str, Any] = {"ok": False, "error": "Scoring run not triggered."}
    try:
        client = WorkspaceClient()
        dlt_resp = _trigger_dlt_update(client, industry)
    except Exception as e:
        dlt_resp = {"ok": False, "error": f"Unable to trigger DLT update: {e}"}

    if wait_seconds > 0 and not non_blocking:
        time.sleep(wait_seconds)
    try:
        score_resp = ui_scoring_run({"industry": industry})
    except HTTPException as e:
        score_resp = {"ok": False, "error": str(getattr(e, "detail", "") or "Unable to trigger scoring run.")}
    except Exception as e:
        score_resp = {"ok": False, "error": f"Unable to trigger scoring run: {e}"}

    # Non-blocking mode never fails the request if rows were emitted; it returns
    # immediately with best-effort trigger details for downstream polling.
    if non_blocking and emitted > 0:
        return {
            "ok": True,
            "accepted": True,
            "industry": industry,
            "ticks": ticks,
            "keep_running": keep_running,
            "rows_emitted": emitted,
            "enabled_fault_assets": enabled_assets,
            "dlt": dlt_resp,
            "score": score_resp,
            "run_id": score_resp.get("run_id") if isinstance(score_resp, dict) else None,
        }
    return {
        "ok": True,
        "industry": industry,
        "ticks": ticks,
        "keep_running": keep_running,
        "rows_emitted": emitted,
        "enabled_fault_assets": enabled_assets,
        "dlt": dlt_resp,
        "score": score_resp,
    }


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
    assets = _asset_defs(industry)
    cfg_assets = _industry_cfg(industry).get("simulator", {}).get("assets", [])
    cfg_by_id = {_asset_token_norm(str(a.get("id") or "")): a for a in cfg_assets if a.get("id")}
    # Keep default sensor fallback available even when user config is partial.
    sensor_map = (
        _industry_cfg(industry).get("sensors", {})
        or _default_industry_cfg(industry).get("sensors", {})
        or {}
    )
    all_sensor_sets = list(sensor_map.values())
    default_sensor_set = next((s for s in all_sensor_sets if isinstance(s, list) and s), [])

    for asset in assets:
        aid = asset["id"]
        fault_cfg = st.get("faults", {}).get(aid, {"enabled": False, "severity": 0, "mode": "degradation"})
        asset_type = str(asset.get("type", "") or "")
        sensors = _sensor_defs(industry, asset_type)[:3]
        if not sensors:
            cfg_asset = cfg_by_id.get(_asset_token_norm(aid), {})
            cfg_asset_type = str(cfg_asset.get("type", "") or "")
            if cfg_asset_type:
                sensors = _sensor_defs(industry, cfg_asset_type)[:3]
        if not sensors:
            sensors = default_sensor_set[:3]
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
        st["last_error"] = str(e)
        print(f"[simulator] tick persist failed industry={industry} error={e}")
        raise HTTPException(status_code=500, detail=f"Simulator ingest failed for {industry}: {e}")
    st["last_error"] = ""
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
    _ui_cache_invalidate(industry)
    return {"asset_id": asset_id, "fault": current, "faults": st["faults"]}


@app.post("/api/ui/simulator/force_critical")
def ui_simulator_force_critical(payload: dict) -> dict:
    industry = str(payload.get("industry", "mining") or "mining")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    asset_id = str(payload.get("asset_id", "") or "").strip()
    if not asset_id:
        raise HTTPException(status_code=400, detail="Missing asset_id")
    if WorkspaceClient is None or sql_service is None:
        raise HTTPException(status_code=500, detail="Databricks WorkspaceClient unavailable in app runtime.")

    assets = _asset_defs(industry)
    if asset_id not in {str(a.get("id", "")) for a in assets}:
        raise HTTPException(status_code=400, detail=f"Unknown asset_id '{asset_id}' for industry '{industry}'.")

    anomaly_score = float(payload.get("anomaly_score", 0.95) or 0.95)
    anomaly_score = max(0.80, min(0.999, anomaly_score))
    rul_hours = float(payload.get("rul_hours", 6.0) or 6.0)
    rul_hours = max(1.0, min(240.0, rul_hours))

    cfg = _industry_cfg(industry)
    catalog = cfg.get("catalog", f"pdm_{industry}")
    table_fqn = f"{catalog}.gold.pdm_predictions"
    prediction_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    failure_ts = (datetime.now(timezone.utc) + timedelta(hours=rul_hours)).strftime("%Y-%m-%d %H:%M:%S")

    client = WorkspaceClient()

    def _exec_checked(statement: str) -> None:
        resp = client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=_WAREHOUSE_ID,
            wait_timeout="50s",
            disposition=sql_service.Disposition.INLINE,
        )
        status = getattr(resp, "status", None)
        if status is None or status.state != sql_service.StatementState.SUCCEEDED:
            state = getattr(status, "state", None) if status is not None else None
            err = getattr(status, "error", None) if status is not None else None
            msg = getattr(err, "message", None) if err is not None else None
            raise RuntimeError(f"SQL statement failed state={state} message={msg or 'unknown error'}")

    try:
        _exec_checked(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        _exec_checked(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
    except Exception:
        # Continue when caller has write access but no create grants.
        pass

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_fqn} (
      equipment_id              STRING NOT NULL,
      prediction_timestamp      TIMESTAMP NOT NULL,
      anomaly_score             DOUBLE NOT NULL,
      anomaly_label             STRING NOT NULL,
      rul_hours                 DOUBLE,
      predicted_failure_date    TIMESTAMP,
      top_contributing_sensor   STRING,
      top_contributing_score    DOUBLE,
      model_version_anomaly     STRING,
      model_version_rul         STRING,
      _scored_at                TIMESTAMP DEFAULT current_timestamp()
    ) USING DELTA
    """
    try:
        _exec_checked(ddl)
    except Exception:
        # If table already exists and CREATE is blocked, insertion may still succeed.
        pass

    insert_stmt = f"""
    INSERT INTO {table_fqn} (
      equipment_id,
      prediction_timestamp,
      anomaly_score,
      anomaly_label,
      rul_hours,
      predicted_failure_date,
      top_contributing_sensor,
      top_contributing_score,
      model_version_anomaly,
      model_version_rul
    ) VALUES (
      '{_sql_escape(asset_id)}',
      TIMESTAMP '{_sql_escape(prediction_ts)}',
      {anomaly_score},
      'anomaly',
      {rul_hours},
      TIMESTAMP '{_sql_escape(failure_ts)}',
      'forced_demo',
      {round(anomaly_score, 3)},
      'forced_demo',
      'forced_demo'
    )
    """
    try:
        _exec_checked(insert_stmt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unable to force critical prediction: {e}")

    st = _sim_state(industry)
    current = st["faults"].get(asset_id, {"enabled": True, "severity": 100, "mode": "degradation"})
    current["enabled"] = True
    current["severity"] = 100
    st["faults"][asset_id] = current
    _ui_cache_invalidate(industry)

    return {
        "ok": True,
        "industry": industry,
        "asset_id": asset_id,
        "table": table_fqn,
        "anomaly_score": round(anomaly_score, 3),
        "rul_hours": round(rul_hours, 1),
        "prediction_timestamp": prediction_ts,
        "faults": st["faults"],
    }


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
    # If simulator is running, emit live ticks from stream page polling so users
    # can see motion without having to stay on Simulator tab.
    st = _sim_state(industry)
    if st.get("running"):
        try:
            ticked = ui_simulator_tick({"industry": industry})
            recent = list(ticked.get("rows", []) or [])[: max(1, min(limit, 200))]
            if recent:
                return {"rows": recent}
        except Exception:
            pass
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
    user_text_lc = user_text.lower()
    force_english = any(
        p in user_text_lc
        for p in [
            "translate to english",
            "translate in english",
            "in english",
            "respond in english",
            "answer in english",
        ]
    ) or ("英語" in user_text) or ("영어" in user_text)
    currency = _normalize_currency(str(payload.get("currency", "") or "").strip().upper(), "")
    respond_japanese = (currency == "JPY") and not force_english
    respond_korean = (currency == "KRW") and not force_english
    if not user_text:
        msg = (
            "質問を入力してください。"
            if respond_japanese
            else "질문을 입력해 주세요."
            if respond_korean
            else "Please enter a question."
        )
        return {"choices": [{"message": {"content": msg}}]}

    industry = str(payload.get("industry", "mining") or "mining").lower()
    if industry not in INDUSTRIES:
        industry = "mining"
    conversation_id = str(payload.get("conversation_id", "") or "").strip()
    resolved_asset, raw_asset = _resolve_asset_alias(industry, user_text)
    effective_user_text = user_text
    if resolved_asset:
        alias = raw_asset or resolved_asset
        confirm_q = (
            f"「'{alias}' を '{resolved_asset}' と解釈しました。この認識でよろしいですか？」"
            if respond_japanese
            else f"'{alias}'를 '{resolved_asset}'로 해석했습니다. 이 해석이 맞을까요?"
            if respond_korean
            else f"\"I interpreted '{alias}' as '{resolved_asset}' — is that what you meant?\""
        )
        effective_user_text = (
            f"{user_text}\n\n"
            f"Resolver note: interpret asset reference '{alias}' as canonical equipment ID '{resolved_asset}'. "
            f"Answer for '{resolved_asset}', and at the end ask a short confirmation question: "
            f"{confirm_q}"
        )
    if respond_japanese:
        effective_user_text = (
            f"{effective_user_text}\n\n"
            "Language note: respond entirely in Japanese. Keep numbers, equipment IDs, and units unchanged."
        )
    elif respond_korean:
        effective_user_text = (
            f"{effective_user_text}\n\n"
            "Language note: respond entirely in Korean. Keep numbers, equipment IDs, and units unchanged."
        )
    elif force_english:
        effective_user_text = (
            f"{effective_user_text}\n\n"
            "Language note: respond entirely in English."
        )
    manual_refs = _manual_references(
        industry,
        f"{effective_user_text} {resolved_asset or ''}".strip(),
        limit=3,
    )
    if manual_refs:
        ref_lines = "\n".join(
            [
                f"- [{r.get('source')}] {r.get('excerpt')}"
                for r in manual_refs
            ]
        )
        effective_user_text = (
            f"{effective_user_text}\n\n"
            "Reference manuals (ground recommendations in these excerpts and cite filename in brackets):\n"
            f"{ref_lines}"
        )

    room_map = _load_genie_room_map(str(payload.get("room_type", "ops") or "ops"))
    space_id = room_map.get(industry) or room_map.get("default", "")
    if not space_id or WorkspaceClient is None:
        missing_reason = (
            "Genie room is not configured for this industry/room type."
            if not space_id
            else "Databricks WorkspaceClient is unavailable in this runtime."
        )
        return {
            "conversation_id": conversation_id,
            "genie_status": "FAILED",
            "genie_failed": True,
            "choices": [{"message": {"content": f"Genie request failed: {missing_reason}"}}],
            "references": manual_refs,
        }

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
            # Return a non-throwing response so product-specific callers can
            # gracefully fall back to deterministic logic.
            failure_text = _genie_extract_text(final_message)
            if not failure_text:
                failure_text = f"Genie message status={status}"
            return {
                "conversation_id": conversation_id,
                "genie_status": status,
                "genie_failed": True,
                "choices": [{"message": {"content": f"Genie request failed: {failure_text}"}}],
            }

        text = _genie_extract_text(final_message) or "Genie completed your request."
        return {
            "conversation_id": conversation_id,
            "choices": [{"message": {"content": text}}],
            "references": manual_refs,
        }
    except Exception as e:
        return {
            "conversation_id": conversation_id,
            "genie_status": "FAILED",
            "genie_failed": True,
            "choices": [{"message": {"content": f"Genie request failed: {e}"}}],
        }


@app.post("/api/agent/finance_chat")
def agent_finance_chat(payload: dict) -> dict:
    industry = str(payload.get("industry", "mining") or "mining").lower()
    if industry not in INDUSTRIES:
        industry = "mining"
    currency = str(payload.get("currency", "") or "").strip().upper()
    messages = payload.get("messages", [])
    user_text = ""
    if messages:
        user_text = str(messages[-1].get("content", "") or "").strip()
    if not user_text:
        return {"choices": [{"message": {"content": "Please enter a finance question."}}]}

    ov = _overview(industry, display_currency=currency)
    ex = ov.get("executive", {}) if isinstance(ov, dict) else {}
    adoption = ex.get("adoption_insights", {}) if isinstance(ex, dict) else {}
    catalog = _industry_cfg(industry).get("catalog", f"pdm_{industry}")
    finance_daily = f"{catalog}.finance.pm_financial_daily"
    maintenance_schedule = f"{catalog}.lakebase.maintenance_schedule"
    parts_inventory = f"{catalog}.lakebase.parts_inventory"
    predictions = f"{catalog}.gold.pdm_predictions"
    features = f"{catalog}.silver.sensor_features"
    ot_raw = f"{catalog}.bronze.sensor_readings"
    site_finance_daily = f"{catalog}.finance.pm_site_financial_daily"
    site_summary_lines: list[str] = []
    site_rollup: list[dict[str, Any]] = []
    site_ctx = _resolve_site_context(industry, user_text)
    try:
        site_rows = _run_sql(
            f"""
            SELECT
              site_id,
              SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) AS critical_events,
              SUM(CASE WHEN severity='warning' THEN 1 ELSE 0 END) AS warning_events
            FROM {catalog}.gold.financial_impact_events
            GROUP BY site_id
            ORDER BY site_id
            """,
            cache_key=None,
        )
        for r in site_rows:
            sid = str(r.get("site_id") or "")
            if not sid:
                continue
            c = int(_to_float(r.get("critical_events"), 0))
            w = int(_to_float(r.get("warning_events"), 0))
            site_rollup.append({"site_id": sid, "critical_events": c, "warning_events": w})
            site_summary_lines.append(f"- {sid}: critical={c}, warning={w}")
        if not site_rollup:
            fallback_rows = _run_sql(
                f"""
                SELECT site_id, MAX(critical_assets) AS critical_events, MAX(warning_assets) AS warning_events
                FROM {catalog}.finance.pm_site_financial_daily
                GROUP BY site_id
                ORDER BY site_id
                """,
                cache_key=None,
            )
            for r in fallback_rows:
                sid = str(r.get("site_id") or "")
                if not sid:
                    continue
                c = int(_to_float(r.get("critical_events"), 0))
                w = int(_to_float(r.get("warning_events"), 0))
                site_rollup.append({"site_id": sid, "critical_events": c, "warning_events": w})
                site_summary_lines.append(f"- {sid}: critical={c}, warning={w}")
    except Exception:
        site_summary_lines = []
        site_rollup = []

    site_resolution_hint = ""
    if site_ctx:
        site_resolution_hint = (
            "Resolved site context (must use exact site_id/site_key equality filters, not fuzzy text search):\n"
            f"- site_id: {site_ctx.get('site_id')}\n"
            f"- site_key: {site_ctx.get('site_key')}\n"
            f"- site_name: {site_ctx.get('site_name')}\n"
            f"- customer: {site_ctx.get('customer')}\n"
            f"- native_currency: {site_ctx.get('currency')}\n"
        )

    adoption_summary_lines: list[str] = []
    if isinstance(adoption, dict):
        adoption_summary_lines.append(
            f"- Model utilization rate: {float(adoption.get('model_utilization_rate_pct', 0.0)):.1f}%"
        )
        adoption_summary_lines.append(
            f"- Genie queries (30d): {int(_to_float(adoption.get('genie_queries_30d'), 0))}"
        )
        adoption_summary_lines.append(
            f"- Prediction-consuming sites: {int(_to_float(adoption.get('predictions_consumed_sites'), 0))}/{int(_to_float(adoption.get('total_sites'), 0))}"
        )
        adoption_summary_lines.append(
            f"- Cost per prediction: {adoption.get('cost_per_prediction_fmt', '')}"
        )
        adoption_summary_lines.append(
            f"- Avoided cost per prediction: {adoption.get('avoided_cost_per_prediction_fmt', '')}"
        )
        adoption_summary_lines.append(
            f"- Platform ROI (x): {float(adoption.get('platform_roi_x', 0.0)):.1f}x"
        )
        maturity = adoption.get("site_maturity", []) if isinstance(adoption.get("site_maturity", []), list) else []
        if maturity:
            top = sorted(maturity, key=lambda r: _to_float((r or {}).get("maturity_score"), 0.0), reverse=True)[:5]
            adoption_summary_lines.append("- Site maturity leaderboard (top 5):")
            for row in top:
                adoption_summary_lines.append(
                    f"  - {row.get('site_id')}: score={_to_float(row.get('maturity_score'), 0):.1f}, "
                    f"action_rate={_to_float(row.get('action_rate_pct'), 0):.1f}%, "
                    f"queries_30d={int(_to_float(row.get('genie_queries_30d'), 0))}"
                )

    context = (
        f"Finance room contract ({industry}): answer using SQL-grounded reasoning on real tables only.\n"
        f"- EBIT source table: {finance_daily}\n"
        f"- Site-level EBIT source table: {site_finance_daily}\n"
        f"- Work-order/operations tables: {maintenance_schedule}, {parts_inventory}\n"
        f"- Work-order execution table: {catalog}.lakebase.work_orders\n"
        f"- OT signal chain: {ot_raw} -> {features} -> {predictions}\n"
        + site_resolution_hint
        + ("- Site event summary (latest table scan):\n" + "\n".join(site_summary_lines) + "\n" if site_summary_lines else "")
        + ("- Platform adoption insights (executive metrics):\n" + "\n".join(adoption_summary_lines) + "\n" if adoption_summary_lines else "")
        + "Rules for consistent site-level executive answers:\n"
        + "1) For site-level EBIT/value, use finance.pm_site_financial_daily (latest ds per site_id) and report net_benefit, avoided_cost, intervention_cost, critical_assets, warning_assets.\n"
        + "2) For 30-day impact, use gold.financial_impact_events filtered by exact site_id and compute avoided/intervention/expected failure totals.\n"
        + "3) For scheduling readiness, use lakebase.work_orders and lakebase.maintenance_schedule filtered by exact site_id; report totals/open/scheduled and next maintenance window.\n"
        + "4) Never use fuzzy ILIKE matching on industry/site text when a concrete site_id/site_key is available.\n"
        + "5) If a metric is null/empty, explicitly say data is unavailable for that metric and still answer remaining metrics.\n"
        + "6) Keep response concise: 4-6 bullets, executive language, include currency.\n"
        + "7) For platform-adoption questions, use adoption metrics and site maturity data from context before asking follow-up clarifications.\n"
        + "8) For site maturity questions, rank by maturity_score and include action_rate_pct + genie_queries_30d.\n"
        + "Do not claim missing finance/work-order tables unless you explicitly verified they are absent in schema. "
        + "If data is incomplete, state what is missing and provide exact SQL checks."
    )
    merged = f"{user_text}\n\n{context}"
    base_payload = {
        "industry": industry,
        "currency": currency or ex.get("currency", ""),
        "conversation_id": payload.get("conversation_id", ""),
        "room_type": "finance",
        "messages": [{"role": "user", "content": merged}],
    }
    reply = agent_chat(base_payload)
    if isinstance(reply, dict):
        reply.setdefault(
            "finance_context",
            {
                "industry": industry,
                "currency": ex.get("currency", ""),
                "source_table": ex.get("source_table", finance_daily),
                "site_source_table": site_finance_daily,
                "ebit_saved_fmt": ex.get("ebit_saved_fmt", ""),
                "roi_pct": ex.get("roi_pct", 0),
                "site_rollup": site_rollup,
                "adoption_insights": adoption,
            },
        )
    return reply


@app.get("/api/geo/sites")
def geo_sites(industries: str = "", currency: str = "") -> dict[str, Any]:
    selected = [s.strip().lower() for s in str(industries or "").split(",") if s.strip()]
    if not selected:
        selected = list(INDUSTRIES)
    selected = [s for s in selected if s in INDUSTRIES]
    ccy_norm = _normalize_currency(currency or "", "")
    geo_key = f"{','.join(sorted(selected))}|{ccy_norm}"
    now = time.time()
    if _GEO_SITES_CACHE_TTL_S > 0:
        hit = _GEO_SITES_CACHE.get(geo_key)
        if hit and (now - hit[0]) < _GEO_SITES_CACHE_TTL_S:
            return copy.deepcopy(hit[1])
    out: list[dict[str, Any]] = []
    for ind in selected:
        site_metas = _geo_sites_for_industry(ind)
        predictions = _predictions_map(ind)
        for site_meta in site_metas:
            site_id = str(site_meta.get("site_id") or "")
            if not site_id:
                continue
            native_currency = _geo_currency(ind, site_id)
            display_currency = _effective_demo_currency(currency, native_currency)
            cfg_assets = _geo_assets_for_site(ind, site_id)
            rows = [
                _asset_snapshot(ind, a, predictions.get(str(a.get("id") or "")), display_currency=display_currency)
                for a in cfg_assets
                if a.get("id")
            ]
            running = 0
            warning = 0
            critical = 0
            top_alert: dict[str, Any] | None = None
            best_score = -1.0
            for r in rows:
                sev = str(r.get("status") or "running").lower()
                score = _to_float(r.get("anomaly_score"), 0.0)
                if sev == "critical":
                    critical += 1
                elif sev == "warning":
                    warning += 1
                else:
                    running += 1
                sev_rank = 2 if sev == "critical" else (1 if sev == "warning" else 0)
                if sev_rank > 0 and (sev_rank > (2 if top_alert and top_alert.get("severity") == "critical" else 1 if top_alert else 0) or score >= best_score):
                    best_score = score
                    equipment_id = str(r.get("id") or "")
                    jpy_mode = display_currency == "JPY"
                    krw_mode = display_currency == "KRW"
                    top_alert = {
                        "asset_name": equipment_id,
                        "severity": sev,
                        "message": (
                            f"{equipment_id} 異常スコア {score:.2f}"
                            if jpy_mode
                            else f"{equipment_id} 이상 점수 {score:.2f}"
                            if krw_mode
                            else f"{equipment_id} anomaly score {score:.2f}"
                        ),
                    }
            out.append(
                {
                    "site_id": site_id,
                    "name": site_meta.get("name"),
                    "customer": site_meta.get("customer"),
                    "industry": ind,
                    "lat": site_meta.get("lat"),
                    "lng": site_meta.get("lng"),
                    "description": site_meta.get("description"),
                    "currency": native_currency,
                    "asset_counts": {
                        "running": running,
                        "warning": warning,
                        "critical": critical,
                        "total": running + warning + critical,
                    },
                    "top_alert": top_alert,
                }
            )
    payload: dict[str, Any] = {"sites": out}
    if _GEO_SITES_CACHE_TTL_S > 0:
        _GEO_SITES_CACHE[geo_key] = (now, copy.deepcopy(payload))
    return payload


@app.get("/api/geo/assets/{site_id}")
def geo_assets(site_id: str, currency: str = "") -> dict[str, Any]:
    industry, site_meta = _geo_site_meta(site_id)
    if not industry or not site_meta:
        raise HTTPException(status_code=404, detail="Unknown site_id")
    cfg_assets = _geo_assets_for_site(industry, site_id)
    predictions = _predictions_map(industry)
    native_currency = _geo_currency(industry, site_id)
    display_currency = _effective_demo_currency(currency, native_currency)
    overview_assets = [
        _asset_snapshot(industry, a, predictions.get(str(a.get("id") or "")), display_currency=display_currency)
        for a in cfg_assets
        if a.get("id")
    ]
    assets_out: list[dict[str, Any]] = []
    for row in overview_assets:
        aid = str(row.get("id") or "")
        if not aid:
            continue
        status = str(row.get("status") or "running").lower()
        anomaly_score = _to_float(row.get("anomaly_score"), 0.0)
        srng = _asset_rng(industry, aid)
        sensors = _sensor_defs(industry, str(row.get("type") or ""))[:6]
        tag_rows = []
        for sensor in sensors:
            low, high = (sensor.get("normal_range") or [0.0, 100.0])[:2]
            low_f = _to_float(low, 0.0)
            high_f = _to_float(high, 100.0)
            if high_f <= low_f:
                high_f = low_f + 1.0
            span = high_f - low_f
            center = low_f + span * srng.uniform(0.3, 0.8)
            history = [round(center + srng.uniform(-0.08, 0.08) * span, 3) for _ in range(24)]
            slope = history[-1] - history[0] if history else 0.0
            tag_rows.append(
                {
                    "name": str(sensor.get("name") or ""),
                    "value": round(history[-1], 3) if history else round(center, 3),
                    "unit": str(sensor.get("unit") or ""),
                    "trend": "up" if slope > 0.2 else "down" if slope < -0.2 else "stable",
                    "history_24h": history,
                }
            )
        exposure_value = _to_float(row.get("cost_exposure_value"), 0.0)
        avoided_cost = round(exposure_value * 0.62, 2)
        intervention_cost = round(exposure_value * 0.17, 2)
        source_table = "overview.synthetic"
        jpy_mode = display_currency == "JPY"
        krw_mode = display_currency == "KRW"
        alert_msg = (
            (
                f"{aid} はリスクが上昇しています (スコア {anomaly_score:.2f})。"
                if jpy_mode
                else f"{aid} 의 위험도가 상승했습니다 (점수 {anomaly_score:.2f})."
                if krw_mode
                else f"{aid} has elevated risk (score {anomaly_score:.2f})."
            )
            if status in {"critical", "warning"}
            else ""
        )
        active_alert = None
        if status in {"critical", "warning"}:
            active_alert = {
                "severity": status,
                "message": alert_msg,
                "recommended_action": (
                    "根本原因を調査し、計画メンテナンスを実行してください。"
                    if jpy_mode
                    else "근본 원인을 조사하고 계획된 정비를 수행하세요."
                    if krw_mode
                    else "Investigate root cause and execute planned maintenance window."
                ),
            }
        fin_payload = None
        if avoided_cost or intervention_cost:
            fin_payload = {
                "avoided_cost": round(avoided_cost, 2),
                "intervention_cost": round(intervention_cost, 2),
                "currency": display_currency,
            }
        assets_out.append(
            {
                "asset_id": aid,
                "equip_id": aid,
                "name": f"{aid} {str(row.get('type') or 'asset')}".strip(),
                "type": str(row.get("type") or "asset"),
                "model": str(row.get("model") or ""),
                "crumb": str(row.get("crumb") or ""),
                "status": status,
                "anomaly_score": round(anomaly_score, 4),
                "rul_hours": round(_to_float(row.get("rul_hours"), 0.0), 2),
                "anomaly_probability": round(max(0.0, min(1.0, anomaly_score)), 4),
                "confidence": round(max(0.0, min(1.0, 1.0 - abs(0.5 - anomaly_score))), 4),
                "exposure": row.get("cost_exposure", ""),
                "exposure_value": exposure_value,
                "tags": sorted(tag_rows, key=lambda t: str(t.get("name"))),
                "active_alert": active_alert,
                "financial": fin_payload,
                "suggestions": _geo_suggestions(
                    alert_msg,
                    status,
                    avoided_cost,
                    intervention_cost,
                    display_currency,
                ),
                "data_source": source_table,
            }
        )
    return {
        "site_id": site_id,
        "industry": industry,
        "assets": assets_out,
    }


@app.get("/api/geo/schematic/{site_id}")
def geo_schematic(site_id: str) -> dict[str, Any]:
    site_key = str(site_id or "").strip().lower()
    if site_key not in GEO_SCHEMATICS:
        raise HTTPException(status_code=404, detail="Unknown site_id")
    industry = _geo_industry_for_site(site_key)
    if not industry:
        return GEO_SCHEMATICS[site_key]
    cfg_assets = _geo_assets_for_site(industry, site_key)
    eq_ids = [str(a.get("id") or "") for a in cfg_assets if a.get("id")]
    template = GEO_SCHEMATICS[site_key]
    if not eq_ids or not template.get("nodes"):
        return template
    nodes_out: list[dict[str, Any]] = []
    for idx, node in enumerate(template.get("nodes", [])):
        equip_id = eq_ids[idx] if idx < len(eq_ids) else str(node.get("equip_id") or "")
        updated = dict(node)
        updated["equip_id"] = equip_id
        updated["label"] = equip_id or str(node.get("label") or "")
        nodes_out.append(updated)
    return {"subtitle": template.get("subtitle"), "nodes": nodes_out, "pipes": template.get("pipes", [])}


@app.post("/api/geo/genie/ask")
def geo_genie_ask(payload: dict[str, Any]) -> dict[str, Any]:
    industry = str(payload.get("industry", "") or "").strip().lower()
    currency = str(payload.get("currency", "") or "").strip().upper()
    site_id = str(payload.get("site_id", "") or "").strip().lower()
    question = str(payload.get("question", "") or "").strip()
    asset_context = payload.get("asset_context")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    if not question:
        raise HTTPException(status_code=400, detail="Missing question")
    prompt = question
    if currency and currency != "AUTO":
        if currency == "JPY":
            prompt = (
                "Respond in Japanese and use JPY for all monetary values in your answer.\n\n"
                f"{prompt}"
            )
        elif currency == "KRW":
            prompt = (
                "Respond in Korean and use KRW for all monetary values in your answer.\n\n"
                f"{prompt}"
            )
        else:
            prompt = f"Use {currency} for all monetary values in your answer.\n\n{prompt}"
    if asset_context:
        prompt = f"{prompt}\n\nAsset context:\n{json.dumps(asset_context, ensure_ascii=True)}"
    # Use the same path/token/room resolution as existing industry/finance genie flows.
    reply = agent_chat(
        {
            "industry": industry,
            "room_type": "ops",
            "messages": [{"role": "user", "content": prompt}],
        }
    )
    if isinstance(reply, dict):
        choices = reply.get("choices", [])
        if choices and isinstance(choices[0], dict):
            answer = str((choices[0].get("message") or {}).get("content") or "").strip()
            if answer:
                return {"answer": answer}
    raise HTTPException(status_code=500, detail="Genie request failed: no response returned.")


@app.post("/api/geo/alert/action")
def geo_alert_action(payload: dict[str, Any]) -> dict[str, Any]:
    asset_id = str(payload.get("asset_id", "") or "").strip()
    industry = str(payload.get("industry", "") or "").strip().lower()
    action = str(payload.get("action", "") or "").strip().lower()
    note = str(payload.get("note", "") or "").strip()
    if not asset_id:
        raise HTTPException(status_code=400, detail="Missing asset_id")
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    if action not in {"approve", "reject", "defer"}:
        raise HTTPException(status_code=400, detail="action must be approve, reject, or defer")
    return ui_recommendation_action(
        {
            "industry": industry,
            "equipment_id": asset_id,
            "decision": action,
            "note": note,
        }
    )


@app.get("/api/docs/finance-report")
def finance_report_pdf() -> FileResponse:
    candidate = APP_FINANCE_REPORT_PDF if APP_FINANCE_REPORT_PDF.exists() else FINANCE_REPORT_PDF
    if not candidate.exists():
        raise HTTPException(status_code=404, detail="Finance report PDF not found")
    return FileResponse(
        candidate,
        media_type="application/pdf",
        filename=candidate.name,
    )


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    index = DIST / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"status": "frontend-not-built", "path": full_path}
