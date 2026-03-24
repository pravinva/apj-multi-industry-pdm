from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"
INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]

app = FastAPI(title="OT PdM Intelligence")

if (DIST / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")


def _load_seed(industry: str, seed_name: str) -> list[dict]:
    path = ROOT.parent / "industries" / industry / "seed" / seed_name
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _asset_ids(industry: str) -> list[str]:
    cfg_path = ROOT.parent / "industries" / industry / "config.yaml"
    if not cfg_path.exists():
        return []
    import yaml

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    return [a["id"] for a in cfg.get("simulator", {}).get("assets", [])]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/fleet/assets")
def fleet_assets(industry: str = "mining") -> list[dict]:
    if industry not in INDUSTRIES:
        raise HTTPException(status_code=400, detail="Invalid industry")
    assets = []
    for aid in _asset_ids(industry):
        anomaly = round(random.uniform(0.12, 0.92), 2)
        health_pct = max(5, int((1 - anomaly) * 100))
        assets.append(
            {
                "equipment_id": aid,
                "anomaly_score": anomaly,
                "anomaly_label": "anomaly" if anomaly >= 0.5 else "normal",
                "rul_hours": round(random.uniform(8, 240), 1),
                "health_score_pct": health_pct,
            }
        )
    return assets


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
    rows = _load_seed(industry, "parts_inventory.json")
    return {"equipment_id": asset_id, "parts": rows}


@app.get("/api/stream/latest")
def stream_latest(industry: str = "mining", limit: int = 50) -> dict:
    now = datetime.now(timezone.utc)
    assets = _asset_ids(industry) or ["ASSET-01"]
    rows = []
    for i in range(min(limit, 200)):
        rows.append(
            {
                "timestamp": (now - timedelta(seconds=i * 5)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "site_id": "site_1",
                "area_id": "area_1",
                "unit_id": "unit_1",
                "equipment_id": random.choice(assets),
                "tag_name": f"sensor_{(i % 6) + 1}",
                "value": round(random.uniform(1.0, 100.0), 2),
                "unit": "u",
                "quality": random.choice(["good", "uncertain", "bad"]),
                "source_protocol": random.choice(["OPC-UA", "MQTT", "Modbus"]),
            }
        )
    return {"rows": rows}


@app.post("/api/agent/chat")
def agent_chat(payload: dict) -> dict:
    messages = payload.get("messages", [])
    user_text = ""
    if messages:
        user_text = messages[-1].get("content", "")
    answer = (
        f"Diagnosis: potential developing fault for the selected asset. "
        f"Action: schedule inspection in the next shift, verify parts, and prepare a maintenance window. "
        f"User message: {user_text}"
    )
    return {"choices": [{"message": {"content": answer}}]}


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    index = DIST / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"status": "frontend-not-built", "path": full_path}
