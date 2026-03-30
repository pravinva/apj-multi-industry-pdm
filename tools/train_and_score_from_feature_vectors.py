from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from sklearn.ensemble import GradientBoostingRegressor, IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

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
    if isinstance(v, (int, float, np.floating)):
        if not math.isfinite(float(v)):
            return "NULL"
        return str(float(v))
    if isinstance(v, str):
        return f"'{_escape(v)}'"
    return f"'{_escape(str(v))}'"


def _run_query(client: WorkspaceClient, statement: str) -> list[dict[str, Any]]:
    resp = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="50s",
        disposition=sql_service.Disposition.INLINE,
    )
    if resp.status.state != sql_service.StatementState.SUCCEEDED:
        err = resp.status.error
        msg = f"{getattr(err, 'error_code', '')}: {getattr(err, 'message', '')}" if err else ""
        raise RuntimeError(f"SQL failed: {msg}")

    result = resp.result
    if not result or not result.data_array:
        return []
    cols = []
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        cols = [c.name for c in resp.manifest.schema.columns]
    out = []
    for row in result.data_array:
        if cols:
            out.append({cols[i]: row[i] for i in range(min(len(cols), len(row)))})
        else:
            out.append({str(i): row[i] for i in range(len(row))})
    return out


def _exec(client: WorkspaceClient, statement: str) -> None:
    _run_query(client, statement)


def _load_config(industry: str) -> dict[str, Any]:
    path = ROOT / "industries" / industry / "config.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _train_and_score_asset(df: pd.DataFrame, severity: float) -> tuple[float, str, float, str, float]:
    feature_cols = [c for c in df.columns if c not in {"equipment_id", "window_start", "window_end", "_processed_at"}]
    x = df[feature_cols].astype(float).fillna(0.0)
    if len(x) < 20:
        return 0.1, "normal", 720.0, feature_cols[0] if feature_cols else "feature_1", 0.1

    iso = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("iso", IsolationForest(n_estimators=200, contamination=0.08, random_state=42)),
        ]
    )
    iso.fit(x)
    raw = -iso.decision_function(x)
    min_v, max_v = float(np.min(raw)), float(np.max(raw))
    norm = np.zeros_like(raw) if abs(max_v - min_v) < 1e-9 else (raw - min_v) / (max_v - min_v)
    anomaly = float(norm[-1])
    label = "anomaly" if anomaly >= 0.5 else "normal"

    n = len(x)
    base_life = max(80.0, 400.0 * (1 - severity))
    y = np.linspace(base_life, max(4.0, base_life * 0.05), n)
    gbr = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("gbr", GradientBoostingRegressor(n_estimators=250, max_depth=3, learning_rate=0.05, random_state=42)),
        ]
    )
    gbr.fit(x, y)
    rul = float(max(4.0, gbr.predict(x.tail(1))[0]))

    z = ((x - x.mean()) / (x.std().replace(0, 1.0))).abs().iloc[-1]
    top_sensor = str(z.idxmax()) if len(z.index) else "feature_1"
    top_score = float(z.max()) if len(z.index) else float(anomaly)
    return anomaly, label, rul, top_sensor, top_score


def run_industry(client: WorkspaceClient, industry: str) -> None:
    cfg = _load_config(industry)
    catalog = cfg["catalog"]
    severity_map = {
        a["id"]: float(a.get("fault_severity", 0.0))
        for a in cfg.get("simulator", {}).get("assets", [])
        if a.get("id")
    }

    rows = _run_query(
        client,
        f"""
        SELECT *
        FROM {catalog}.bronze.feature_vectors
        ORDER BY equipment_id, window_start
        """.strip(),
    )
    if not rows:
        print(f"[skip] no feature vectors for {industry}")
        return

    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"])
    df["window_end"] = pd.to_datetime(df["window_end"])

    out_rows = []
    now = datetime.now(timezone.utc)
    for eid, group in df.groupby("equipment_id"):
        g = group.sort_values("window_start").tail(120)
        sev = severity_map.get(eid, 0.0)
        anomaly, label, rul, top_sensor, top_score = _train_and_score_asset(g, sev)
        pf = (now + timedelta(hours=rul)).strftime("%Y-%m-%d %H:%M:%S") if rul < 9000 else None
        out_rows.append(
            "("
            + ", ".join(
                [
                    _sql_literal(eid),
                    _sql_literal(now.strftime("%Y-%m-%d %H:%M:%S")),
                    _sql_literal(round(anomaly, 6)),
                    _sql_literal(label),
                    _sql_literal(round(rul, 3)),
                    _sql_literal(pf),
                    _sql_literal(top_sensor),
                    _sql_literal(round(top_score, 6)),
                    _sql_literal("local-iso-v1"),
                    _sql_literal("local-gbr-v1"),
                    "current_timestamp()",
                ]
            )
            + ")"
        )

    _exec(client, f"DELETE FROM {catalog}.gold.pdm_predictions WHERE TRUE")
    _exec(
        client,
        f"""
        INSERT INTO {catalog}.gold.pdm_predictions
        (equipment_id, prediction_timestamp, anomaly_score, anomaly_label, rul_hours, predicted_failure_date, top_contributing_sensor, top_contributing_score, model_version_anomaly, model_version_rul, _scored_at)
        VALUES {", ".join(out_rows)}
        """.strip(),
    )
    print(f"[ok] predictions scored for {industry} ({len(out_rows)} assets)")


def main() -> None:
    client = WorkspaceClient(profile="DEFAULT")
    for industry in INDUSTRIES:
        run_industry(client, industry)


if __name__ == "__main__":
    main()
