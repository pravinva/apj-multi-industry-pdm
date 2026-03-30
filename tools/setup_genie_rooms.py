#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient

INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
DEFAULT_WAREHOUSE_ID = "4b9b953939869799"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create/update OT PdM Genie rooms per industry.")
    p.add_argument("--profile", default="DEFAULT")
    p.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
    p.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "app" / "genie_rooms.json"),
        help="Path to write industry -> space_id mapping JSON",
    )
    return p.parse_args()


def _title(industry: str) -> str:
    return f"OT PdM Intelligence - {industry.title()} Genie"


def _description(industry: str) -> str:
    return (
        f"AI/BI Genie room for OT PdM {industry} skin. "
        "Includes fleet health, anomaly, RUL, parts, and maintenance analysis."
    )


def _space_payload(industry: str) -> dict[str, Any]:
    catalog = f"pdm_{industry}"
    table_ids = sorted(
        [
            f"{catalog}.gold.pdm_predictions",
            f"{catalog}.silver.sensor_features",
            f"{catalog}.bronze.sensor_readings",
            f"{catalog}.lakebase.parts_inventory",
            f"{catalog}.lakebase.maintenance_schedule",
            f"{catalog}.bronze.feature_vectors",
            f"{catalog}.gold.financial_impact_events",
        ]
    )
    def _hid(label: str) -> str:
        return hashlib.md5(f"{industry}:{label}".encode("utf-8")).hexdigest()

    sample_questions = [
        {
            "id": _hid("sq1"),
            "question": [f"Which {industry} assets are currently critical by anomaly score and lowest RUL?"],
        },
        {"id": _hid("sq2"), "question": ["Show fleet health trend and explain top contributing sensors for the worst assets."]},
        {"id": _hid("sq3"), "question": ["Which parts are low stock for assets with high near-term failure risk?"]},
        {"id": _hid("sq4"), "question": ["What maintenance windows are available for high-risk assets in the next shifts?"]},
        {"id": _hid("sq5"), "question": ["Summarize protocol quality mix and latest sensor anomalies by site/area/unit."]},
    ]
    text_instructions = [
        {
            "id": _hid("instr"),
            "content": [
                f"You are the OT PdM copilot for the {industry.title()} industry skin.\n\n",
                "Primary goals:\n",
                "- Prioritize assets by operational risk using anomaly score, RUL, and quality trends.\n",
                "- Tie risk to operational action: maintenance timing and parts readiness.\n",
                "- Keep responses concise, decision-oriented, and explicit about confidence.\n\n",
                "Data usage rules:\n",
                "- Use latest rows when ranking current risk.\n",
                "- Use weighted/aggregated KPIs for fleet views; avoid naive averaging of percentages.\n",
                "- Prefer Unity Catalog table fields over inferred values.\n",
                "- If data is missing for a question, state assumptions and suggest the exact SQL check.\n\n",
                "Relevant tables:\n",
                f"- {catalog}.gold.pdm_predictions (latest anomaly/RUL)\n",
                f"- {catalog}.silver.sensor_features (sensor behavior features)\n",
                f"- {catalog}.bronze.sensor_readings (stream-level context)\n",
                f"- {catalog}.lakebase.parts_inventory (parts stock/readiness)\n",
                f"- {catalog}.lakebase.maintenance_schedule (maintenance windows)\n",
                f"- {catalog}.gold.financial_impact_events (event-level OT+finance impact)\n",
            ]
        }
    ]
    example_question_sqls = [
        {
            "id": _hid("ex1"),
            "question": ["Latest top-risk assets with anomaly and RUL"],
            "sql": [
                "WITH latest AS (\n",
                "  SELECT *,\n",
                "         ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY prediction_timestamp DESC) AS rn\n",
                f"  FROM {catalog}.gold.pdm_predictions\n",
                ")\n",
                "SELECT equipment_id, anomaly_score, rul_hours, top_contributing_sensor, prediction_timestamp\n",
                "FROM latest\n",
                "WHERE rn = 1\n",
                "ORDER BY anomaly_score DESC, rul_hours ASC\n",
                "LIMIT 20;"
            ],
        },
        {
            "id": _hid("ex2"),
            "question": ["Parts at risk for high-risk assets"],
            "sql": [
                "WITH risky AS (\n",
                "  SELECT equipment_id,\n",
                "         ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY prediction_timestamp DESC) rn,\n",
                "         anomaly_score, rul_hours\n",
                f"  FROM {catalog}.gold.pdm_predictions\n",
                ")\n",
                "SELECT r.equipment_id, r.anomaly_score, r.rul_hours,\n",
                "       p.part_number, p.description, p.quantity, p.reorder_point, p.lead_time_days\n",
                "FROM risky r\n",
                f"LEFT JOIN {catalog}.lakebase.parts_inventory p ON 1=1\n",
                "WHERE r.rn = 1 AND (r.anomaly_score >= 0.6 OR r.rul_hours <= 72)\n",
                "ORDER BY r.anomaly_score DESC, r.rul_hours ASC, p.quantity ASC\n",
                "LIMIT 100;"
            ],
        },
        {
            "id": _hid("ex3"),
            "question": ["Upcoming maintenance windows for risky assets"],
            "sql": [
                "WITH risky AS (\n",
                "  SELECT equipment_id,\n",
                "         ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY prediction_timestamp DESC) rn,\n",
                "         anomaly_score, rul_hours\n",
                f"  FROM {catalog}.gold.pdm_predictions\n",
                ")\n",
                "SELECT r.equipment_id, r.anomaly_score, r.rul_hours,\n",
                "       m.shift_label, m.shift_start, m.shift_end, m.maintenance_window_start, m.maintenance_window_end, m.crew_available\n",
                "FROM risky r\n",
                f"LEFT JOIN {catalog}.lakebase.maintenance_schedule m ON r.equipment_id = m.equipment_id\n",
                "WHERE r.rn = 1 AND (r.anomaly_score >= 0.6 OR r.rul_hours <= 72)\n",
                "ORDER BY r.anomaly_score DESC, m.shift_start ASC\n",
                "LIMIT 100;"
            ],
        },
    ]
    return {
        "version": 2,
        "config": {
            "sample_questions": sorted(sample_questions, key=lambda x: x["id"])
        },
        "data_sources": {
            "tables": [
                {"identifier": table_id}
                for table_id in table_ids
            ]
        },
        "instructions": {
            "text_instructions": sorted(text_instructions, key=lambda x: x["id"]),
            "example_question_sqls": sorted(example_question_sqls, key=lambda x: x["id"]),
        },
    }


def _find_space_id_by_title(w: WorkspaceClient, title: str) -> str:
    page_token: str | None = None
    while True:
        query: dict[str, Any] = {}
        if page_token:
            query["page_token"] = page_token
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query=query)
        for s in resp.get("spaces") or []:
            if (s.get("title") or "").strip() == title:
                return s.get("space_id") or ""
        page_token = resp.get("next_page_token")
        if not page_token:
            return ""


def _create_space(w: WorkspaceClient, industry: str, warehouse_id: str) -> str:
    payload = _space_payload(industry)
    created = w.api_client.do(
        "POST",
        "/api/2.0/genie/spaces",
        body={
            "title": _title(industry),
            "description": _description(industry),
            "warehouse_id": warehouse_id,
            "serialized_space": json.dumps(payload),
        },
    )
    return created["space_id"]


def _update_space(w: WorkspaceClient, space_id: str, industry: str, warehouse_id: str) -> None:
    payload = _space_payload(industry)
    w.api_client.do(
        "PATCH",
        f"/api/2.0/genie/spaces/{space_id}",
        body={
            "title": _title(industry),
            "description": _description(industry),
            "warehouse_id": warehouse_id,
            "serialized_space": json.dumps(payload),
        },
    )


def main() -> None:
    args = parse_args()
    w = WorkspaceClient(profile=args.profile)
    mapping: dict[str, str] = {}

    for industry in INDUSTRIES:
        title = _title(industry)
        space_id = _find_space_id_by_title(w, title)
        if space_id:
            _update_space(w, space_id, industry, args.warehouse_id)
            print(f"updated {industry}: {space_id}")
        else:
            space_id = _create_space(w, industry, args.warehouse_id)
            print(f"created {industry}: {space_id}")
        mapping[industry] = space_id

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")
    print(f"wrote mapping: {out_path}")


if __name__ == "__main__":
    main()

