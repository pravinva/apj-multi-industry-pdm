"""
OT PdM Intelligence - Deployment Guide
Run this notebook/script top-to-bottom on a clean workspace.
"""

import json
import os
from pathlib import Path

from core.config.loader import load_config


INDUSTRY = os.environ.get("INDUSTRY", "mining")
INGEST_SOURCE_TABLE = os.environ.get("OT_PDM_INGEST_SOURCE_TABLE", "pravin_zerobus")

print(f"Deploying OT PdM Intelligence - industry: {INDUSTRY}")
config = load_config(INDUSTRY)
catalog = config["catalog"]


def _exec_sql_file(sql_path: Path, catalog_name: str) -> None:
    raw = sql_path.read_text(encoding="utf-8")
    rendered = raw.replace("${catalog_name}", catalog_name)
    statements = [s.strip() for s in rendered.split(";") if s.strip()]
    for stmt in statements:
        spark.sql(stmt)  # noqa: F821


def _seed_table(table_name: str, seed_file: Path) -> None:
    if not seed_file.exists():
        print(f"Seed file missing, skipping: {seed_file}")
        return
    rows = json.loads(seed_file.read_text(encoding="utf-8"))
    if not rows:
        print(f"No rows in seed file: {seed_file}")
        return
    df = spark.createDataFrame(rows)  # noqa: F821
    df.write.mode("append").format("delta").saveAsTable(table_name)
    print(f"Seeded {len(rows)} rows into {table_name}")


# Step 1: Create catalog and schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")  # noqa: F821
for schema in ["bronze", "silver", "gold", "lakebase", "agent_tools", "models"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")  # noqa: F821

# Step 2: Run DDL
_exec_sql_file(Path("core/catalog/schema.sql"), catalog)

# Step 3: Seed Lakebase with industry-specific data
seed_root = Path("industries") / INDUSTRY / "seed"
_seed_table(f"{catalog}.lakebase.parts_inventory", seed_root / "parts_inventory.json")
_seed_table(
    f"{catalog}.lakebase.maintenance_schedule",
    seed_root / "maintenance_schedule.json",
)

# Step 4: Insert asset metadata from config
asset_rows = []
for asset in config.get("simulator", {}).get("assets", []):
    asset_rows.append(
        {
            "equipment_id": asset["id"],
            "site_id": asset.get("site", ""),
            "area_id": asset.get("area", ""),
            "unit_id": asset.get("unit", ""),
            "asset_type": asset.get("type", ""),
            "asset_model": asset.get("model", ""),
            "industry": INDUSTRY,
            "cost_per_unit": config.get("accounts", {}).get("pipeline_monthly", 0) / 720.0,
            "cost_currency": config.get("agent", {})
            .get("terminology", {})
            .get("cost_currency", "AUD"),
        }
    )

if asset_rows:
    spark.createDataFrame(asset_rows).write.mode("append").format("delta").saveAsTable(  # noqa: F821
        f"{catalog}.bronze.asset_metadata"
    )

print("Step 5: Start DLT pipeline -> run via databricks bundle deploy/run job.")
print("Step 6: Trigger training job -> run ot_pdm_training_job after ~5 mins of data.")
print("\nDeployment baseline complete.")
print(f"Catalog:  {catalog}")
print(f"Industry: {config['display_name']}")
print(f"Ingest source table: {catalog}.bronze.{INGEST_SOURCE_TABLE}")
