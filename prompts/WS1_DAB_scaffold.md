# Workstream 1 — DAB Scaffold, Unity Catalog Schema, Config Loader

## Owner
Pravin Varma

## Depends on
Nothing. This is the root workstream. All other workstreams depend on it.

## Deliverables
- `databricks.yml` — full DAB defining all resources
- `industries/{industry}/config.yaml` × 5 (mining, energy, water, automotive, semiconductor)
- `core/config/loader.py` — config loader used by all downstream components
- `core/catalog/schema.sql` — Unity Catalog DDL template
- `RUNME.py` — deployment guide notebook

---

## Context

This accelerator is config-driven. A single `databricks.yml` deploys the entire stack for any of the five industry skins. Swapping one YAML file changes simulator data, sensor schemas, ISA-95 hierarchy, ML model parameters, agent persona, and dashboard.

The five industries are: **mining** (Rio Tinto haul fleet), **energy** (Alinta wind + BESS + transformer), **water** (Sydney Water pumps + smart meters), **automotive** (Toyota stamping press + robotic welder), **semiconductor** (Renesas etch chamber + stepper).

---

## Task 1.1 — databricks.yml

Create `databricks.yml` at repo root. It must define all the following resources using the Databricks Asset Bundle schema:

```yaml
bundle:
  name: ot-pdm-intelligence

variables:
  industry:
    description: "Industry skin to deploy"
    default: mining
  use_simulator:
    description: "Use OT simulator instead of live Zerobus connector"
    default: "true"
  catalog_name:
    description: "Unity Catalog catalog name"
    default: "pdm_${var.industry}"

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production
```

Resources to define in `resources:`:

**DLT Pipeline** — `ot_pdm_dlt_pipeline`:
- Continuous mode
- Libraries: `core/dlt/bronze.py`, `core/dlt/silver.py`, `core/dlt/gold.py`
- Configuration: pass `industry`, `use_simulator`, `catalog_name` as pipeline parameters
- Cluster: single node, DBR 15.4 LTS ML

**MLflow Experiment** — `ot_pdm_mlflow_experiment`:
- Path: `/Shared/ot-pdm-intelligence/${var.industry}`

**Model Serving Endpoint** — `ot_pdm_model_endpoint`:
- Name: `ot-pdm-${var.industry}`
- Config: served model pointing to registered model `ot_pdm_rul_${var.industry}`, scale to zero

**Genie Space** — `ot_pdm_genie`:
- Display name: `OT PdM Intelligence — ${var.industry}`
- Tables: `${var.catalog_name}.gold.pdm_predictions`, `${var.catalog_name}.lakebase.parts_inventory`, `${var.catalog_name}.lakebase.maintenance_schedule`

**Databricks App** — `ot_pdm_app`:
- Source: `app/`
- Description: `OT PdM Intelligence — ${var.industry}`

**Jobs** (for batch scoring):
- `ot_pdm_scoring_job`: runs `core/ml/batch_score.py` on schedule `0 */2 * * *`
- `ot_pdm_training_job`: runs `core/ml/train.py` on schedule `0 2 * * *`

---

## Task 1.2 — industry_config.yaml × 5

Create `industries/{industry}/config.yaml` for each of the five industries. The schema for each file must include all of the following top-level keys. Use the exact field names — downstream code imports these with `config['key']`.

### Required schema

```yaml
industry: <string>              # key: mining | energy | water | automotive | semiconductor
display_name: <string>          # human-readable
catalog: <string>               # Unity Catalog catalog name, e.g. pdm_mining
version: <string>               # semver

accounts:                       # pitch deck context only, not used in pipeline
  primary: <string>
  references: [<string>]
  pipeline_monthly: <number>    # USD

isa95_hierarchy:
  levels:
    - key: <string>             # site | area | unit | equipment | component
      display: <string>
      example: <string>

simulator:
  tick_interval_ms: <int>
  noise_factor: <float>         # 0.01–0.05
  assets:
    - id: <string>
      type: <string>            # snake_case asset class
      model: <string>           # manufacturer model name
      site: <string>
      area: <string>
      unit: <string>
      inject_fault: <string>    # optional: failure_mode key
      fault_severity: <float>   # 0.0–1.0
      fault_start_offset_hours: <int>  # negative = fault began N hours ago

sensors:
  <asset_type>:
    - name: <string>            # snake_case, becomes column name in Bronze
      display: <string>
      unit: <string>
      opc_ua_node: <string>     # e.g. ns=2;s=Engine.EGT
      normal_range: [<float>, <float>]
      warning_threshold: <float|null>
      critical_threshold: <float|null>
      dir: <int>                # 1 = higher is worse, -1 = lower is worse (default 1)
      failure_mode: <string|null>   # which failure_mode this sensor is primary for

failure_modes:
  <mode_key>:
    display: <string>
    sensors: [<string>]         # sensor names that contribute to this mode
    mtbf_hours: <int|null>
    repair_time_hours: <float>
    cost_per_event: <float>     # in cost_currency
    downtime_type: <string>

features:
  - name: <string>
    formula: mean | stddev | slope | zscore | cumsum
    window: <string>            # e.g. "15 minutes", "1 hour", "30 days"
    apply_to: all | [<sensor_names>]

agent:
  persona: <string>
  system_prompt_file: <string>  # path to system_prompt.txt in same directory
  terminology:
    asset: <string>
    asset_plural: <string>
    downtime_event: <string>
    maintenance_window: <string>
    production_unit: <string>
    cost_currency: <string>     # AUD | USD | JPY
    cost_event_label: <string>
    site_timezone: <string>     # AWST | AEST | JST | UTC

dashboard:
  primary_kpi: <string>
  kpis:
    - label: <string>
      field: <string>           # field name in gold.asset_health_summary view
      format: percent_1dp | integer | currency | megalitres_1dp
      good_direction: up | down
      good_threshold: <float>
```

### Populate with real data

**mining:** 4 assets (HT-012 Cat 793F inject engine_overheat sev 0.92, HT-007 inject bearing_wear sev 0.68, HT-001 healthy, CV-003 Overland Conveyor healthy). Sensors: engine_egt, coolant_temp, oil_temp, vibration_rms, tyre_pressure_fl, tyre_pressure_fr, payload_weight, fuel_rate, belt_tension, motor_current, roller_vibration, throughput_rate. Failure modes: engine_overheat ($45K AUD), bearing_wear ($12K), tyre_blowout ($8K), belt_slip ($25K), motor_overload ($35K). Timezone: AWST.

**energy:** 4 assets (WT-004 Vestas V150 inject gearbox_degradation sev 0.91, BESS-01 Tesla Megapack inject thermal_runaway sev 0.71, TX-07 transformer healthy, WT-011 healthy). Sensors: gearbox_vibration, nacelle_temp, rotor_speed, output_power, cell_temp_max, cell_voltage_min, soc, oil_temp, hydrogen_ppm, load_factor. Failure modes: gearbox_degradation, transformer_thermal_failure, battery_thermal_runaway. Timezone: AEST.

**water:** 4 assets (PS-07 centrifugal pump inject bearing_wear sev 0.92, MT-03 smart meter inject pipe_leak sev 0.71, TP-01 chlorination unit healthy, VS-11 vent shaft healthy). Protocol mix: OPC-UA for pumps, MQTT for smart meters. Failure modes include EPA regulatory_note field. Timezone: AEST.

**automotive:** 4 assets (TP-07 stamping press inject die_wear sev 0.93, WR-14 robotic welder inject servo_degradation sev 0.65, CNC-22 healthy, CV-A3 assembly conveyor healthy). Japanese labels in agent terminology. Cost currency: JPY. Timezone: JST.

**semiconductor:** 4 assets (ET-04 plasma etch inject chamber_contamination sev 0.91, LT-11 ASML stepper inject overlay_drift sev 0.68, CMP-07 CMP tool healthy, IN-02 KLA inspection healthy). SEMI E10-compliant hierarchy (fab/bay/tool_class/equipment/chamber). Cost currency: USD. Timezone: JST.

Also create `industries/{industry}/system_prompt.txt` for each industry — the full agent system prompt in plain text, referenced from config.

---

## Task 1.3 — Config loader (core/config/loader.py)

```python
# core/config/loader.py
"""
Loads and validates industry_config.yaml.
Used by DLT pipelines, ML training, agent, and app.
"""
import yaml
from pathlib import Path
from typing import Any

def load_config(industry: str, config_root: str = "industries") -> dict[str, Any]:
    """Load config for the given industry. Validates required keys."""
    path = Path(config_root) / industry / "config.yaml"
    with open(path) as f:
        config = yaml.safe_load(f)
    _validate(config)
    return config

def _validate(config: dict) -> None:
    required = ["industry", "catalog", "isa95_hierarchy", "simulator", "sensors",
                "failure_modes", "features", "agent", "dashboard"]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")

def get_asset_types(config: dict) -> list[str]:
    return list(config["sensors"].keys())

def get_sensors_for_asset(config: dict, asset_id: str) -> list[dict]:
    asset = next((a for a in config["simulator"]["assets"] if a["id"] == asset_id), None)
    if not asset:
        return []
    return config["sensors"].get(asset["type"], [])

def get_isa95_fields(config: dict) -> list[str]:
    return [level["key"] for level in config["isa95_hierarchy"]["levels"]]

def get_failure_modes(config: dict) -> dict:
    return config["failure_modes"]

def get_agent_config(config: dict) -> dict:
    return config["agent"]
```

---

## Task 1.4 — Unity Catalog DDL (core/catalog/schema.sql)

Generate parameterised DDL. All tables prefixed with `${catalog_name}`. Run via `%run core/catalog/schema` in RUNME.py.

```sql
-- Bronze: raw sensor readings (streaming)
CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.sensor_readings (
  site_id        STRING NOT NULL,
  area_id        STRING NOT NULL,
  unit_id        STRING NOT NULL,
  equipment_id   STRING NOT NULL,
  component_id   STRING,
  tag_name       STRING NOT NULL,
  value          DOUBLE NOT NULL,
  unit           STRING,
  quality        STRING NOT NULL,   -- "good" | "uncertain" | "bad"
  quality_code   STRING NOT NULL,   -- OPC-UA: "0x00" | "0x40" | "0x80"
  source_protocol STRING NOT NULL,  -- OPC-UA | MQTT | Modbus | CAN bus
  timestamp      TIMESTAMP NOT NULL,
  _ingested_at   TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Silver: engineered features
CREATE TABLE IF NOT EXISTS ${catalog_name}.silver.sensor_features (
  equipment_id      STRING NOT NULL,
  tag_name          STRING NOT NULL,
  window_start      TIMESTAMP NOT NULL,
  window_end        TIMESTAMP NOT NULL,
  mean_15m          DOUBLE,
  stddev_15m        DOUBLE,
  slope_1h          DOUBLE,
  zscore_30d        DOUBLE,
  quality_good_pct  DOUBLE,         -- % of readings with quality=good in window
  reading_count     INT,
  _processed_at     TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Gold: per-asset PdM predictions
CREATE TABLE IF NOT EXISTS ${catalog_name}.gold.pdm_predictions (
  equipment_id              STRING NOT NULL,
  prediction_timestamp      TIMESTAMP NOT NULL,
  anomaly_score             DOUBLE NOT NULL,    -- 0.0–1.0, Isolation Forest
  anomaly_label             STRING NOT NULL,    -- "normal" | "anomaly"
  rul_hours                 DOUBLE,             -- NULL if no degradation detected
  predicted_failure_date    TIMESTAMP,
  top_contributing_sensor   STRING,
  top_contributing_score    DOUBLE,
  model_version_anomaly     STRING,
  model_version_rul         STRING,
  _scored_at                TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Gold: asset health summary (view joining predictions + asset metadata)
CREATE OR REPLACE VIEW ${catalog_name}.gold.asset_health_summary AS
SELECT
  p.equipment_id,
  a.site_id, a.area_id, a.unit_id,
  a.asset_type, a.asset_model,
  p.anomaly_score,
  CASE
    WHEN p.anomaly_score >= 0.8 THEN 'critical'
    WHEN p.anomaly_score >= 0.5 THEN 'warning'
    ELSE 'healthy'
  END AS health_status,
  ROUND((1 - p.anomaly_score) * 100, 1) AS health_score_pct,
  p.rul_hours,
  p.predicted_failure_date,
  p.top_contributing_sensor,
  p._scored_at
FROM ${catalog_name}.gold.pdm_predictions p
JOIN ${catalog_name}.bronze.asset_metadata a USING (equipment_id)
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.equipment_id ORDER BY p._scored_at DESC) = 1;

-- Lakebase: parts inventory
CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.parts_inventory (
  part_number    STRING NOT NULL,
  description    STRING,
  quantity       INT NOT NULL DEFAULT 0,
  location       STRING,
  depot          STRING,
  unit_cost      DOUBLE,
  currency       STRING DEFAULT 'AUD',
  reorder_point  INT DEFAULT 1,
  lead_time_days INT,
  last_updated   TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Lakebase: work orders
CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.work_orders (
  work_order_id  STRING NOT NULL,
  equipment_id   STRING NOT NULL,
  failure_mode   STRING,
  priority       STRING NOT NULL,   -- critical | high | medium | low
  status         STRING NOT NULL DEFAULT 'draft',  -- draft | submitted | in_progress | complete
  scheduled_time TIMESTAMP,
  parts_required ARRAY<STRING>,
  estimated_hours DOUBLE,
  created_at     TIMESTAMP DEFAULT current_timestamp(),
  created_by     STRING DEFAULT current_user(),
  updated_at     TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Lakebase: maintenance schedule
CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.maintenance_schedule (
  equipment_id   STRING NOT NULL,
  shift_label    STRING NOT NULL,   -- e.g. "Day Shift", "Night Shift", "Shift 2"
  shift_start    TIMESTAMP NOT NULL,
  shift_end      TIMESTAMP NOT NULL,
  planned_downtime_hours DOUBLE DEFAULT 0,
  maintenance_window_start TIMESTAMP,
  maintenance_window_end   TIMESTAMP,
  crew_available BOOLEAN DEFAULT true
)
USING DELTA;

-- Lakebase: asset feature vectors (real-time feature store)
CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.asset_feature_vectors (
  equipment_id   STRING NOT NULL,
  feature_vector ARRAY<DOUBLE> NOT NULL,
  feature_names  ARRAY<STRING> NOT NULL,
  computed_at    TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA;

-- Bronze: asset metadata (populated at deploy time from config)
CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.asset_metadata (
  equipment_id  STRING NOT NULL,
  site_id       STRING NOT NULL,
  area_id       STRING NOT NULL,
  unit_id       STRING NOT NULL,
  asset_type    STRING NOT NULL,
  asset_model   STRING,
  industry      STRING NOT NULL,
  created_at    TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;
```

---

## Task 1.5 — RUNME.py

Notebook-style deployment guide. Must:
1. Load config for the target industry
2. Create Unity Catalog schemas (bronze, silver, gold, lakebase, agent_tools, models)
3. Run DDL from schema.sql
4. Seed Lakebase tables from `industries/{industry}/seed/` JSON files
5. Start the DLT pipeline
6. Trigger the training job
7. Start the Databricks App

```python
# RUNME.py
# ============================================================
# OT PdM Intelligence — Deployment Guide
# Run this notebook top-to-bottom on a clean workspace.
# Expected deploy time: 12–15 minutes.
# ============================================================

import os
from core.config.loader import load_config

INDUSTRY = os.environ.get("INDUSTRY", "mining")
USE_SIMULATOR = os.environ.get("USE_SIMULATOR", "true")

print(f"Deploying OT PdM Intelligence — industry: {INDUSTRY}")
config = load_config(INDUSTRY)
catalog = config["catalog"]

# Step 1: Create catalog and schemas
# ... (spark.sql calls)

# Step 2: Run DDL
# ... (read schema.sql, substitute ${catalog_name}, execute)

# Step 3: Seed Lakebase with industry-specific data
# ... (read seed JSON files, write to Delta tables)

# Step 4: Insert asset metadata from config
# ... (iterate config["simulator"]["assets"], write to bronze.asset_metadata)

# Step 5: Start DLT pipeline
# ... (dbutils.notebook.run or API call)

# Step 6: Trigger training job
# ... (if data is available after simulator runs for 5 minutes)

# Step 7: Print app URL
print(f"\nDeployment complete.")
print(f"  Catalog:   {catalog}")
print(f"  Industry:  {config['display_name']}")
print(f"  App URL:   [printed after app starts]")
```

---

## Seed data

Create `industries/{industry}/seed/` directory for each industry with:
- `parts_inventory.json` — realistic part numbers, quantities, locations (see PARTS_DATA in `ot_pdm_app_layout.html` for exact data per industry)
- `maintenance_schedule.json` — 7 days of shift schedules with maintenance windows
- `asset_metadata.json` — auto-generated from config by RUNME.py

---

## Success criteria

- `databricks bundle validate` passes with no errors
- `databricks bundle deploy --target dev` completes in under 15 minutes on a clean workspace
- All five industry configs load without error via `load_config(industry)`
- All Unity Catalog tables created with correct schemas
- Lakebase seed data is queryable immediately after deploy
