# OT PdM Intelligence

Config-driven predictive maintenance accelerator for heavy-asset industries.
Five industry skins. One Databricks Asset Bundle. Deploy in under 15 minutes.

## Industries

| Industry | Anchor account | Pipeline context |
|---|---|---|
| `mining` | Rio Tinto | Haul fleet and conveyor reliability |
| `energy` | Alinta | Wind + BESS + transformer availability |
| `water` | Sydney Water | Pumping and leak-risk operations |
| `automotive` | Toyota | Press/weld line stop-risk reduction |
| `semiconductor` | Renesas | Etch/litho fab yield protection |

## Architecture

```text
OT source (simulator or Zerobus)
  -> Bronze DLT (quality-aware sensor stream)
  -> Silver DLT (feature engineering)
  -> Gold DLT (feature vectors + predictions)
  -> MLflow models (anomaly + RUL per asset)
  -> Agent tools (UC functions on Lakebase + Gold)
  -> Databricks App (FastAPI + React)
```

- **Ingest**: OT simulator or Zerobus connector writes Bronze-compatible schema.
- **Transform**: DLT Bronze -> Silver -> Gold with config-driven features.
- **ML**: per-asset anomaly + RUL models registered in Unity Catalog.
- **Lakebase**: parts inventory, maintenance schedule, and work orders.
- **GenAI**: agent tools as UC functions and serving endpoint integration.
- **UI**: React + FastAPI app (`app/`) with API fallback behavior.

## App Design

- **Page 1 - Fleet**: starts with an executive value view (EBIT impact), then drills into fleet risk and operational actions.
- **Page 2 - Drilldown**: per-asset telemetry, anomaly and RUL context.
- **Page 3 - ISA-95**: hierarchy browsing and roll-up health.
- **Page 4 - Stream**: near-real-time row stream with quality/protocol flags.
- **Page 5 - Model**: model status and explainability views.
- **Page 6 - Simulator**: fault controls, configuration and connector setup.
- **Advanced PdM Command Layer** (on Model page): failure-mode ranking, prescriptive optimizer, spare-parts risk, MLOps health, and manual-aware guidance.
- **Manual Upload + Index**: upload PDF/MD/TXT manuals from UI, parse text, and persist chunked references in UC Delta (`bronze.manual_reference_chunks`) for retrieval/citations.

## Manual RAG Pattern (Demo)

Manual retrieval is wired as a lightweight RAG pattern:

- Ingestion: manuals from `industries/<skin>/manuals` and UI uploads
- Parsing: text extraction for `.md`, `.txt`, and `.pdf` (`pypdf`)
- Index: chunked rows persisted in UC Delta table `pdm_<industry>.bronze.manual_reference_chunks`
- Retrieval: token-overlap ranking against user prompt and asset context
- Grounding: top snippets injected into Genie prompt with source-aware instructions
- Citations: source list shown in chat bubbles (maintenance and finance panels)

For production-scale semantic retrieval, add vector embeddings + Databricks Vector Search on top of this Delta index.

## Executive Value Layer (Finance + ERP Simulation)

The executive view now includes a finance-first narrative for EBC use:

- Value statement: **Impact on EBIT saved through prescriptive maintenance**
- KPI cards: EBIT saved, ROI, payback days, EBIT margin lift (bps)
- Value bridge: avoided downtime + quality + energy, minus intervention and platform cost
- ERP context: plant code, fiscal period, planner group, cost centers
- Work-order simulation: recommended WOs with expected failure cost, intervention cost, and net EBIT impact

### Multi-skin sector-specific ERP and financial assumptions

Each skin has distinct ERP and finance assumptions (not shared defaults):

- `mining`: plant/cost centers, mining downtime economics, AUD
- `energy`: grid and storage work centers, outage economics, AUD
- `water`: distribution + quality operations economics, AUD
- `automotive`: line-stop and quality economics, JPY
- `semiconductor`: fab yield and tool downtime economics, USD

These are simulated in backend payload generation and surfaced in the executive UI for each industry skin.

## Quick Start (clean workspace)

1. Clone repository.
2. Validate bundle:
   - `databricks bundle validate --target dev -p DEFAULT`
3. Deploy:
   - `databricks bundle deploy --target dev -p DEFAULT`
4. Open `RUNME_BOOTSTRAP_ALL.py` in Databricks and run top-to-bottom.
5. Confirm data appears in Bronze/Silver/Gold/Finance tables.
6. Open app endpoint.

## Portable Workspace Bootstrap (single notebook)

Use `RUNME_BOOTSTRAP_ALL.py` as the one notebook for a fresh workspace deployment.

What it does in one run:
- Creates catalogs/schemas for all configured industry skins.
- Applies core DDL (`core/catalog/schema.sql`) and finance table setup.
- Seeds Lakebase and operational demo data from each `industries/*/seed` directory.
- Backfills 2 years of daily finance data into `finance.pm_financial_daily`.
- Grants read/use permissions for demo users.
- Optionally triggers initial runs of training/scoring/finance jobs.

Notebook widgets:
- `industries_csv` (default: all 5 skins)
- `history_days` (default: 730)
- `grant_principal` (default: `account users`)
- `trigger_jobs` (`true` / `false`)

For portability across workspaces, scheduled jobs now use job-cluster definitions (no hardcoded cluster IDs).

### Bootstrap as a Databricks Job

Bundle now includes `ot_pdm_workspace_bootstrap_job` which runs `RUNME_BOOTSTRAP_ALL.py` as a single task.

Run it after deploy:
- `databricks bundle run --target dev -p DEFAULT ot_pdm_workspace_bootstrap_job`

This provisions all tables/data/finance history and triggers initial training/scoring/backfill runs.

## Switching Industries

- `databricks bundle deploy --target dev -p DEFAULT --var industry=mining`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=energy`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=water`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=automotive`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=semiconductor`

## Connector Architecture

`core/zerobus_ingest/connector.py` wraps `unified-ot-zerobus-connector` for production OT networks.
For FE/demo workflows use `USE_SIMULATOR=true` to produce identical Bronze schema with synthetic but physics-shaped data.
Reference: [unified-ot-zerobus-connector](https://github.com/pravinva/unified-ot-zerobus-connector).

### Real Zerobus Ingest (all protocols)

The repo now supports a real first-mile connector flow with defaults for:

- OPC-UA
- MQTT JSON
- MQTT Sparkplug B
- Modbus TCP
- CANBUS

Defaults live in `core/zerobus_ingest/defaults.yaml` and can be overridden with `ZEROBUS_*` env vars.

#### Databricks (real ingestion path)

1. Set `USE_SIMULATOR=false` when deploying/running Bronze.
2. Run job `ot_pdm_zerobus_connector_job` (added in `databricks.yml`).
3. Connector writes into `{catalog}.bronze._zerobus_staging`.
4. Bronze DLT reads `_zerobus_staging` and materializes `{catalog}.bronze.sensor_readings`.

#### Local devloop (easy defaults)

- Start simulator + connector:
  - `python tools/zerobus_easy_start.py`
- Skip simulator and only run connector:
  - `python tools/zerobus_easy_start.py --no-simulator`

The default simulator endpoint/ports align with `ot_simulator` from
[unified-ot-zerobus-connector](https://github.com/pravinva/unified-ot-zerobus-connector/tree/main/ot_simulator).

#### Swinging Door Trending (SDT) in simulator

SDT compression is implemented in `core/simulator/sdt.py` and applied in
`core/simulator/engine.py` before writing to
`{catalog}.bronze._simulator_staging`. This is intentional so compression
happens at the edge/simulator side, not in the Zerobus ingest connector.

Per-industry controls are under `simulator.sdt` in `industries/*/config.yaml`:

- `enabled`: turn SDT on/off
- `epsilon_abs`: absolute SDT door width
- `epsilon_pct`: percentage-based SDT door width
- `heartbeat_ms`: force periodic keepalive points
- `tag_overrides`: per-tag SDT tuning map

Full runbook: `docs/zerobus-real-ingest.md`.

## Ten Differentiators

1. First-mile OT connectivity path (Zerobus)
2. Physics-oriented simulator with fault modes
3. OPC-UA quality codes as first-class fields
4. ISA-95 hierarchy mapped into Unity Catalog
5. Config-driven multi-industry deployment
6. Dual per-asset ML (anomaly + RUL)
7. Industry-grounded agent responses
8. Genie + agent dual interaction model
9. Lakebase operational data pattern
10. Cross-industry reuse with one codebase

## Local UI Validation

Run local validation before remote app deployment:

- Backend: `cd app && uvicorn server:app --reload --port 8000`
- Frontend: `cd app && npm install && npm run dev`
- Build check: `cd app && npm run build`

If npm install is blocked by corporate SSL chain issues, use the local network-approved npm registry configuration and rerun build verification.

## Integration Testing

- Unit/regression tests: `python3 -m pytest tests/`
- Integration scaffold: `tests/integration/test_full_stack.py`
