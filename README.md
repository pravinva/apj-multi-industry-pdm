# OT PdM Intelligence

Config-driven industrial maintenance application that combines predictive and prescriptive intelligence across multiple industry skins.

## What This App Is

This app is intentionally both:

- **Predictive maintenance**: estimates anomaly risk and remaining useful life (RUL) from telemetry.
- **Prescriptive maintenance**: recommends actions using risk predictions plus operational and financial context.

## Why Predictive and Prescriptive Both

Predictive outputs answer "what is likely to happen."
Prescriptive outputs answer "what should we do now."

Using only predictive signals often leaves an execution gap. Operations and finance still need:

- intervention timing,
- cost and savings view,
- expected downside if no action is taken,
- and actionability under constraints (crew, windows, parts, work orders).

The app closes this gap by computing recommendations and business impact on top of model outputs.

## Supported Industries

| Industry | Anchor account | Pipeline context |
|---|---|---|
| `mining` | Rio Tinto | Haul fleet and conveyor reliability |
| `energy` | Alinta | Wind, BESS, transformer availability |
| `water` | Sydney Water | Pumping and leak-risk operations |
| `automotive` | Toyota | Press and weld line stop-risk reduction |
| `semiconductor` | Renesas | Etch and lithography yield protection |

## End-to-End Flow

```text
OT source (simulator or Zerobus)
  -> Bronze landing tables
  -> Bronze DLT (sensor_readings, pi_tag_readings)
  -> Silver DLT (sensor_features, ot_pi_aligned)
  -> Gold DLT (feature_vectors, pdm_predictions, maintenance_alerts, financial_impact_events)
  -> ML training/scoring jobs (anomaly + RUL via MLflow)
  -> FastAPI service layer
  -> React UI (fleet, drilldown, model, finance, simulator)
```

## Architecture and Modules

### Core data and ML modules

- `core/dlt/bronze.py`
  - Normalizes landing streams and creates Bronze DLT tables.
- `core/dlt/silver.py`
  - Feature engineering and OT/PI alignment (`ot_pi_aligned`).
- `core/dlt/gold.py`
  - Prediction-ready vectors and business-facing outputs including `financial_impact_events`.
- `core/ml/train.py`
  - Per-industry model training workflow.
- `core/ml/batch_score.py`
  - Batch scoring pipeline using registered MLflow models.
- `core/ml/features.py`
  - Canonical feature read path and feature preparation logic.

### Ingestion and simulation modules

- `core/zerobus_ingest/connector.py`
  - First-mile OT connectivity integration.
- `core/simulator/engine.py`
  - Deterministic and fault-injected telemetry generation for demos.
- `core/simulator/sdt.py`
  - Swinging Door Trending compression logic at simulator stage.

### Application and user interface modules

- `app/server.py`
  - FastAPI backend, SQL orchestration, model and finance payload shaping, action APIs.
- `app/src/App.jsx`
  - React UI for fleet, model, simulator, and executive finance views.
- `app/src/globals.css`
  - Shared typography and layout styles.

### Deployment and bootstrap modules

- `databricks.yml`
  - Bundle resources (jobs, DLT, app deployment, variables).
- `RUNME_BOOTSTRAP_ALL.py`
  - One-run workspace bootstrap for all industries.
- `tools/bootstrap_all_industries.py`
  - Scripted bootstrap path.
- `industries/deployment_matrix.yaml`
  - Resource consistency matrix across industry skins.
- `tools/reconcile_industry_matrix.py`
  - Audit and reconcile resources against the matrix.

## Detailed Code Flow

1. **Telemetry landing**
   - Simulator and Zerobus connector both land into canonical Bronze schema.
2. **Bronze to Silver**
   - DLT validates and standardizes raw rows.
   - Silver computes features and creates alignment tables.
3. **PI plus OT alignment**
   - Silver table `ot_pi_aligned` links OT and PI streams using time proximity.
   - `data_source` provenance is carried to UI and downstream logic.
4. **Silver to Gold**
   - Gold materializes feature vectors, predictions, and maintenance alerts.
5. **Scoring and model lifecycle**
   - Training jobs register models in MLflow.
   - Scoring jobs read champion/current model aliases and write outputs.
6. **OT plus Finance integration**
   - Gold `financial_impact_events` combines prediction severity with planning and cost assumptions.
   - Backend exposes computed impacts for executive and prescriptive panels.
7. **App rendering**
   - FastAPI returns operational and executive payloads.
   - React renders model, maintenance, and finance decision views.

## Predictive and Prescriptive Features in UI

### Predictive

- Asset anomaly score and severity.
- RUL degradation views.
- Health rollups and alerting.
- Failure mode contextualization.

### Prescriptive

- Recommended intervention windows.
- Expected failure cost vs planned intervention cost.
- Expected avoided loss and action options.
- Work-order and financial decision framing.

## Currency and Localization

The app supports both currency adaptation and localization-aware presentation.

### Currency handling

- Supported display currencies include `USD`, `AUD`, and `JPY` (plus automatic mode).
- Backend computes native values and converts to selected display currency.
- Financial cards and executive statements are rendered in selected currency format.
- Industry assumptions are sector-specific (for example, automotive defaults to JPY, water and mining commonly to AUD, semiconductor often to USD).

### Localization behavior

- UI includes localized labels and presentation logic (including Japanese title path in header rendering).
- Backend keeps numeric and unit payloads normalized while frontend handles language-facing rendering.
- Currency, date window, and narrative statements are designed for executive readability across regional audiences.

## PI plus OT Integration

PI simulation is integrated alongside OT ingestion, not as a separate disconnected demo path.

- PI rows land to `bronze.pi_simulated_tags`.
- OT rows land to canonical OT Bronze source.
- Silver `ot_pi_aligned` performs time-aligned join and records provenance (`BOTH`, `OT_ONLY`, `UNKNOWN`).
- This provenance is used in downstream prescriptive context and model metadata displayed in the UI.

## OT plus Finance Integration

Prescriptive value is grounded in finance-aware outputs.

- Gold table `gold.financial_impact_events` merges prediction context with operational planning inputs.
- Output fields include:
  - `event_type`,
  - `maintenance_cost`,
  - `expected_failure_cost`,
  - `avoided_cost`,
  - `total_event_cost`,
  - and source/provenance metadata.
- Finance page and advanced maintenance sections consume these fields to provide recommendation economics, not just risk scores.

## App Pages

- **Fleet page**: executive value, portfolio risk, decision cards.
- **Drilldown page**: asset-level telemetry and risk context.
- **ISA-95 page**: hierarchy navigation and rolled health.
- **Factory map mode** (in ISA-95 page): textured physical-layout map with live status pins by industry, click-to-select asset context, and direct "Investigate in Genie" handoff.
- **Stream page**: recent ingest rows with quality and protocol context.
- **Model page**: model outputs and advanced maintenance panels.
- **Simulator page**: fault controls, non-blocking inject/score pipeline, "Force critical" demo action, and ingestion configuration.

## Alert Action Workflow and Lakebase Role

Alert rows in the Fleet view now include in-row actions:

- `Approve`
- `Reject`
- `Defer`

When an operator selects one of these actions:

1. The UI calls `POST /api/ui/recommendation/action`.
2. The backend writes a decision event to the configured Zerobus target for operational traceability.
3. The backend persists the durable decision record in Lakebase OLTP (`otpdm.operator_recommendation_actions` by default).
4. The overview refreshes and the row is marked as `Actioned`.

This is the role of Lakebase in the incident loop: it is the operational system-of-record for operator decisions, while Delta Gold predictions remain the analytical source for risk and alert generation.

## Genie Room Integration

- The app resolves per-industry Genie room IDs from `app/genie_rooms.json`.
- The "Maintenance Supervisor AI" header includes an "Open Genie room" deep link for the active industry.
- The UI also shows mapping coverage across industries ("Genie rooms configured: X/5").
- Hierarchy map/detail flows include "Investigate in Genie" for location-aware incident triage.

## Manual Retrieval Pattern

Manual retrieval is implemented as a lightweight grounded pattern:

- Ingestion from `industries/<skin>/manuals` and UI uploads.
- Text extraction for markdown, text, and PDF files.
- Chunk storage in `pdm_<industry>.bronze.manual_reference_chunks`.
- Prompt-time ranking by token overlap and context.
- Source snippets included in agent responses for traceability.

## Quick Start

1. Validate bundle:
   - `databricks bundle validate --target dev -p DEFAULT`
2. Deploy bundle:
   - `databricks bundle deploy --target dev -p DEFAULT`
3. Run bootstrap notebook:
   - `RUNME_BOOTSTRAP_ALL.py` in Databricks.
4. Verify data in Bronze, Silver, Gold, and finance tables.
5. Open app endpoint from Databricks Apps.

## Bootstrap Notes

`RUNME_BOOTSTRAP_ALL.py` provisions all configured industries in one run:

- catalog and schema creation,
- core DDL application (`core/catalog/schema.sql`),
- seed and planning data,
- finance history backfill,
- grants,
- optional trigger of training/scoring jobs.

Key widgets:

- `industries_csv`
- `history_days`
- `grant_principal`
- `trigger_jobs`
- `reset_existing`
- `seed_demo_planning_case`

## Industry Deploy and Reconcile

Deploy a specific industry:

- `databricks bundle deploy --target dev -p DEFAULT --var industry=mining`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=energy`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=water`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=automotive`
- `databricks bundle deploy --target dev -p DEFAULT --var industry=semiconductor`

Resource consistency checks:

- Dry run:
  - `python tools/reconcile_industry_matrix.py --owner <your-user>`
- Apply reconcile:
  - `python tools/reconcile_industry_matrix.py --owner <your-user> --apply`

## Local Development and Validation

- Backend:
  - `cd app && uvicorn server:app --reload --port 8000`
- Frontend:
  - `cd app && npm install && npm run dev`
- Build verification:
  - `cd app && npm run build`
- Tests:
  - `python3 -m pytest tests/`

## Summary

This project is a single multi-industry codebase for reliability intelligence that:

- predicts failures and degradation,
- prescribes maintenance decisions with business context,
- aligns PI and OT data streams,
- and translates model signals into finance-aware operational action.
