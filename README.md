# OT PdM Intelligence

Config-driven predictive maintenance accelerator for heavy-asset industries.  
One codebase, five industry skins, unified Databricks deployment.

## Industries

- `mining` - haul trucks and conveyors
- `energy` - wind turbines, BESS, transformers
- `water` - pumps, metering, treatment
- `automotive` - stamping, welding, CNC, conveyors
- `semiconductor` - etch, lithography, CMP, metrology

## Architecture

- **Ingest**: OT simulator or Zerobus connector writes to Bronze.
- **Transform**: DLT Bronze -> Silver -> Gold with config-driven features.
- **ML**: per-asset anomaly + RUL models in Unity Catalog model registry.
- **Operational store**: Lakebase schemas for parts, schedules, and work orders.
- **AI layer**: Agent tools as Unity Catalog functions + model serving endpoint.
- **UI**: React + FastAPI app (`app/`) reading APIs backed by Delta tables.

## App Design

- **Page 1 - Fleet**: operator and executive views, KPIs, alerts, recommendations.
- **Page 2 - Drilldown**: per-asset sensor telemetry, anomaly timeline, health cards.
- **Page 3 - ISA-95**: hierarchy navigation with roll-up health.
- **Page 4 - Stream**: live sensor stream with quality/protocol badges.
- **Page 5 - Model**: RUL chart, feature importance, anomaly decomposition.
- **Page 6 - Simulator**: fault injection controls, config builder, connector setup.

## Quick Start

1. Configure Databricks auth for FE workspace.
2. Deploy bundle:
   - `databricks bundle validate`
   - `databricks bundle deploy --target dev`
3. Run `RUNME.py` in a Databricks notebook context.
4. Open the Databricks App URL.

## Local UI First

When `app/` workstream is implemented, run local UI validation before remote deploy:

- Backend: `uvicorn server:app --reload`
- Frontend: `npm install && npm run dev`
- Verify all six pages and API fallbacks locally, then deploy.
