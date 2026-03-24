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

- **Page 1 - Fleet**: KPI strip, fleet risk, operational actions.
- **Page 2 - Drilldown**: per-asset telemetry, anomaly and RUL context.
- **Page 3 - ISA-95**: hierarchy browsing and roll-up health.
- **Page 4 - Stream**: near-real-time row stream with quality/protocol flags.
- **Page 5 - Model**: model status and explainability views.
- **Page 6 - Simulator**: fault controls, configuration and connector setup.

## Quick Start (clean workspace)

1. Clone repository.
2. Validate bundle:
   - `databricks bundle validate --target dev -p DEFAULT`
3. Deploy:
   - `databricks bundle deploy --target dev -p DEFAULT`
4. Open `RUNME.py` in Databricks and run top-to-bottom.
5. Confirm data appears in Bronze/Silver/Gold tables.
6. Open app endpoint.

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
