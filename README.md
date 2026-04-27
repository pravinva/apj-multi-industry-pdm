# OT PdM Intelligence

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-1f75ff?style=for-the-badge)](#technology-stack)
[![License: MIT](https://img.shields.io/badge/License-MIT-ff3621?style=for-the-badge)](./LICENSE)
[![Tests: Pytest](https://img.shields.io/badge/Tests-Pytest-14213d?style=for-the-badge)](#validation-and-tests)

Config-driven predictive and prescriptive maintenance application built on Databricks (DABs + Jobs + DLT + MLflow + Apps) with a single codebase that supports multiple industry deployments.

## Table of Contents

- [Platform Overview](#platform-overview)
- [Business and Technical Functionality](#business-and-technical-functionality)
- [System Architecture](#system-architecture)
- [End-to-End Code Flow](#end-to-end-code-flow)
- [Repository Sections](#repository-sections)
- [Data Layers and Key Tables](#data-layers-and-key-tables)
- [API and UI Sections](#api-and-ui-sections)
- [Quick Start (15 Minutes)](#quick-start-15-minutes)
- [Deployment Modes](#deployment-modes)
- [Simulator and Fault Injection](#simulator-and-fault-injection)
- [Localization and Currency](#localization-and-currency)
- [Genie and Retrieval](#genie-and-retrieval)
- [Validation and Tests](#validation-and-tests)
- [Technology Stack](#technology-stack)
- [License](#license)

## Platform Overview

This application combines:

- predictive maintenance (anomaly + remaining useful life),
- prescriptive maintenance (recommended action pathways),
- financial impact context (avoided loss, intervention cost, risk exposure),
- governed AI operations (Unity Catalog + lineage + model lifecycle traceability).

It is designed to support executive, operator, and technical workflows from one integrated solution.

## Business and Technical Functionality

### Predictive capabilities

- Asset-level anomaly and severity scoring
- Remaining useful life trends
- Fleet health rollups and critical risk visibility
- Failure mode contextualization

### Prescriptive capabilities

- Intervention timing recommendations
- Action options (approve, reject, defer)
- Financial consequence framing per action path
- Decision trace persistence for governance and operations

### Operational views

- Fleet overview and risk matrix
- Asset drilldown and hierarchy context
- Stream health and ingestion observability
- Simulator-driven scenario rehearsal

## System Architecture

```text
Source systems (Simulator / Zerobus / PI / ERP context)
  -> Bronze Delta landing
  -> DLT Bronze normalization
  -> DLT Silver feature engineering + OT/PI alignment
  -> DLT Gold predictions + maintenance + finance outputs
  -> MLflow training and scoring jobs
  -> FastAPI service layer
  -> React application (Fleet, Finance, Geo, Model, Simulator, Data Hub)
```

## End-to-End Code Flow

1. **Ingest**
   - `core/zerobus_ingest/connector.py` and simulator feeds land telemetry into Bronze.
2. **Normalize and align**
   - `core/dlt/bronze.py` standardizes incoming records.
   - `core/dlt/silver.py` computes features and OT/PI alignment.
3. **Materialize business outputs**
   - `core/dlt/gold.py` publishes predictions, alerts, and finance-aware outputs.
4. **Model lifecycle**
   - `core/ml/train.py` trains and registers models.
   - `core/ml/batch_score.py` scores using registered aliases.
5. **Serve to application**
   - `app/server.py` assembles operational and executive payloads.
6. **Render and action**
   - `app/src/App.jsx` and component modules render sections and trigger actions.
7. **Persist operator decisions**
   - Action APIs write durable decision traces and refresh portfolio-level summaries.

## Repository Sections

| Section | Path | Purpose |
|---|---|---|
| App backend | `app/server.py` | API orchestration, SQL queries, simulator hooks, response shaping |
| App frontend | `app/src/` | Fleet, Finance, Geo, Data Hub, Simulator, Model UX |
| DLT pipelines | `core/dlt/` | Bronze/Silver/Gold transformations |
| ML modules | `core/ml/` | Feature prep, training, scoring, model logic |
| Simulator | `core/simulator/` | Telemetry and fault generation |
| Bundle resources | `databricks.yml` | Jobs, pipelines, app, variables, schedules |
| Bootstrap orchestration | `RUNME_BOOTSTRAP_ALL.py` | Multi-industry workspace setup and seed |
| Automation scripts | `tools/` | Deploy, bootstrap, reconcile, seed utilities |
| Industry configs | `industries/` | Per-industry prompts, seeds, deployment matrix |
| Test suite | `tests/` | API, ML, simulator, pipeline and integration tests |

## Data Layers and Key Tables

### Bronze

- Raw and normalized ingestion tables
- PI and OT landing paths
- Manual retrieval chunk storage

### Silver

- `sensor_features`
- `ot_pi_aligned`

### Gold

- `feature_vectors`
- `pdm_predictions`
- `maintenance_alerts`
- `financial_impact_events`

## API and UI Sections

### Backend APIs

- Fleet and executive summary APIs
- Geo site and asset APIs with currency-aware financial payloads
- Simulator control APIs for scenario injection
- Recommendation action APIs for approve/reject/defer workflow
- Data discovery and concierge APIs
- Agent/Genie chat APIs

### UI sections

- Fleet Health
- Finance
- Stoppage
- Data Discovery Hub
- Geo Map
- Hierarchy (ISA-95)
- Asset and Stream
- Model
- Simulator

## Quick Start (15 Minutes)

### Prerequisites

- Databricks CLI authenticated to target workspace
- Python 3.10+
- Node.js 18+ (for frontend build/update paths)

### Recommended path

1. Clone repository.
2. Run quickstart:
   - `./RUNME_15_MIN.sh --target dev`
3. Verify app and data:
   - App is running in Databricks Apps.
   - Bronze/Silver/Gold tables are populated.
   - Finance and simulator pages show non-empty outputs.

## Deployment Modes

### Quickstart mode

- `./RUNME_15_MIN.sh --target dev`
- Fast path to provision shared resources and bootstrap all industries.

### Full mode

- `python tools/deploy_bundle_and_bootstrap.py --mode full --target dev --industries mining,energy,water,automotive,semiconductor`
- Deploys all configured industry variants and runs full bootstrap sequence.

## Simulator and Fault Injection

- Simulator can inject warning/critical scenarios for demonstration and validation.
- Bulk injection supports all industries and table paths (bronze/silver/gold visibility).
- Relevant endpoints and controls are wired through `app/server.py` and simulator UI actions.

## Localization and Currency

- Supported display currencies include `USD`, `AUD`, `JPY`, `INR`, `SGD`, `KRW`, plus `AUTO`.
- Geo and financial sections are currency-aware and site-context-aware.
- Language behavior can localize prompts and guidance for selected currency contexts.

## Genie and Retrieval

- Industry Genie room routing is configured in `app/genie_rooms.json`.
- Retrieval ingestion supports markdown/text/PDF from industry manuals and uploads.
- Chunked references are stored and ranked for grounded responses with source traceability.

## Validation and Tests

Run core checks:

- `python3 -m pytest tests/`
- `cd app && npm run build`

Representative coverage includes:

- API behavior (`tests/test_app_api.py`)
- simulator behavior (`tests/test_simulator.py`)
- DLT/feature paths (`tests/test_bronze.py`, `tests/test_features.py`)
- ML pipeline logic (`tests/test_ml.py`)
- integration workflow (`tests/integration/test_full_stack.py`)

## Technology Stack

- Python 3.10+
- FastAPI
- React
- Databricks Apps
- Databricks Asset Bundles (DABs)
- Delta Live Tables
- MLflow
- Unity Catalog
- Pytest

## License

This project is licensed under the MIT License. See [`LICENSE`](./LICENSE).
