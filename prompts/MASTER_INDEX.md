# OT PdM Intelligence — Implementation Workstreams
## Master Index

**Project:** APJ Industries Buildathon — `databricks-field-eng/ANZ_OT-PdM-Intelligence`
**Team:** Pravin Varma (ANZ) · Satoshi Kuramitsu (Japan) · Niels Peter Lassen (Japan AE)
**Sprint:** 6 Apr – 24 Apr 2026 · Submission: 24 Apr 18:00 SGT

---

## How to use these workstreams

Each workstream is a self-contained implementation prompt. Feed it directly to Cursor or Claude Code. The prompt includes:
- The files to create and their exact paths
- Complete code skeletons with all class/function signatures
- Expected schemas, SQL DDL, and config structures
- Success criteria to verify the workstream is complete

Start each prompt with:
> "Read [WS file] and implement all tasks. Use the exact file paths and class names specified. All backend data must be real — no static, mock, or hardcoded return values except where explicitly noted as seed data."

---

## Workstreams

| # | Workstream | Owner | Input | Key output | Days |
|---|---|---|---|---|---|
| WS1 | DAB scaffold, Unity Catalog schema, config loader | Pravin | — | `databricks.yml`, 5× `config.yaml`, `loader.py`, `schema.sql`, `RUNME.py` | 1–2 |
| WS2 | OT simulator + Zerobus Bronze DLT | Satoshi | WS1 | `engine.py`, `physics.py`, `fault_injection.py`, `bronze.py` | 1–3 |
| WS3 | Silver + Gold DLT feature engineering | Pravin | WS1, WS2 | `silver.py`, `gold.py`, `engineering.py` | 3–5 |
| WS4 | MLflow anomaly + RUL models | Satoshi | WS1, WS3 | `train.py`, `anomaly_model.py`, `rul_model.py`, `batch_score.py` | 4–7 |
| WS5 | Lakebase + Agent Framework + UC functions | Pravin | WS1, WS3, WS4 | `tools.py`, `agent.py`, `evaluate.py`, 5× `system_prompt.txt` | 5–9 |
| WS6 | Genie spaces + Databricks App (React) | Pravin | WS1, WS3, WS5 | `app/`, `server.py`, `genie/setup.py`, 5× `genie_questions.json` | 7–12 |
| WS7 | Semiconductor physics + skin completion | Satoshi + Niels | WS2 | `semiconductor_physics.py`, semiconductor config/seed validated by Niels | 2–4 |
| WS8 | Integration, testing, packaging, submission | All | WS1–WS7 | `README.md`, integration tests, 15-min deploy verified, pitch deck, video | 14–19 |

---

## Repository structure

```
ANZ_OT-PdM-Intelligence/
├── databricks.yml                      # WS1 — DAB root
├── RUNME.py                            # WS1 — deployment guide
├── README.md                           # WS8
│
├── industries/
│   ├── mining/
│   │   ├── config.yaml                 # WS1
│   │   ├── system_prompt.txt           # WS5
│   │   ├── genie_questions.json        # WS6
│   │   └── seed/
│   │       ├── parts_inventory.json    # WS1
│   │       └── maintenance_schedule.json
│   ├── energy/  (same structure)
│   ├── water/   (same structure)
│   ├── automotive/ (same structure)
│   └── semiconductor/
│       ├── config.yaml                 # WS1 + WS7 (Niels validates)
│       ├── system_prompt.txt           # WS5 + WS7 (Niels validates)
│       ├── genie_questions.json        # WS7
│       └── seed/
│           └── parts_inventory.json    # WS7
│
├── core/
│   ├── config/
│   │   └── loader.py                   # WS1
│   ├── catalog/
│   │   └── schema.sql                  # WS1
│   ├── simulator/
│   │   ├── engine.py                   # WS2
│   │   ├── physics.py                  # WS2
│   │   ├── fault_injection.py          # WS2
│   │   └── semiconductor_physics.py    # WS7
│   ├── dlt/
│   │   ├── bronze.py                   # WS2
│   │   ├── silver.py                   # WS3
│   │   └── gold.py                     # WS3
│   ├── features/
│   │   └── engineering.py              # WS3
│   ├── ml/
│   │   ├── train.py                    # WS4
│   │   ├── anomaly_model.py            # WS4
│   │   ├── rul_model.py                # WS4
│   │   ├── feature_importance.py       # WS4
│   │   ├── batch_score.py              # WS4
│   │   └── evaluate.py                 # WS4
│   ├── agent/
│   │   ├── tools.py                    # WS5
│   │   ├── agent.py                    # WS5
│   │   ├── personas.py                 # WS5
│   │   └── evaluate.py                 # WS5
│   ├── genie/
│   │   └── setup.py                    # WS6
│   └── zerobus_ingest/
│       └── connector.py                # WS2
│
├── app/
│   ├── app.yaml                        # WS6
│   ├── server.py                       # WS6
│   ├── package.json
│   └── src/
│       ├── main.jsx                    # WS6
│       ├── App.jsx                     # WS6
│       ├── api/                        # WS6
│       ├── components/                 # WS6
│       ├── hooks/                      # WS6
│       └── styles/
│           └── globals.css             # WS6 (from ot_pdm_app_layout.html)
│
└── tests/
    ├── test_simulator.py               # WS2
    ├── test_bronze.py                  # WS2
    └── integration/
        └── test_full_stack.py          # WS8
```

---

## Critical interfaces between workstreams

These are the contracts that workstreams depend on. Do not change them without coordinating across teams.

### Bronze table schema (WS2 → WS3, WS4, WS6)
```
site_id, area_id, unit_id, equipment_id, component_id,
tag_name, value (DOUBLE), unit, quality, quality_code,
source_protocol, timestamp, _ingested_at
```
Quality codes: `"good"/"0x00"`, `"uncertain"/"0x40"`, `"bad"/"0x80"`

### Config loader contract (WS1 → all)
`load_config(industry)` returns a dict with keys: `industry`, `catalog`, `isa95_hierarchy`, `simulator`, `sensors`, `failure_modes`, `features`, `agent`, `dashboard`

### Gold feature vector schema (WS3 → WS4, WS6)
Columns: `equipment_id`, `window_start`, `window_end`, `{sensor}__{feature}` × N, `_processed_at`

### Agent endpoint contract (WS5 → WS6)
```
POST /serving-endpoints/ot-pdm-agent-{industry}/invocations
Body: {"messages": [...], "context": {"equipment_id": "...", "industry": "..."}}
Response: {"choices": [{"message": {"content": "..."}}]}
```

### Lakebase parts_inventory schema (WS1 → WS5, WS6)
```
part_number, description, quantity, location, depot,
unit_cost, currency, reorder_point, lead_time_days, last_updated
```

---

## Environment variables required

```bash
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<PAT token>
DATABRICKS_WAREHOUSE_ID=<SQL warehouse ID>
INDUSTRY=mining                    # or energy, water, automotive, semiconductor
USE_SIMULATOR=true                 # false only when Zerobus connector is on OT network
SIMULATOR_JOB_ID=<job ID>          # set after WS1 deploy
```

---

## Cursor / Claude Code usage notes

1. **Feed one workstream at a time.** Each workstream is designed to be implementable independently. Do not feed multiple workstreams simultaneously.

2. **Reference the HTML for UI fidelity.** `ot_pdm_app_layout.html` is the reference design for WS6. The React components must replicate it exactly. Paste the CSS from the HTML into `app/src/styles/globals.css` verbatim.

3. **No mock data in production code paths.** The only acceptable static data is seed JSON files (`parts_inventory.json`, `maintenance_schedule.json`) which are loaded once at deploy time. All API endpoints must query live Delta tables.

4. **Semiconductor physics are genuinely novel.** WS7 `semiconductor_physics.py` requires implementing contamination event physics (Poisson trigger + exponential growth) and thermal coupling (sinusoidal + linear drift). These are NOT standard degradation curves. The full physics equations are in the workstream.

5. **Config-driven means zero hardcoded industry strings.** No `if industry == 'mining':` in pipeline or ML code. All industry-specific behaviour must be resolved through `load_config(industry)`.
