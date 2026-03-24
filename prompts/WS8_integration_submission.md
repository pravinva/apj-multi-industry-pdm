# Workstream 8 — Integration, Testing, DAB Packaging & Submission

## Owner
Pravin Varma (lead), all contributors for sign-off

## Depends on
WS1–WS7 all complete

## Deliverables
- `README.md` — full deployment guide
- `tests/integration/` — end-to-end integration tests
- Verified 15-minute clean deploy on FEVM
- Google Slides pitch deck (owned by Niels)
- Walkthrough video (under 10 minutes)
- Submission to buildathon Google Form

---

## Task 8.1 — README.md

The README is what judges read first. Must include:

```markdown
# OT PdM Intelligence

Config-driven predictive maintenance accelerator for heavy-asset industries.
Five industry skins. One DAB. Deploy in 15 minutes.

## Industries
| Industry | Anchor account | Pipeline |
...

## Architecture
[diagram]

## Quick start (15 minutes)
1. databricks bundle deploy --var industry=mining
2. Open RUNME.py and run all cells
3. Navigate to Databricks App URL

## Switching industries
databricks bundle deploy --var industry=energy
# Takes ~5 minutes — only DLT pipeline and model serving endpoint update

## Connector architecture
The unified-ot-zerobus-connector runs on edge hardware in the customer OT network.
For demo purposes, USE_SIMULATOR=true (default) generates physics-realistic sensor data
with the identical Bronze schema. See core/zerobus_ingest/connector.py for production setup.
Repository: https://github.com/pravinva/unified-ot-zerobus-connector

## The ten differentiators
1. First-mile connectivity (Zerobus)
2. Physics-realistic simulation with PLC scan timing
3. OPC-UA quality codes as first-class data
4. ISA-95 native hierarchy in Unity Catalog
5. Config-driven multi-industry extensibility
6. Dual per-asset ML: anomaly + RUL
7. Industry-grounded GenAI agent
8. Genie alongside the agent
9. Lakebase as CMMS-compatible operational store
10. Renesas–Nissan industrial value chain

## Regulatory applicability
- Australia: APRA CPS 234, WaterNSW audit requirements, EPA notification obligations
- Japan: 金融庁 (FSA) IoT security guidelines, SEMI E10/E58 equipment standards
- Singapore: MAS TRM Guidelines, PDPA data sovereignty requirements

## Roadmap
- Transport/Rail skin (Downer Group — 32K signals/second/train)
- Oil and Gas skin (PETRONAS — C3.AI displacement)
- Aviation skin (Malaysia Airlines — MEL-aware agent)
```

---

## Task 8.2 — Integration tests

```python
# tests/integration/test_full_stack.py
"""
End-to-end integration tests. Run against a live FEVM workspace.
Requires: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID environment variables.
"""
import pytest
import time
from core.config.loader import load_config

INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]

@pytest.mark.parametrize("industry", INDUSTRIES)
def test_config_loads(industry):
    config = load_config(industry)
    assert config["industry"] == industry
    assert len(config["simulator"]["assets"]) >= 4

@pytest.mark.parametrize("industry", INDUSTRIES)
def test_bronze_table_has_data(spark, industry):
    config = load_config(industry)
    count = spark.table(f"{config['catalog']}.bronze.sensor_readings").count()
    assert count > 0, f"Bronze table empty for {industry}"

@pytest.mark.parametrize("industry", INDUSTRIES)
def test_silver_quality_filtering(spark, industry):
    config = load_config(industry)
    bad = spark.table(f"{config['catalog']}.silver.sensor_features") \
               .filter("quality_code = '0x80'").count()
    assert bad == 0, f"Bad quality readings found in Silver for {industry}"

@pytest.mark.parametrize("industry", INDUSTRIES)
def test_gold_predictions_exist(spark, industry):
    config = load_config(industry)
    count = spark.table(f"{config['catalog']}.gold.pdm_predictions").count()
    assert count > 0, f"No predictions in Gold for {industry}"

@pytest.mark.parametrize("industry", INDUSTRIES)
def test_anomaly_scores_in_range(spark, industry):
    config = load_config(industry)
    out_of_range = spark.table(f"{config['catalog']}.gold.pdm_predictions") \
                        .filter("anomaly_score < 0 OR anomaly_score > 1").count()
    assert out_of_range == 0

@pytest.mark.parametrize("industry", INDUSTRIES)
def test_fault_assets_have_high_anomaly_score(spark, industry):
    """Assets with fault injection should have anomaly_score > 0.5."""
    config = load_config(industry)
    fault_assets = [a["id"] for a in config["simulator"]["assets"]
                    if a.get("inject_fault") and a.get("fault_severity", 0) > 0.7]
    for asset_id in fault_assets:
        rows = spark.table(f"{config['catalog']}.gold.pdm_predictions") \
                    .filter(f"equipment_id = '{asset_id}'") \
                    .orderBy("_scored_at", ascending=False) \
                    .limit(1).collect()
        assert rows, f"No prediction for {asset_id}"
        assert rows[0]["anomaly_score"] > 0.5, \
            f"{asset_id} anomaly_score={rows[0]['anomaly_score']:.2f}, expected > 0.5"

def test_lakebase_parts_inventory_populated(spark):
    """Parts inventory has seed data for all industries."""
    for industry in INDUSTRIES:
        config = load_config(industry)
        count = spark.table(f"{config['catalog']}.lakebase.parts_inventory").count()
        assert count > 0, f"Parts inventory empty for {industry}"

def test_uc_agent_tools_exist(spark):
    """All 6 UC agent tool functions exist in each catalog."""
    tools = ["get_asset_sensor_history", "get_rul_prediction", "check_parts_inventory",
             "get_maintenance_schedule", "create_work_order", "estimate_production_impact"]
    for industry in INDUSTRIES:
        config = load_config(industry)
        for tool in tools:
            result = spark.sql(f"DESCRIBE FUNCTION {config['catalog']}.agent_tools.{tool}").collect()
            assert result, f"UC function {tool} missing for {industry}"

def test_app_api_endpoints(httpx_client, base_url):
    """App API returns 200 for all key endpoints."""
    endpoints = ["/api/fleet/assets", "/api/fleet/kpis",
                 "/api/stream/latest", "/api/asset/HT-012/prediction"]
    for ep in endpoints:
        resp = httpx_client.get(base_url + ep)
        assert resp.status_code == 200, f"API {ep} returned {resp.status_code}"
        data = resp.json()
        assert data is not None

def test_15_minute_deploy(subprocess_run):
    """Full deploy completes in under 15 minutes."""
    import time
    start = time.time()
    result = subprocess_run(
        ["databricks", "bundle", "deploy", "--var", "industry=mining"],
        timeout=900
    )
    elapsed = time.time() - start
    assert result.returncode == 0, "Bundle deploy failed"
    assert elapsed < 900, f"Deploy took {elapsed:.0f}s, expected < 900s (15 minutes)"
```

---

## Task 8.3 — 15-minute deploy verification checklist

Run through this checklist on a **clean FEVM workspace** before submission:

```
[ ] 1. Create fresh FEVM workspace (no prior state)
[ ] 2. Clone repo: git clone <EMU repo URL>
[ ] 3. Run: databricks bundle deploy --var industry=mining
[ ] 4. Verify deploy completes in < 15 minutes
[ ] 5. Open RUNME.py, run all cells — no errors
[ ] 6. Start simulator: RUNME.py cell "Start simulator"
[ ] 7. Wait 60 seconds — verify Bronze table has rows
[ ] 8. Wait 5 minutes — verify Silver table has feature rows
[ ] 9. Trigger training job — verify MLflow experiments created
[ ] 10. Open Databricks App URL — verify Fleet page loads
[ ] 11. Switch to Executive view — verify cost figures populated
[ ] 12. Open agent chat — type "HT-007 vibration anomaly" — verify industry-grounded response
[ ] 13. Click "Check parts stock" on HT-007 — verify Lakebase data appears
[ ] 14. Open Genie tab — ask "Which assets are predicted to fail this week?" — verify results
[ ] 15. Switch industry tab to Energy — verify app re-renders with energy data
[ ] 16. Open Simulator tab — start simulator — verify Bronze stream updates
[ ] 17. Open Config Builder — select Semiconductor — verify YAML preview generates
[ ] 18. Repeat deploy for energy: databricks bundle deploy --var industry=energy
[ ] 19. Verify energy deploy completes in < 5 minutes (only pipeline + endpoint update)
[ ] 20. Document any failures with steps to reproduce
```

---

## Task 8.4 — Pitch deck (Google Slides, owned by Niels)

The pitch deck must cover:

**Slide 1: Title**
OT PdM Intelligence · APJ Industries Buildathon · Databricks Field Engineering

**Slide 2: The problem (ten gaps)**
Every existing PdM accelerator leaves ten gaps. This build closes all ten. (Use the PRD Section 2 framing.)

**Slide 3: The solution**
One DAB, five industries, one YAML swap. Architecture diagram from app.

**Slide 4: The business case (Niels)**
$2.7M/mo addressable pipeline. Named accounts at every level. Renesas wafer quality + Nissan CAN bus as anchor stories. The Renesas–Nissan value chain.

**Slide 5: Live demo (screenshots from app)**
Executive view, fleet page, agent chat, parts stock modal. Real data, not mockups.

**Slide 6: Technical differentiation**
Physics simulator + OPC-UA quality codes + ISA-95 + dual per-asset ML + Genie + Lakebase. None of this exists in any existing accelerator.

**Slide 7: Reusability proof**
"We added Water utilities in 30 minutes by writing one YAML file." Show the diff — the YAML change vs zero code changes.

**Slide 8: Extensibility roadmap**
Transport/Rail (Downer 32K signals/train), O&G (PETRONAS), Aviation (Malaysia Airlines). Show these as "future configs" using the same architecture.

**Slide 9: Team**
Pravin Varma (ANZ), Satoshi Kuramitsu (Japan), Niels Peter Lassen (Japan AE). Cross-region + cross-functional bonuses called out explicitly.

---

## Task 8.5 — Walkthrough video (under 10 minutes)

Structure:
- **0:00–1:30** — Niels: The problem. Why OT/IT convergence deals stall at POC. The $2.7M/mo pipeline.
- **1:30–3:00** — Pravin: Architecture walkthrough. The ten differentiators. One YAML = one industry.
- **3:00–5:30** — Pravin: Live demo. Fleet page → Executive view → agent chat → parts stock → sensor drilldown.
- **5:30–7:00** — Satoshi: Simulator tab. Fault injection → Bronze output → feature engineering → MLflow training.
- **7:00–8:30** — Pravin: Config Builder demo. Select Energy → 30 seconds → YAML generated. "This is how a SA configures a new industry without editing YAML."
- **8:30–10:00** — Niels: Business impact. Renesas wafer quality story. Nissan CAN bus story. Direct path to revenue.

Record as a screen recording with voiceover. No slides — live demo throughout.

---

## Submission checklist

```
[ ] GitHub EMU repo: databricks-field-eng/ANZ_OT-PdM-Intelligence
[ ] README.md complete with all required sections
[ ] All 5 industry configs present and validated
[ ] databricks bundle validate passes
[ ] 15-minute deploy verified on clean FEVM (by 20 Apr)
[ ] Deployed app URL working on FEVM
[ ] Google Slides pitch deck final (Niels, by 22 Apr)
[ ] Walkthrough video recorded and under 10 minutes (by 23 Apr)
[ ] Google Drive folder created, viewer access granted to Databricks
[ ] Submit via buildathon Google Form by 18:00 SGT 24 Apr 2026
```

---

## Dependency graph (build order)

```
WS1 (DAB + config + schema)
  ├── WS2 (Simulator + Bronze DLT)
  │     └── WS3 (Silver + Gold DLT)
  │           └── WS4 (ML models)
  │                 └── WS5 (Agent + Lakebase)  ← parallel with WS6
  │                       └── WS6 (App + Genie)
  └── WS7 (Semiconductor physics) ← parallel with WS2–WS4
        feeds into WS4 (semiconductor ML models)

WS8 (Integration + Testing + Submission) ← final, all complete
```

## Sprint allocation

| Workstream | Who | When |
|---|---|---|
| WS1 | Pravin | Days 1–2 |
| WS2 | Satoshi | Days 1–3 (parallel with WS1) |
| WS7 | Satoshi + Niels | Days 2–4 (parallel) |
| WS3 | Pravin | Days 3–5 |
| WS4 | Satoshi | Days 4–7 |
| WS5 | Pravin | Days 5–9 |
| WS6 | Pravin | Days 7–12 |
| WS8 | All | Days 14–19 |
