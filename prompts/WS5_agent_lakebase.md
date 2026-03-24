# Workstream 5 — Lakebase Operational Store + Databricks Agent Framework

## Owner
Pravin Varma

## Depends on
WS1 (Lakebase schema, seed data), WS3 (pdm_predictions queryable), WS4 (models in registry)

## Deliverables
- `core/agent/tools.py` — Unity Catalog functions (agent tools)
- `core/agent/agent.py` — Databricks Agent Framework setup
- `core/agent/personas.py` — per-industry system prompts + terminology
- `core/agent/evaluate.py` — Mosaic AI Agent Evaluation
- `industries/{industry}/system_prompt.txt` × 5

---

## Context

The agent is the structured recommendation layer. It does NOT replace Genie — Genie handles open-ended data exploration. The agent handles: specific failure diagnosis → recommended action → time window → parts availability → cost of deferral. One-tap work order creation.

Each industry has its own system prompt that establishes operational context: shift schedules, regulatory obligations, cost units, parts depot locations, terminology.

---

## Task 5.1 — Unity Catalog Functions (agent tools)

All tools are registered as Unity Catalog functions. This makes them callable from the agent, from Genie, and from the app API.

```python
# core/agent/tools.py
"""
Register all agent tools as Unity Catalog functions.
Run once at deploy time via RUNME.py.
Each function reads from Lakebase or Gold tables.
"""
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

def register_all_tools(catalog: str, spark: SparkSession):
    """Register all UC functions for the agent tools schema."""
    tools = [
        _get_asset_sensor_history(catalog),
        _get_rul_prediction(catalog),
        _check_parts_inventory(catalog),
        _get_maintenance_schedule(catalog),
        _create_work_order(catalog),
        _estimate_production_impact(catalog),
    ]
    for sql in tools:
        spark.sql(sql)

def _get_asset_sensor_history(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.get_asset_sensor_history(
        equipment_id STRING COMMENT 'Asset identifier e.g. HT-012',
        sensor_name  STRING COMMENT 'Sensor tag name e.g. engine_egt',
        hours_back   INT    COMMENT 'How many hours of history to return'
    )
    RETURNS TABLE (timestamp TIMESTAMP, value DOUBLE, quality STRING)
    COMMENT 'Returns recent sensor history for a specific asset and sensor'
    RETURN
        SELECT timestamp, value, quality
        FROM {catalog}.bronze.sensor_readings
        WHERE equipment_id = get_asset_sensor_history.equipment_id
          AND tag_name = get_asset_sensor_history.sensor_name
          AND timestamp >= current_timestamp() - INTERVAL get_asset_sensor_history.hours_back HOURS
        ORDER BY timestamp DESC
        LIMIT 500
    """

def _get_rul_prediction(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.get_rul_prediction(
        equipment_id STRING COMMENT 'Asset identifier'
    )
    RETURNS TABLE (
        anomaly_score DOUBLE,
        anomaly_label STRING,
        rul_hours DOUBLE,
        predicted_failure_date TIMESTAMP,
        top_contributing_sensor STRING,
        scored_at TIMESTAMP
    )
    COMMENT 'Returns the latest RUL prediction and anomaly score for an asset'
    RETURN
        SELECT anomaly_score, anomaly_label, rul_hours,
               predicted_failure_date, top_contributing_sensor, _scored_at
        FROM {catalog}.gold.pdm_predictions
        WHERE equipment_id = get_rul_prediction.equipment_id
        ORDER BY _scored_at DESC
        LIMIT 1
    """

def _check_parts_inventory(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.check_parts_inventory(
        part_number        STRING COMMENT 'Part number to check',
        quantity_required  INT    COMMENT 'Quantity needed',
        depot              STRING COMMENT 'Depot/warehouse location'
    )
    RETURNS TABLE (
        part_number STRING, description STRING, quantity INT,
        location STRING, depot STRING, available BOOLEAN, lead_time_days INT
    )
    COMMENT 'Check if a part is in stock at the specified depot'
    RETURN
        SELECT part_number, description, quantity, location, depot,
               quantity >= check_parts_inventory.quantity_required AS available,
               lead_time_days
        FROM {catalog}.lakebase.parts_inventory
        WHERE (part_number = check_parts_inventory.part_number
               OR check_parts_inventory.part_number = '*')
          AND (depot = check_parts_inventory.depot
               OR check_parts_inventory.depot = '*')
    """

def _get_maintenance_schedule(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.get_maintenance_schedule(
        equipment_id   STRING COMMENT 'Asset identifier',
        lookahead_hours INT   COMMENT 'How many hours ahead to check'
    )
    RETURNS TABLE (
        shift_label STRING, shift_start TIMESTAMP, shift_end TIMESTAMP,
        maintenance_window_start TIMESTAMP, maintenance_window_end TIMESTAMP,
        crew_available BOOLEAN
    )
    COMMENT 'Returns upcoming maintenance windows for an asset'
    RETURN
        SELECT shift_label, shift_start, shift_end,
               maintenance_window_start, maintenance_window_end, crew_available
        FROM {catalog}.lakebase.maintenance_schedule
        WHERE equipment_id = get_maintenance_schedule.equipment_id
          AND shift_start >= current_timestamp()
          AND shift_start <= current_timestamp()
                + INTERVAL get_maintenance_schedule.lookahead_hours HOURS
        ORDER BY shift_start
    """

def _create_work_order(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.create_work_order(
        equipment_id    STRING  COMMENT 'Asset to raise work order for',
        failure_mode    STRING  COMMENT 'Diagnosed failure mode',
        priority        STRING  COMMENT 'critical | high | medium | low',
        scheduled_time  STRING  COMMENT 'ISO 8601 datetime string',
        parts_required  STRING  COMMENT 'Comma-separated part numbers'
    )
    RETURNS TABLE (work_order_id STRING, status STRING, created_at TIMESTAMP)
    COMMENT 'Create a maintenance work order and return the work order ID'
    RETURN
        WITH new_order AS (
            INSERT INTO {catalog}.lakebase.work_orders
                (work_order_id, equipment_id, failure_mode, priority,
                 status, scheduled_time, parts_required)
            VALUES (
                concat('WO-', date_format(current_timestamp(), 'yyyyMMdd-HHmmss')),
                create_work_order.equipment_id,
                create_work_order.failure_mode,
                create_work_order.priority,
                'submitted',
                to_timestamp(create_work_order.scheduled_time),
                split(create_work_order.parts_required, ',')
            )
        )
        SELECT work_order_id, status, created_at
        FROM {catalog}.lakebase.work_orders
        WHERE equipment_id = create_work_order.equipment_id
        ORDER BY created_at DESC
        LIMIT 1
    """

def _estimate_production_impact(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.estimate_production_impact(
        equipment_id          STRING COMMENT 'Asset identifier',
        downtime_hours        DOUBLE COMMENT 'Estimated downtime in hours',
        current_production_rate DOUBLE COMMENT 'Current production rate in production units/hour'
    )
    RETURNS TABLE (
        equipment_id STRING, downtime_hours DOUBLE,
        production_loss DOUBLE, cost_estimate DOUBLE, currency STRING
    )
    COMMENT 'Estimate production impact and cost of a maintenance event'
    RETURN
        SELECT
            estimate_production_impact.equipment_id,
            estimate_production_impact.downtime_hours,
            estimate_production_impact.downtime_hours
                * estimate_production_impact.current_production_rate AS production_loss,
            -- cost_per_production_unit from asset metadata (populated at seed time)
            estimate_production_impact.downtime_hours
                * estimate_production_impact.current_production_rate
                * COALESCE(
                    (SELECT cost_per_unit FROM {catalog}.bronze.asset_metadata
                     WHERE equipment_id = estimate_production_impact.equipment_id), 42000
                  ) AS cost_estimate,
            COALESCE(
                (SELECT cost_currency FROM {catalog}.bronze.asset_metadata
                 WHERE equipment_id = estimate_production_impact.equipment_id), 'AUD'
            ) AS currency
    """
```

---

## Task 5.2 — Industry system prompts (industries/{industry}/system_prompt.txt)

Create a full system prompt text file for each industry. Requirements:
- Establishes operational context (site name, timezone, shift schedule)
- Defines cost of downtime in concrete numbers
- Specifies parts depot location and lead time
- Lists regulatory obligations (e.g. EPA notification for water, MEL for aviation)
- Defines terminology (what to call the asset, what to call downtime, cost units)
- Instructs the agent to NEVER use ML jargon ("anomaly score", "isolation forest", "model inference") in responses
- Instructs the agent to always include: failure diagnosis, recommended action, time window, parts status, cost of deferral

Example structure for `industries/mining/system_prompt.txt`:
```
You are the Maintenance Supervisor AI for Rio Tinto Iron Ore operations at the
Gudai-Darri mine in the Pilbara, Western Australia.

OPERATIONAL CONTEXT:
...
```

Write full prompts for all five industries. See `EXEC_DATA` in `ot_pdm_app_layout.html` for the business language each industry uses — match it exactly.

---

## Task 5.3 — Agent setup (core/agent/agent.py)

```python
"""
Databricks Agent Framework setup using LangChain + MLflow tracing.
One agent instance per industry, configured from industry_config.yaml.
"""
from databricks import agents
from databricks.sdk import WorkspaceClient
import mlflow
from mlflow.models import ModelConfig
from core.config.loader import load_config, get_agent_config
import os

INDUSTRY = os.environ.get("INDUSTRY", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]
agent_conf = get_agent_config(config)

# Load system prompt from file
system_prompt_path = f"industries/{INDUSTRY}/system_prompt.txt"
with open(system_prompt_path) as f:
    system_prompt = f.read()

# Tool definitions — point to UC functions
TOOLS = [
    {
        "type": "unity_catalog_function",
        "function": {"name": f"{catalog}.agent_tools.get_asset_sensor_history"}
    },
    {
        "type": "unity_catalog_function",
        "function": {"name": f"{catalog}.agent_tools.get_rul_prediction"}
    },
    {
        "type": "unity_catalog_function",
        "function": {"name": f"{catalog}.agent_tools.check_parts_inventory"}
    },
    {
        "type": "unity_catalog_function",
        "function": {"name": f"{catalog}.agent_tools.get_maintenance_schedule"}
    },
    {
        "type": "unity_catalog_function",
        "function": {"name": f"{catalog}.agent_tools.create_work_order"}
    },
    {
        "type": "unity_catalog_function",
        "function": {"name": f"{catalog}.agent_tools.estimate_production_impact"}
    },
]

def build_agent():
    """Build and return the LangChain agent with UC function tools."""
    from langchain.agents import AgentExecutor
    from databricks_langchain import ChatDatabricks, UCFunctionToolkit

    llm = ChatDatabricks(
        endpoint="databricks-claude-3-5-sonnet",
        temperature=0.1,
        max_tokens=2048
    )

    toolkit = UCFunctionToolkit(
        function_names=[t["function"]["name"] for t in TOOLS],
        warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID")
    )

    agent = AgentExecutor.from_agent_and_tools(
        agent=llm.bind_tools(toolkit.get_tools()),
        tools=toolkit.get_tools(),
        system_message=system_prompt,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=8,
    )
    return agent

# MLflow logging of the agent
def log_agent(agent):
    with mlflow.start_run(run_name=f"ot_pdm_agent_{INDUSTRY}"):
        mlflow.langchain.log_model(
            agent,
            artifact_path="agent",
            registered_model_name=f"{catalog}.models.ot_pdm_agent_{INDUSTRY}",
        )
```

---

## Task 5.4 — Agent evaluation (core/agent/evaluate.py)

Use Mosaic AI Agent Evaluation. Create evaluation datasets per industry:

```python
"""
Evaluate agent quality using Mosaic AI Agent Evaluation.
Creates evaluation datasets from the expected Q&A pairs for each industry.
"""
import mlflow
from databricks.agents import evaluate

EVAL_DATASETS = {
    "mining": [
        {
            "request": "HT-007 is showing a vibration anomaly. What do you recommend?",
            "expected_response_contains": ["bearing", "shift", "Karratha", "SKF"],
            "expected_tool_calls": ["get_rul_prediction", "check_parts_inventory"]
        },
        {
            "request": "Is HT-012 safe to run the next shift?",
            "expected_response_contains": ["engine", "temperature", "hours", "not"],
            "expected_tool_calls": ["get_rul_prediction", "get_maintenance_schedule"]
        },
        {
            "request": "What is the cost if we defer HT-012 maintenance by 24 hours?",
            "expected_response_contains": ["$", "TOR", "production"],
            "expected_tool_calls": ["estimate_production_impact"]
        },
    ],
    # ... similar for energy, water, automotive, semiconductor
}

def run_evaluation(industry: str, agent_model_uri: str):
    eval_data = EVAL_DATASETS.get(industry, [])
    results = mlflow.evaluate(
        model=agent_model_uri,
        data=eval_data,
        model_type="databricks-agent",
        evaluators="databricks",
        evaluator_config={
            "databricks-agent": {
                "metrics": ["groundedness", "relevance", "safety"],
            }
        }
    )
    return results
```

---

## Task 5.5 — API endpoint for the app

The Databricks App calls the agent via a REST endpoint. Register the agent as a Model Serving endpoint:

```python
# Defined in databricks.yml as a served model endpoint
# endpoint name: ot-pdm-agent-{industry}
# model: {catalog}.models.ot_pdm_agent_{industry}/Production
# scale_to_zero: true
# workload_size: Small
```

The app calls it via:
```
POST /serving-endpoints/ot-pdm-agent-mining/invocations
{
  "messages": [{"role": "user", "content": "HT-007 vibration anomaly..."}],
  "context": {"equipment_id": "HT-007", "industry": "mining"}
}
```

---

## Success criteria

- All 6 UC functions created and queryable via Genie
- `create_work_order` function writes to `lakebase.work_orders` table
- Agent responds with industry-appropriate language (no ML jargon)
- Agent always calls at least `get_rul_prediction` for asset-specific questions
- Agent evaluation scores: groundedness > 0.8, relevance > 0.8, safety = 1.0
- System prompts tested against at least 3 evaluation questions per industry
