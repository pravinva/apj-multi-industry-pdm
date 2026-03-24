"""
Databricks Agent Framework setup.
"""

import os

import mlflow
from databricks import agents
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain.agents import AgentExecutor

from core.agent.personas import load_system_prompt
from core.config.loader import get_agent_config, load_config

INDUSTRY = os.environ.get("INDUSTRY", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]
agent_conf = get_agent_config(config)
system_prompt = load_system_prompt(INDUSTRY)

TOOLS = [
    f"{catalog}.agent_tools.get_asset_sensor_history",
    f"{catalog}.agent_tools.get_rul_prediction",
    f"{catalog}.agent_tools.check_parts_inventory",
    f"{catalog}.agent_tools.get_maintenance_schedule",
    f"{catalog}.agent_tools.create_work_order",
    f"{catalog}.agent_tools.estimate_production_impact",
]


def build_agent():
    llm = ChatDatabricks(
        endpoint=os.environ.get("AGENT_LLM_ENDPOINT", "databricks-claude-sonnet-4-5"),
        temperature=0.1,
        max_tokens=2048,
    )
    toolkit = UCFunctionToolkit(
        function_names=TOOLS,
        warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID", "4b9b953939869799"),
    )
    return AgentExecutor.from_agent_and_tools(
        agent=llm.bind_tools(toolkit.get_tools()),
        tools=toolkit.get_tools(),
        system_message=system_prompt,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=8,
    )


def log_agent(agent):
    with mlflow.start_run(run_name=f"ot_pdm_agent_{INDUSTRY}"):
        mlflow.langchain.log_model(
            agent,
            artifact_path="agent",
            registered_model_name=f"{catalog}.models.ot_pdm_agent_{INDUSTRY}",
        )


def deploy_agent(agent):
    model_uri = f"models:/{catalog}.models.ot_pdm_agent_{INDUSTRY}/1"
    agents.deploy(
        model_name=f"{catalog}.models.ot_pdm_agent_{INDUSTRY}",
        model_version="1",
        endpoint_name=f"ot-pdm-agent-{INDUSTRY}",
    )
    return model_uri
