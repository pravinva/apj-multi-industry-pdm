"""
Mosaic AI agent evaluation wrapper.
"""

import mlflow

EVAL_DATASETS = {
    "mining": [
        {
            "request": "HT-007 is showing a vibration anomaly. What do you recommend?",
            "expected_response_contains": ["bearing", "shift", "Karratha", "SKF"],
            "expected_tool_calls": ["get_rul_prediction", "check_parts_inventory"],
        },
        {
            "request": "Is HT-012 safe to run the next shift?",
            "expected_response_contains": ["engine", "temperature", "hours", "not"],
            "expected_tool_calls": ["get_rul_prediction", "get_maintenance_schedule"],
        },
        {
            "request": "What is the cost if we defer HT-012 maintenance by 24 hours?",
            "expected_response_contains": ["AUD", "production"],
            "expected_tool_calls": ["estimate_production_impact"],
        },
    ]
}


def run_evaluation(industry: str, agent_model_uri: str):
    data = EVAL_DATASETS.get(industry, [])
    if not data:
        return {"status": "no_dataset"}
    return mlflow.evaluate(
        model=agent_model_uri,
        data=data,
        model_type="databricks-agent",
        evaluators="databricks",
        evaluator_config={"databricks-agent": {"metrics": ["groundedness", "relevance", "safety"]}},
    )
