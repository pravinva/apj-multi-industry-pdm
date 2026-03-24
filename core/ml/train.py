"""
Per-asset training for anomaly + RUL models.
"""

import os

import mlflow
import pandas as pd

from core.config.loader import load_config
from core.ml.anomaly_model import OTPdMAnomalyModel
from core.ml.features import get_feature_matrix
from core.ml.rul_model import OTPdMRULModel, generate_rul_labels

INDUSTRY = os.environ.get("INDUSTRY", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/ot-pdm-intelligence/{INDUSTRY}")


def train_asset_models(equipment_id: str, spark):
    x, _ = get_feature_matrix(spark, catalog, equipment_id, n_hours=720)
    if x.empty or len(x) < 50:
        return {"equipment_id": equipment_id, "status": "skipped", "reason": "insufficient_data"}

    asset_conf = next((a for a in config["simulator"]["assets"] if a["id"] == equipment_id), {})
    fault_offset = abs(asset_conf.get("fault_start_offset_hours", 0))
    n_healthy = max(20, len(x) - int(fault_offset / 0.25))
    x_healthy = x.iloc[:n_healthy]

    with mlflow.start_run(run_name=f"{equipment_id}_anomaly"):
        anomaly_model = OTPdMAnomalyModel(equipment_id)
        anomaly_model.fit(x_healthy)
        scores = anomaly_model.score(x)
        anomaly_model.log_to_mlflow(
            x_healthy,
            {
                "mean_score_healthy": float(scores[:n_healthy].mean()),
                "mean_score_fault": float(scores[n_healthy:].mean()) if len(scores) > n_healthy else 0.0,
            },
        )

    with mlflow.start_run(run_name=f"{equipment_id}_rul"):
        y = generate_rul_labels(x, asset_conf)
        rul_model = OTPdMRULModel(equipment_id)
        rul_model.fit(x, y)
        metrics = rul_model.evaluate(x, y)
        rul_model.log_to_mlflow(metrics, catalog)

    return {"equipment_id": equipment_id, "status": "trained", **metrics}


def train_all_assets(spark):
    equipment_ids = [a["id"] for a in config["simulator"]["assets"]]
    rows = []
    for eid in equipment_ids:
        rows.append(train_asset_models(eid, spark))
    return pd.DataFrame(rows)


if __name__ == "__main__":
    results = train_all_assets(spark)  # noqa: F821
    print(results)
