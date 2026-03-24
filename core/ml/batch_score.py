"""
Batch scoring job for all configured assets.
"""

import os

import mlflow
import pandas as pd

from core.config.loader import load_config
from core.ml.anomaly_model import OTPdMAnomalyModel
from core.ml.features import get_feature_matrix

INDUSTRY = os.environ.get("INDUSTRY", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]


def score_all_assets(spark):
    results = []
    for asset in config["simulator"]["assets"]:
        eid = asset["id"]
        try:
            anomaly_pipeline = mlflow.sklearn.load_model(
                f"models:/{catalog}.models.ot_pdm_anomaly_{eid.lower()}/Production"
            )
            rul_model = mlflow.sklearn.load_model(
                f"models:/{catalog}.models.ot_pdm_rul_{eid.lower()}/Production"
            )
        except Exception:
            continue

        x, _ = get_feature_matrix(spark, catalog, eid, n_hours=2)
        if x.empty:
            continue

        model = OTPdMAnomalyModel(eid)
        model.pipeline = anomaly_pipeline
        scores = model.score(x)
        ruls = rul_model.predict(x)
        i = -1
        results.append(
            {
                "equipment_id": eid,
                "prediction_timestamp": pd.Timestamp.utcnow(),
                "anomaly_score": float(scores[i]),
                "anomaly_label": "anomaly" if scores[i] > 0.5 else "normal",
                "rul_hours": float(ruls[i]),
                "predicted_failure_date": (
                    pd.Timestamp.utcnow() + pd.Timedelta(hours=float(ruls[i]))
                )
                if ruls[i] < 9000
                else None,
            }
        )

    if results:
        spark.createDataFrame(pd.DataFrame(results)).write.format("delta").mode("append").saveAsTable(
            f"{catalog}.gold.pdm_predictions"
        )


if __name__ == "__main__":
    score_all_assets(spark)  # noqa: F821
