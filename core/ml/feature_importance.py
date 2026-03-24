import numpy as np
import pandas as pd
import shap


def compute_importance(anomaly_model, x: pd.DataFrame) -> pd.DataFrame:
    explainer = shap.TreeExplainer(anomaly_model.pipeline.named_steps["iso_forest"])
    x_scaled = anomaly_model.pipeline.named_steps["scaler"].transform(x)
    shap_values = explainer.shap_values(x_scaled)
    return (
        pd.DataFrame(
            {
                "feature_name": x.columns,
                "importance_score": np.abs(shap_values).mean(axis=0),
            }
        )
        .sort_values("importance_score", ascending=False)
        .reset_index(drop=True)
    )
