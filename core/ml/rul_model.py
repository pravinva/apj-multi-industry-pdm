import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class OTPdMRULModel:
    def __init__(self, equipment_id: str):
        self.equipment_id = equipment_id
        self.pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "gbr",
                    GradientBoostingRegressor(
                        n_estimators=300,
                        max_depth=4,
                        learning_rate=0.05,
                        subsample=0.8,
                        random_state=42,
                    ),
                ),
            ]
        )

    def fit(self, x: pd.DataFrame, y: pd.Series):
        self.pipeline.fit(x, y)
        self.feature_names_ = list(x.columns)
        return self

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        return self.pipeline.predict(x).clip(0)

    def evaluate(self, x: pd.DataFrame, y: pd.Series) -> dict:
        y_pred = self.predict(x)
        return {
            "rmse": float(np.sqrt(mean_squared_error(y, y_pred))),
            "r2": float(r2_score(y, y_pred)),
            "mae": float(np.abs(y - y_pred).mean()),
        }

    def log_to_mlflow(self, metrics: dict, catalog: str):
        import mlflow
        from mlflow import MlflowClient
        from mlflow.models import infer_signature
        import mlflow.sklearn

        # Use feature matrix columns seen during fit to create a stable signature.
        feature_names = getattr(self, "feature_names_", [])
        x_sample = pd.DataFrame([[0.0 for _ in feature_names]], columns=feature_names)
        y_sample = self.pipeline.predict(x_sample)
        signature = infer_signature(x_sample, y_sample)
        model_name = f"{catalog}.models.ot_pdm_rul_{self.equipment_id.lower()}"
        model_info = mlflow.sklearn.log_model(
            self.pipeline,
            artifact_path="rul_model",
            registered_model_name=model_name,
            signature=signature,
            input_example=x_sample,
        )
        # Keep the newest logged version as the active champion alias.
        try:
            if getattr(model_info, "registered_model_version", None):
                MlflowClient().set_registered_model_alias(
                    model_name, "champion", str(model_info.registered_model_version)
                )
        except Exception:
            # Alias setting is best-effort and should not fail model logging.
            pass
        mlflow.log_params(
            {
                "equipment_id": self.equipment_id,
                "n_estimators": 300,
                "feature_count": len(self.feature_names_),
            }
        )
        for k, v in metrics.items():
            mlflow.log_metric(k, v)


def generate_rul_labels(feature_df: pd.DataFrame, fault_config: dict) -> pd.Series:
    if not fault_config.get("inject_fault"):
        return pd.Series([9999.0] * len(feature_df), index=feature_df.index)

    severity = fault_config.get("fault_severity", 0.5)
    offset_h = abs(fault_config.get("fault_start_offset_hours", 0))
    estimated_life = 200 * (1 - severity)
    n = len(feature_df)
    window_hours = 0.25
    rul_start = max(0, estimated_life - offset_h)
    rul_end = max(0, rul_start - n * window_hours)
    return pd.Series(np.linspace(rul_start, rul_end, n), index=feature_df.index).clip(0)
