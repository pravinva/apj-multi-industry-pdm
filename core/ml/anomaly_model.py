import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class OTPdMAnomalyModel:
    def __init__(self, equipment_id: str, contamination: float = 0.05):
        self.equipment_id = equipment_id
        self.contamination = contamination
        self.pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "iso_forest",
                    IsolationForest(
                        n_estimators=200,
                        contamination=contamination,
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        )

    def fit(self, x: pd.DataFrame):
        self.pipeline.fit(x)
        self.feature_names_ = list(x.columns)
        return self

    def score(self, x: pd.DataFrame) -> np.ndarray:
        raw = self.pipeline.decision_function(x)
        inverted = -raw
        min_v, max_v = inverted.min(), inverted.max()
        if max_v == min_v:
            return np.zeros(len(x))
        return (inverted - min_v) / (max_v - min_v)

    def predict_label(self, x: pd.DataFrame) -> np.ndarray:
        return np.where(self.score(x) > 0.5, "anomaly", "normal")

    def log_to_mlflow(self, x_train: pd.DataFrame, metrics: dict, catalog: str):
        import mlflow
        from mlflow.models import infer_signature
        import mlflow.sklearn

        x_sample = x_train.head(min(len(x_train), 50))
        y_sample = self.pipeline.predict(x_sample)
        signature = infer_signature(x_sample, y_sample)
        mlflow.sklearn.log_model(
            self.pipeline,
            artifact_path="anomaly_model",
            registered_model_name=f"{catalog}.models.ot_pdm_anomaly_{self.equipment_id.lower()}",
            signature=signature,
            input_example=x_sample.head(5),
        )
        mlflow.log_params(
            {
                "equipment_id": self.equipment_id,
                "contamination": self.contamination,
                "n_train_samples": len(x_train),
                "feature_count": len(self.feature_names_),
            }
        )
        for k, v in metrics.items():
            mlflow.log_metric(k, v)
