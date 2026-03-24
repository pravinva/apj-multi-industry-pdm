# Workstream 4 — MLflow Anomaly Detection & RUL Models

## Owner
Satoshi Kuramitsu

## Depends on
WS1 (catalog schema), WS3 (Gold feature vectors table)

## Deliverables
- `core/ml/train.py` — DAXS-style per-asset model training (Isolation Forest + RUL)
- `core/ml/rul_model.py` — RUL regression model class
- `core/ml/anomaly_model.py` — Isolation Forest wrapper
- `core/ml/feature_importance.py` — SHAP-based feature contribution
- `core/ml/batch_score.py` — batch scoring job (runs on schedule)
- `core/ml/evaluate.py` — model evaluation and promotion logic

---

## Context

Each asset gets TWO models trained independently:
1. **Anomaly detection** — Isolation Forest, unsupervised, trained on healthy baseline data. Detects deviation from normal operating envelope.
2. **RUL regression** — Supervised regression trained on simulator fault injection data with known RUL labels. Predicts hours to failure.

Use the **DAXS pattern**: Pandas UDFs for parallel training across all assets simultaneously. Each asset's model is registered independently in MLflow Unity Catalog.

---

## Task 4.1 — Feature vector preparation

```python
# core/ml/features.py
def get_feature_matrix(spark, catalog: str, equipment_id: str,
                        n_hours: int = 720) -> tuple[pd.DataFrame, list[str]]:
    """
    Load feature vectors for one asset from Gold table.
    Returns (X, feature_names) where X is a numpy-ready DataFrame.
    Excludes non-numeric and metadata columns.
    """
    df = spark.table(f"{catalog}.gold.feature_vectors") \
              .filter(F.col("equipment_id") == equipment_id) \
              .filter(F.col("window_start") >= F.date_sub(F.current_timestamp(), n_hours // 24)) \
              .orderBy("window_start") \
              .toPandas()

    exclude = {"equipment_id", "window_start", "window_end", "_processed_at"}
    feature_cols = [c for c in df.columns if c not in exclude]
    X = df[feature_cols].fillna(0.0)
    return X, feature_cols
```

---

## Task 4.2 — Anomaly model (core/ml/anomaly_model.py)

```python
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import mlflow
import mlflow.sklearn

class OTPdMAnomalyModel:
    """
    Isolation Forest anomaly detector for a single OT asset.
    Trained on healthy-baseline data only (quality=good, no fault injection period).
    """
    def __init__(self, equipment_id: str, contamination: float = 0.05):
        self.equipment_id = equipment_id
        self.contamination = contamination
        self.pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("iso_forest", IsolationForest(
                n_estimators=200,
                contamination=contamination,
                random_state=42,
                n_jobs=-1
            ))
        ])

    def fit(self, X: pd.DataFrame) -> "OTPdMAnomalyModel":
        self.pipeline.fit(X)
        self.feature_names_ = list(X.columns)
        return self

    def score(self, X: pd.DataFrame) -> np.ndarray:
        """Returns normalised anomaly score in [0, 1]. 1 = most anomalous."""
        raw = self.pipeline.decision_function(X)
        # Isolation Forest: lower = more anomalous. Invert and normalise.
        inverted = -raw
        min_v, max_v = inverted.min(), inverted.max()
        if max_v == min_v:
            return np.zeros(len(X))
        return (inverted - min_v) / (max_v - min_v)

    def predict_label(self, X: pd.DataFrame) -> np.ndarray:
        scores = self.score(X)
        return np.where(scores > 0.5, "anomaly", "normal")

    def log_to_mlflow(self, run, X_train: pd.DataFrame, metrics: dict):
        mlflow.sklearn.log_model(
            self.pipeline,
            artifact_path="anomaly_model",
            registered_model_name=f"ot_pdm_anomaly_{self.equipment_id.lower()}"
        )
        mlflow.log_params({
            "equipment_id": self.equipment_id,
            "contamination": self.contamination,
            "n_train_samples": len(X_train),
            "feature_count": len(self.feature_names_),
        })
        for k, v in metrics.items():
            mlflow.log_metric(k, v)
```

---

## Task 4.3 — RUL model (core/ml/rul_model.py)

```python
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np
import pandas as pd
import mlflow

class OTPdMRULModel:
    """
    RUL regression model for a single OT asset.
    Trained on simulator data with injected faults and known RUL labels.
    RUL = hours_until_failure = fault_start_offset_hours × (1 - severity)
    Uses Gradient Boosting — captures nonlinear degradation curves well.
    """
    def __init__(self, equipment_id: str):
        self.equipment_id = equipment_id
        self.pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("gbr", GradientBoostingRegressor(
                n_estimators=300,
                max_depth=4,
                learning_rate=0.05,
                subsample=0.8,
                random_state=42
            ))
        ])

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "OTPdMRULModel":
        """
        X: feature vectors from Gold table
        y: RUL labels in hours (computed from fault injection config)
        """
        self.pipeline.fit(X, y)
        self.feature_names_ = list(X.columns)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.pipeline.predict(X).clip(0)  # RUL cannot be negative

    def evaluate(self, X: pd.DataFrame, y: pd.Series) -> dict:
        y_pred = self.predict(X)
        return {
            "rmse": float(np.sqrt(mean_squared_error(y, y_pred))),
            "r2":   float(r2_score(y, y_pred)),
            "mae":  float(np.abs(y - y_pred).mean()),
        }

    def log_to_mlflow(self, run, metrics: dict, catalog: str):
        mlflow.sklearn.log_model(
            self.pipeline,
            artifact_path="rul_model",
            registered_model_name=f"{catalog}.models.ot_pdm_rul_{self.equipment_id.lower()}"
        )
        mlflow.log_params({
            "equipment_id": self.equipment_id,
            "n_estimators": 300,
            "feature_count": len(self.feature_names_),
        })
        for k, v in metrics.items():
            mlflow.log_metric(k, v)

def generate_rul_labels(feature_df: pd.DataFrame, fault_config: dict) -> pd.Series:
    """
    Synthesise RUL labels from simulator fault injection configuration.
    fault_config: the asset dict from config["simulator"]["assets"]
    Returns pd.Series of RUL values aligned with feature_df index.
    """
    if not fault_config.get("inject_fault"):
        # Healthy asset: RUL = large constant (not failing)
        return pd.Series([9999.0] * len(feature_df), index=feature_df.index)

    severity = fault_config.get("fault_severity", 0.5)
    offset_h = abs(fault_config.get("fault_start_offset_hours", 0))
    # Estimated total life after fault injection: proportional to 1/severity
    estimated_life = 200 * (1 - severity)  # hours

    # RUL decreases linearly from (offset_h + estimated_life) at the start of data
    n = len(feature_df)
    window_hours = 0.25  # 15-minute windows
    rul_start = max(0, estimated_life - offset_h)
    rul_end = max(0, rul_start - n * window_hours)

    return pd.Series(
        np.linspace(rul_start, rul_end, n),
        index=feature_df.index
    ).clip(0)
```

---

## Task 4.4 — DAXS-style parallel training (core/ml/train.py)

```python
"""
Per-asset parallel training using Pandas UDFs.
Trains one Anomaly model + one RUL model per equipment_id.
Logs all models to MLflow Unity Catalog.

Run as a Databricks job (defined in databricks.yml as ot_pdm_training_job).
"""
import mlflow
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from core.config.loader import load_config
from core.ml.anomaly_model import OTPdMAnomalyModel
from core.ml.rul_model import OTPdMRULModel, generate_rul_labels
import os

INDUSTRY = os.environ.get("INDUSTRY", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/ot-pdm-intelligence/{INDUSTRY}")

def train_asset_models(equipment_id: str, spark) -> dict:
    """Train anomaly + RUL models for one asset. Returns metrics dict."""
    from core.ml.features import get_feature_matrix

    X, feature_cols = get_feature_matrix(spark, catalog, equipment_id, n_hours=720)

    if len(X) < 50:
        return {"equipment_id": equipment_id, "status": "skipped", "reason": "insufficient_data"}

    # Get fault config for this asset
    asset_conf = next(
        (a for a in config["simulator"]["assets"] if a["id"] == equipment_id), {}
    )

    # Split: healthy baseline = first 40% of data (before fault onset)
    fault_offset = abs(asset_conf.get("fault_start_offset_hours", 0))
    n_healthy = max(20, len(X) - int(fault_offset / 0.25))
    X_healthy = X.iloc[:n_healthy]
    X_full    = X

    with mlflow.start_run(run_name=f"{equipment_id}_anomaly"):
        anomaly_model = OTPdMAnomalyModel(equipment_id)
        anomaly_model.fit(X_healthy)
        scores = anomaly_model.score(X_full)
        anomaly_model.log_to_mlflow(None, X_healthy, {
            "mean_score_healthy": float(scores[:n_healthy].mean()),
            "mean_score_fault":   float(scores[n_healthy:].mean()) if len(scores) > n_healthy else 0.0,
        })

    with mlflow.start_run(run_name=f"{equipment_id}_rul"):
        y = generate_rul_labels(X_full, asset_conf)
        rul_model = OTPdMRULModel(equipment_id)
        rul_model.fit(X_full, y)
        metrics = rul_model.evaluate(X_full, y)
        rul_model.log_to_mlflow(None, metrics, catalog)

    return {"equipment_id": equipment_id, "status": "trained", **metrics}

# Parallel training across all assets using Pandas UDF
def train_all_assets(spark):
    assets = config["simulator"]["assets"]
    equipment_ids = [a["id"] for a in assets]

    # Use Pandas UDF for parallel execution
    asset_df = spark.createDataFrame(
        [(eid,) for eid in equipment_ids],
        schema="equipment_id STRING"
    )

    @pandas_udf("equipment_id STRING, status STRING, rmse DOUBLE, r2 DOUBLE")
    def train_udf(equipment_ids: pd.Series) -> pd.DataFrame:
        results = []
        for eid in equipment_ids:
            result = train_asset_models(eid, spark)
            results.append({
                "equipment_id": eid,
                "status": result.get("status", "error"),
                "rmse": float(result.get("rmse", -1)),
                "r2":   float(result.get("r2", -1)),
            })
        return pd.DataFrame(results)

    results = asset_df.groupby("equipment_id").apply(train_udf)
    results.show()
    return results

if __name__ == "__main__":
    train_all_assets(spark)
```

---

## Task 4.5 — Feature importance (core/ml/feature_importance.py)

```python
"""
SHAP-based feature importance for Isolation Forest.
Used by the Model Explainability page in the Databricks App.
Writes results to {catalog}.gold.feature_importance table.
"""
import shap
import pandas as pd
import numpy as np

def compute_importance(anomaly_model, X: pd.DataFrame) -> pd.DataFrame:
    """
    Returns DataFrame with columns: feature_name, importance_score (mean |SHAP|).
    """
    explainer = shap.TreeExplainer(anomaly_model.pipeline.named_steps["iso_forest"])
    X_scaled = anomaly_model.pipeline.named_steps["scaler"].transform(X)
    shap_values = explainer.shap_values(X_scaled)

    importance = pd.DataFrame({
        "feature_name": X.columns,
        "importance_score": np.abs(shap_values).mean(axis=0)
    }).sort_values("importance_score", ascending=False)

    return importance
```

---

## Task 4.6 — Batch scoring job (core/ml/batch_score.py)

Runs every 2 hours on schedule. Reads latest feature vectors, scores with production models, writes to `gold.pdm_predictions`.

```python
"""
Batch scoring job. Scheduled every 2 hours via databricks.yml.
Reads latest feature vectors → loads production MLflow models → writes predictions.
"""
def score_all_assets(spark, config: dict, catalog: str):
    from mlflow.tracking import MlflowClient
    client = MlflowClient()

    assets = config["simulator"]["assets"]
    results = []

    for asset in assets:
        eid = asset["id"]
        try:
            anomaly_model = mlflow.sklearn.load_model(
                f"models:/{catalog}.models.ot_pdm_anomaly_{eid.lower()}/Production"
            )
            rul_model = mlflow.sklearn.load_model(
                f"models:/{catalog}.models.ot_pdm_rul_{eid.lower()}/Production"
            )
        except Exception:
            continue  # model not yet trained, skip

        X, feature_cols = get_feature_matrix(spark, catalog, eid, n_hours=2)
        if X.empty:
            continue

        anomaly_scores = OTPdMAnomalyModel(eid)
        anomaly_scores.pipeline = anomaly_model
        scores = anomaly_scores.score(X)
        ruls = rul_model.predict(X)

        # Write latest prediction
        latest_idx = -1
        results.append({
            "equipment_id": eid,
            "prediction_timestamp": pd.Timestamp.utcnow(),
            "anomaly_score": float(scores[latest_idx]),
            "anomaly_label": "anomaly" if scores[latest_idx] > 0.5 else "normal",
            "rul_hours": float(ruls[latest_idx]),
            "predicted_failure_date": (
                pd.Timestamp.utcnow() + pd.Timedelta(hours=float(ruls[latest_idx]))
            ) if ruls[latest_idx] < 9000 else None,
        })

    if results:
        df = spark.createDataFrame(pd.DataFrame(results))
        df.write.format("delta").mode("append").saveAsTable(f"{catalog}.gold.pdm_predictions")

```

---

## Model registration and promotion

After training, register models in Unity Catalog and promote to Production:

```python
client = MlflowClient()
for eid in equipment_ids:
    for model_type in ["anomaly", "rul"]:
        model_name = f"{catalog}.models.ot_pdm_{model_type}_{eid.lower()}"
        versions = client.search_model_versions(f"name='{model_name}'")
        if versions:
            latest = max(versions, key=lambda v: int(v.version))
            client.transition_model_version_stage(
                name=model_name,
                version=latest.version,
                stage="Production"
            )
```

---

## Success criteria

- All assets have trained anomaly + RUL models registered in MLflow Unity Catalog
- Anomaly scores for healthy assets < 0.3 on average
- Anomaly scores for fault-injected assets > 0.5 after fault onset
- RUL predictions decrease monotonically for degrading assets
- RUL RMSE < 50 hours on held-out test set
- Feature importance scores available for all assets
- Batch scoring job runs in under 3 minutes for all assets across one industry
