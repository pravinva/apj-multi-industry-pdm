# Workstream 3 — Silver & Gold DLT Feature Engineering

## Owner
Pravin Varma

## Depends on
WS1 (schema), WS2 (Bronze table populated)

## Deliverables
- `core/dlt/silver.py` — config-driven feature engineering DLT pipeline
- `core/dlt/gold.py` — Gold table DLT pipeline (MLflow-ready feature vectors)
- `core/features/engineering.py` — feature computation library

---

## Context

Silver applies config-driven feature engineering to the Bronze sensor readings, filtering bad quality codes and computing rolling statistics per sensor per asset. Gold produces MLflow-ready feature vectors — one row per asset per scoring window — that the ML pipeline (WS4) reads for both training and inference.

The feature set is defined in `config["features"]` — see WS1 for the schema. Every feature formula must be implemented and selectable by name from config.

---

## Task 3.1 — Feature engineering library (core/features/engineering.py)

```python
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def apply_features(df: DataFrame, feature_configs: list[dict]) -> DataFrame:
    """
    Apply all features defined in config["features"] to a Silver DataFrame.
    Input: Bronze sensor readings filtered to good/uncertain quality only.
    Output: DataFrame with feature columns appended.
    """
    for feat in feature_configs:
        formula = feat["formula"]
        window_str = feat.get("window", "15 minutes")
        apply_to = feat.get("apply_to", "all")
        col_name = feat["name"]

        window_spec = _build_window(window_str)

        if apply_to == "all":
            df = _apply_formula(df, formula, col_name, window_spec)
        else:
            # Apply only to specific sensors, NULL for others
            df = _apply_formula_conditional(df, formula, col_name, window_spec, apply_to)

    return df

def _build_window(window_str: str):
    """Parse '15 minutes', '1 hour', '30 days' into PySpark window spec."""
    # Map to seconds
    parts = window_str.split()
    n, unit = int(parts[0]), parts[1].lower()
    seconds = {"minutes": 60, "minute": 60, "hours": 3600, "hour": 3600,
               "days": 86400, "day": 86400}[unit] * n
    return (
        Window.partitionBy("equipment_id", "tag_name")
              .orderBy(F.col("timestamp").cast("long"))
              .rangeBetween(-seconds, 0)
    )

def _apply_formula(df: DataFrame, formula: str, col_name: str, window) -> DataFrame:
    if formula == "mean":
        return df.withColumn(col_name, F.avg("value").over(window))
    elif formula == "stddev":
        return df.withColumn(col_name, F.stddev("value").over(window))
    elif formula == "slope":
        # Linear regression slope over window using Theil-Sen estimator approximation
        return df.withColumn(col_name, _slope_udf(F.collect_list("value").over(window)))
    elif formula == "zscore":
        mean_col = f"_zscore_mean_{col_name}"
        std_col  = f"_zscore_std_{col_name}"
        df = df.withColumn(mean_col, F.avg("value").over(window))
        df = df.withColumn(std_col,  F.stddev("value").over(window))
        df = df.withColumn(col_name, F.when(
            F.col(std_col) > 0,
            (F.col("value") - F.col(mean_col)) / F.col(std_col)
        ).otherwise(0.0))
        return df.drop(mean_col, std_col)
    elif formula == "cumsum":
        cum_window = Window.partitionBy("equipment_id","tag_name").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
        return df.withColumn(col_name, F.sum("value").over(cum_window))
    else:
        raise ValueError(f"Unknown feature formula: {formula}")

# Slope UDF using simple linear regression
from pyspark.sql.functions import udf
import numpy as np

@udf(returnType=DoubleType())
def _slope_udf(values):
    if not values or len(values) < 2:
        return 0.0
    x = np.arange(len(values), dtype=float)
    y = np.array(values, dtype=float)
    if np.std(x) == 0:
        return 0.0
    return float(np.polyfit(x, y, 1)[0])
```

---

## Task 3.2 — Silver DLT pipeline (core/dlt/silver.py)

```python
import dlt
from pyspark.sql import functions as F
from core.config.loader import load_config
from core.features.engineering import apply_features
import os

INDUSTRY = spark.conf.get("industry", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]

@dlt.table(
    name="sensor_features",
    comment="Silver: quality-filtered sensor readings with engineered features",
    table_properties={"quality": "silver"}
)
def sensor_features():
    # 1. Read from Bronze, filter to good and uncertain quality only
    #    Bad quality (0x80) excluded from all downstream ML
    bronze = dlt.read("sensor_readings").filter(
        F.col("quality").isin(["good", "uncertain"])
    )

    # 2. Apply config-driven feature engineering
    featured = apply_features(bronze, config["features"])

    # 3. Add quality tracking column
    featured = featured.withColumn(
        "quality_good_pct",
        F.when(F.col("quality") == "good", 1.0).otherwise(0.0)
    )

    return featured.select(
        "equipment_id", "tag_name", "timestamp",
        "value", "unit", "quality", "quality_code",
        # Feature columns added by apply_features
        *[f["name"] for f in config["features"]],
        "quality_good_pct",
        F.current_timestamp().alias("_processed_at")
    )

@dlt.table(
    name="asset_health_scores",
    comment="Silver: per-asset rolling health score based on feature z-scores",
    table_properties={"quality": "silver"}
)
def asset_health_scores():
    """
    Aggregate per-asset health score: weighted mean of z-scores across all sensors,
    normalised to 0–1 (0=healthy, 1=critical).
    Used by the dashboard before MLflow scoring is available.
    """
    return (
        dlt.read("sensor_features")
        .groupBy("equipment_id", F.window("timestamp", "15 minutes"))
        .agg(
            F.avg(F.abs("zscore_30d")).alias("mean_abs_zscore"),
            F.max(F.abs("zscore_30d")).alias("max_abs_zscore"),
            F.count("*").alias("reading_count"),
            F.avg("quality_good_pct").alias("quality_good_pct")
        )
        .withColumn(
            "health_score_raw",
            F.least(F.lit(1.0), F.col("mean_abs_zscore") / 3.0)  # normalise: zscore=3 → score=1
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )
```

---

## Task 3.3 — Gold DLT pipeline (core/dlt/gold.py)

The Gold pipeline produces MLflow-ready feature vectors. It pivots the Silver per-sensor-per-row data into one wide row per asset per time window, with one column per (sensor × feature) combination. This is what the ML training job reads.

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType
from core.config.loader import load_config
import os

INDUSTRY = spark.conf.get("industry", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]

def _feature_columns(config: dict) -> list[str]:
    """Generate the list of expected feature column names after pivot."""
    features = [f["name"] for f in config["features"]]
    sensor_names = []
    for sensors in config["sensors"].values():
        sensor_names.extend([s["name"] for s in sensors])
    return [f"{s}__{f}" for s in sensor_names for f in features]

@dlt.table(
    name="feature_vectors",
    comment="Gold: wide feature vectors per asset per 15-min window — MLflow training input",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"}
)
def feature_vectors():
    """
    Pivot Silver features: one row per (equipment_id, window),
    columns: equipment_id, window_start, {sensor}__{feature} × N, quality_good_pct_mean.
    """
    silver = dlt.read("sensor_features")
    feature_cols = [f["name"] for f in config["features"]]

    # Create composite column key: sensor__feature
    pivot_exprs = {}
    for feat in feature_cols:
        pivot_exprs[feat] = F.first(F.col(feat))

    windowed = silver.groupBy(
        "equipment_id",
        F.window("timestamp", "15 minutes").alias("w")
    ).pivot("tag_name").agg(pivot_exprs)

    # Flatten window struct
    return (
        windowed
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end",   F.col("w.end"))
        .withColumn("_processed_at", F.current_timestamp())
        .drop("w")
    )

@dlt.table(
    name="pdm_predictions",
    comment="Gold: anomaly scores and RUL predictions from MLflow model serving",
    table_properties={"quality": "gold"}
)
def pdm_predictions():
    """
    Score feature vectors using deployed MLflow model endpoint.
    Reads from feature_vectors, calls model serving endpoint, writes predictions.
    """
    import mlflow
    from mlflow.tracking import MlflowClient

    vectors = dlt.read("feature_vectors")

    # Load registered anomaly model
    anomaly_model_uri = f"models:/{catalog}.models.ot_pdm_anomaly_{INDUSTRY}/Production"
    rul_model_uri     = f"models:/{catalog}.models.ot_pdm_rul_{INDUSTRY}/Production"

    anomaly_model = mlflow.sklearn.load_model(anomaly_model_uri)
    rul_model     = mlflow.sklearn.load_model(rul_model_uri)

    # Pandas UDF for batch scoring
    from pyspark.sql.functions import pandas_udf
    import pandas as pd
    import numpy as np

    feature_cols = [c for c in vectors.columns
                    if c not in ("equipment_id","window_start","window_end","_processed_at")]

    @pandas_udf("double")
    def score_anomaly(features: pd.DataFrame) -> pd.Series:
        X = features.fillna(0).values
        # Isolation Forest: decision_function returns negative = anomaly
        scores = anomaly_model.decision_function(X)
        # Normalise to 0–1 (1=most anomalous)
        normalised = 1 - (scores - scores.min()) / (scores.max() - scores.min() + 1e-9)
        return pd.Series(normalised)

    @pandas_udf("double")
    def predict_rul(features: pd.DataFrame) -> pd.Series:
        X = features.fillna(0).values
        return pd.Series(rul_model.predict(X).clip(0))

    return (
        vectors
        .withColumn("anomaly_score", score_anomaly(*[F.col(c) for c in feature_cols]))
        .withColumn("rul_hours",     predict_rul(*[F.col(c) for c in feature_cols]))
        .withColumn("anomaly_label", F.when(F.col("anomaly_score") > 0.5, "anomaly").otherwise("normal"))
        .withColumn("prediction_timestamp", F.col("window_end"))
        .select(
            "equipment_id", "prediction_timestamp",
            "anomaly_score", "anomaly_label", "rul_hours",
            F.current_timestamp().alias("_scored_at")
        )
    )
```

---

## Task 3.4 — Maintenance alerts (written by Gold pipeline to Lakebase)

Add a DLT table that writes alerts when anomaly score or RUL cross thresholds:

```python
@dlt.table(name="maintenance_alerts", comment="Gold: triggered maintenance alerts")
@dlt.expect("has_equipment", "equipment_id IS NOT NULL")
def maintenance_alerts():
    predictions = dlt.read("pdm_predictions")
    failure_modes = config["failure_modes"]

    return (
        predictions
        .filter(F.col("anomaly_score") >= 0.5)   # warning threshold
        .withColumn("severity", F.when(F.col("anomaly_score") >= 0.8, "critical").otherwise("warning"))
        .withColumn("triggered_at", F.current_timestamp())
    )
```

---

## Success criteria

- Silver table populated within 2 minutes of Bronze data arriving
- All feature columns present (no NULLs on sliding window after warm-up period)
- Quality filtering confirmed: rows with `quality_code=0x80` absent from Silver
- Gold feature vectors have one row per asset per 15-minute window
- Anomaly scores in `[0.0, 1.0]` range
- RUL predictions positive (clipped at 0)
- `pdm_predictions` table queryable via Genie with natural language
