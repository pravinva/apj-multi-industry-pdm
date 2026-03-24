import pandas as pd
from pyspark.sql import functions as F


def get_feature_matrix(spark, catalog: str, equipment_id: str, n_hours: int = 720):
    df = (
        spark.table(f"{catalog}.gold.feature_vectors")
        .filter(F.col("equipment_id") == equipment_id)
        .filter(F.col("window_start") >= F.expr(f"current_timestamp() - INTERVAL {int(n_hours)} HOURS"))
        .orderBy("window_start")
        .toPandas()
    )
    if df.empty:
        return pd.DataFrame(), []
    exclude = {"equipment_id", "window_start", "window_end", "_processed_at"}
    feature_cols = [c for c in df.columns if c not in exclude]
    x = df[feature_cols].fillna(0.0)
    return x, feature_cols
