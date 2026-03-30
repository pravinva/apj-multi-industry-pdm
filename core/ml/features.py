import pandas as pd
from pyspark.sql import functions as F


def get_feature_matrix(spark, catalog: str, equipment_id: str, n_hours: int = 720):
    # Standardized canonical source across all industries.
    feature_table = f"{catalog}.bronze.feature_vectors"
    if not spark.catalog.tableExists(feature_table):
        raise RuntimeError(f"Feature table not found: {feature_table}")
    base = spark.table(feature_table).filter(F.col("equipment_id") == equipment_id)
    df = (
        base.filter(F.col("window_start") >= F.expr(f"current_timestamp() - INTERVAL {int(n_hours)} HOURS"))
        .orderBy("window_start")
        .toPandas()
    )
    # Fall back to all available history when the requested recent window is empty.
    if df.empty:
        df = base.orderBy("window_start").toPandas()
    if df.empty:
        return pd.DataFrame(), []
    exclude = {"equipment_id", "window_start", "window_end", "_processed_at"}
    feature_cols = [c for c in df.columns if c not in exclude]
    x = df[feature_cols].fillna(0.0)
    return x, feature_cols
