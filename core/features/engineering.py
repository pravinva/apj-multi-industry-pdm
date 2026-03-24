from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

import numpy as np


def apply_features(df: DataFrame, feature_configs: list[dict]) -> DataFrame:
    for feat in feature_configs:
        formula = feat["formula"]
        window_str = feat.get("window", "15 minutes")
        apply_to = feat.get("apply_to", "all")
        col_name = feat["name"]
        window_spec = _build_window(window_str)

        if apply_to == "all":
            df = _apply_formula(df, formula, col_name, window_spec)
        else:
            df = _apply_formula_conditional(df, formula, col_name, window_spec, apply_to)

    return df


def _build_window(window_str: str):
    parts = window_str.split()
    n, unit = int(parts[0]), parts[1].lower()
    seconds = {"minutes": 60, "minute": 60, "hours": 3600, "hour": 3600, "days": 86400, "day": 86400}[unit] * n
    return (
        Window.partitionBy("equipment_id", "tag_name")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-seconds, 0)
    )


def _apply_formula(df: DataFrame, formula: str, col_name: str, window) -> DataFrame:
    if formula == "mean":
        return df.withColumn(col_name, F.avg("value").over(window))
    if formula == "stddev":
        return df.withColumn(col_name, F.stddev("value").over(window))
    if formula == "slope":
        return df.withColumn(col_name, _slope_udf(F.collect_list("value").over(window)))
    if formula == "zscore":
        mean_col = f"_zscore_mean_{col_name}"
        std_col = f"_zscore_std_{col_name}"
        df = df.withColumn(mean_col, F.avg("value").over(window))
        df = df.withColumn(std_col, F.stddev("value").over(window))
        return df.withColumn(
            col_name,
            F.when(F.col(std_col) > 0, (F.col("value") - F.col(mean_col)) / F.col(std_col)).otherwise(0.0),
        ).drop(mean_col, std_col)
    if formula == "cumsum":
        cum_window = (
            Window.partitionBy("equipment_id", "tag_name")
            .orderBy("timestamp")
            .rowsBetween(Window.unboundedPreceding, 0)
        )
        return df.withColumn(col_name, F.sum("value").over(cum_window))
    raise ValueError(f"Unknown feature formula: {formula}")


def _apply_formula_conditional(
    df: DataFrame, formula: str, col_name: str, window, apply_to: list[str]
) -> DataFrame:
    temp_col = f"__tmp_{col_name}"
    out = _apply_formula(df, formula, temp_col, window)
    return out.withColumn(
        col_name, F.when(F.col("tag_name").isin(apply_to), F.col(temp_col)).otherwise(F.lit(None))
    ).drop(temp_col)


@udf(returnType=DoubleType())
def _slope_udf(values):
    if not values or len(values) < 2:
        return 0.0
    x = np.arange(len(values), dtype=float)
    y = np.array(values, dtype=float)
    if np.std(x) == 0:
        return 0.0
    return float(np.polyfit(x, y, 1)[0])
