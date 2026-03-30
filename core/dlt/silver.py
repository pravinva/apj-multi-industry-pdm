import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import numpy as np


def _build_window(window_str: str):
    parts = window_str.split()
    n, unit = int(parts[0]), parts[1].lower()
    seconds = {
        "minutes": 60,
        "minute": 60,
        "hours": 3600,
        "hour": 3600,
        "days": 86400,
        "day": 86400,
    }[unit] * n
    return (
        Window.partitionBy("equipment_id", "tag_name")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-seconds, 0)
    )


@udf(returnType=DoubleType())
def _slope_udf(values):
    if not values or len(values) < 2:
        return 0.0
    x = np.arange(len(values), dtype=float)
    y = np.array(values, dtype=float)
    if np.std(x) == 0:
        return 0.0
    return float(np.polyfit(x, y, 1)[0])


def _apply_formula(df, formula: str, col_name: str, window):
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


def apply_features(df, feature_configs: list[dict]):
    for feat in feature_configs:
        formula = feat["formula"]
        window_str = feat.get("window", "15 minutes")
        apply_to = feat.get("apply_to", "all")
        col_name = feat["name"]
        window_spec = _build_window(window_str)

        if apply_to == "all":
            df = _apply_formula(df, formula, col_name, window_spec)
        else:
            temp_col = f"__tmp_{col_name}"
            out = _apply_formula(df, formula, temp_col, window_spec)
            df = out.withColumn(
                col_name, F.when(F.col("tag_name").isin(apply_to), F.col(temp_col)).otherwise(F.lit(None))
            ).drop(temp_col)
    return df

FEATURE_CONFIGS = [
    {"name": "mean_15m", "formula": "mean", "window": "15 minutes", "apply_to": "all"},
    {"name": "stddev_15m", "formula": "stddev", "window": "15 minutes", "apply_to": "all"},
    {"name": "slope_1h", "formula": "slope", "window": "1 hour", "apply_to": "all"},
    {"name": "zscore_30d", "formula": "zscore", "window": "30 days", "apply_to": "all"},
    {"name": "cumsum_24h", "formula": "cumsum", "window": "1 day", "apply_to": "all"},
]


@dlt.table(
    name="sensor_features",
    comment="Silver: quality-filtered sensor readings with engineered features",
    table_properties={"quality": "silver"},
)
def sensor_features():
    bronze = dlt.read("sensor_readings").filter(F.col("quality").isin(["good", "uncertain"]))
    featured = apply_features(bronze, FEATURE_CONFIGS)
    featured = featured.withColumn(
        "quality_good_pct", F.when(F.col("quality") == "good", 1.0).otherwise(0.0)
    )
    feature_cols = [f["name"] for f in FEATURE_CONFIGS]
    return featured.select(
        "equipment_id",
        "tag_name",
        "timestamp",
        "value",
        "unit",
        "quality",
        "quality_code",
        *feature_cols,
        "quality_good_pct",
        F.current_timestamp().alias("_processed_at"),
    )


@dlt.table(
    name="asset_health_scores",
    comment="Silver: per-asset rolling health score based on feature z-scores",
    table_properties={"quality": "silver"},
)
def asset_health_scores():
    return (
        dlt.read("sensor_features")
        .groupBy("equipment_id", F.window("timestamp", "15 minutes"))
        .agg(
            F.avg(F.abs("zscore_30d")).alias("mean_abs_zscore"),
            F.max(F.abs("zscore_30d")).alias("max_abs_zscore"),
            F.count("*").alias("reading_count"),
            F.avg("quality_good_pct").alias("quality_good_pct"),
        )
        .withColumn("health_score_raw", F.least(F.lit(1.0), F.col("mean_abs_zscore") / 3.0))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


@dlt.table(
    name="ot_pi_aligned",
    comment="Silver: nearest OT/PI tag alignment with match quality metrics",
    table_properties={"quality": "silver"},
)
def ot_pi_aligned():
    ot = dlt.read("sensor_readings").select(
        "site_id",
        "area_id",
        "unit_id",
        "equipment_id",
        "tag_name",
        F.col("value").alias("ot_value"),
        F.col("unit").alias("ot_unit"),
        F.col("quality").alias("ot_quality"),
        F.col("timestamp").alias("ot_timestamp"),
    )
    pi = dlt.read("pi_tag_readings").select(
        "equipment_id",
        "tag_name",
        F.col("value").alias("pi_value"),
        F.col("unit").alias("pi_unit"),
        F.col("quality").alias("pi_quality"),
        F.col("timestamp").alias("pi_timestamp"),
    )

    # Align PI rows to nearest OT row per (equipment, tag) within 30 seconds.
    joined = (
        ot.alias("ot")
        .join(
            pi.alias("pi"),
            (
                (F.col("ot.equipment_id") == F.col("pi.equipment_id"))
                & (F.col("ot.tag_name") == F.col("pi.tag_name"))
                & (
                    F.abs(
                        F.unix_timestamp(F.col("ot.ot_timestamp")) - F.unix_timestamp(F.col("pi.pi_timestamp"))
                    )
                    <= F.lit(30)
                )
            ),
            "left",
        )
        .withColumn(
            "time_delta_seconds",
            F.abs(F.unix_timestamp(F.col("ot.ot_timestamp")) - F.unix_timestamp(F.col("pi.pi_timestamp"))),
        )
    )

    best = joined.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy(
                F.col("ot.equipment_id"),
                F.col("ot.tag_name"),
                F.col("ot.ot_timestamp"),
            ).orderBy(F.col("time_delta_seconds").asc_nulls_last(), F.col("pi.pi_timestamp").desc_nulls_last())
        ),
    ).filter(F.col("rn") == 1)

    return best.select(
        F.col("ot.site_id").alias("site_id"),
        F.col("ot.area_id").alias("area_id"),
        F.col("ot.unit_id").alias("unit_id"),
        F.col("ot.equipment_id").alias("equipment_id"),
        F.col("ot.tag_name").alias("tag_name"),
        F.col("ot.ot_timestamp").alias("ot_timestamp"),
        F.col("ot.ot_value").alias("ot_value"),
        F.col("ot.ot_unit").alias("ot_unit"),
        F.col("ot.ot_quality").alias("ot_quality"),
        F.col("pi.pi_timestamp").alias("pi_timestamp"),
        F.col("pi.pi_value").alias("pi_value"),
        F.col("pi.pi_unit").alias("pi_unit"),
        F.col("pi.pi_quality").alias("pi_quality"),
        F.col("time_delta_seconds"),
        F.when(F.col("pi.pi_timestamp").isNotNull(), F.lit("BOTH")).otherwise(F.lit("OT_ONLY")).alias("data_source"),
        F.current_timestamp().alias("_processed_at"),
    )
