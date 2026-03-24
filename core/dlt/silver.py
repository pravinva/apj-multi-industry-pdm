import dlt
from pyspark.sql import functions as F

from core.config.loader import load_config
from core.features.engineering import apply_features

INDUSTRY = spark.conf.get("industry", "mining")
config = load_config(INDUSTRY)


@dlt.table(
    name="sensor_features",
    comment="Silver: quality-filtered sensor readings with engineered features",
    table_properties={"quality": "silver"},
)
def sensor_features():
    bronze = dlt.read("sensor_readings").filter(F.col("quality").isin(["good", "uncertain"]))
    featured = apply_features(bronze, config["features"])
    featured = featured.withColumn(
        "quality_good_pct", F.when(F.col("quality") == "good", 1.0).otherwise(0.0)
    )
    feature_cols = [f["name"] for f in config["features"]]
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
