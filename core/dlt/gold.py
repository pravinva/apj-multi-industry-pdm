import dlt
from pyspark.sql import functions as F
FEATURE_COLS = ["mean_15m", "stddev_15m", "slope_1h", "zscore_30d", "cumsum_24h"]


@dlt.table(
    name="feature_vectors",
    comment="Gold: fixed-schema feature vectors per asset per 15-min window",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def feature_vectors():
    silver = dlt.read("sensor_features")
    aggs = []
    for f in FEATURE_COLS:
        aggs.append(F.avg(F.col(f)).alias(f"{f}_avg"))
        aggs.append(F.max(F.col(f)).alias(f"{f}_max"))
    aggs.append(F.count("*").alias("reading_count"))
    aggs.append(F.countDistinct("tag_name").alias("tag_count"))
    return (
        silver.groupBy("equipment_id", F.window("timestamp", "15 minutes"))
        .agg(*aggs)
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("_processed_at", F.current_timestamp())
    )


@dlt.table(
    name="pdm_predictions",
    comment="Gold: model-ready rows with placeholders until WS4 scoring is wired",
    table_properties={"quality": "gold"},
)
def pdm_predictions():
    vectors = dlt.read("feature_vectors")
    return vectors.select(
        "equipment_id",
        F.col("window_end").alias("prediction_timestamp"),
        F.lit(0.0).cast("double").alias("anomaly_score"),
        F.lit("normal").alias("anomaly_label"),
        F.lit(None).cast("double").alias("rul_hours"),
        F.current_timestamp().alias("_scored_at"),
    )


@dlt.table(name="maintenance_alerts", comment="Gold: triggered maintenance alerts")
@dlt.expect("has_equipment", "equipment_id IS NOT NULL")
def maintenance_alerts():
    predictions = dlt.read("pdm_predictions")
    return (
        predictions.filter(F.col("anomaly_score") >= 0.5)
        .withColumn("severity", F.when(F.col("anomaly_score") >= 0.8, "critical").otherwise("warning"))
        .withColumn("triggered_at", F.current_timestamp())
    )
