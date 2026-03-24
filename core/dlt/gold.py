import dlt
from pyspark.sql import functions as F

from core.config.loader import load_config

INDUSTRY = spark.conf.get("industry", "mining")
config = load_config(INDUSTRY)
catalog = config["catalog"]


@dlt.table(
    name="feature_vectors",
    comment="Gold: wide feature vectors per asset per 15-min window - MLflow training input",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def feature_vectors():
    silver = dlt.read("sensor_features")
    feature_cols = [f["name"] for f in config["features"]]

    grouped = silver.groupBy("equipment_id", F.window("timestamp", "15 minutes"), "tag_name").agg(
        *[F.first(F.col(f)).alias(f) for f in feature_cols]
    )
    rows = []
    for f in feature_cols:
        rows.append(F.struct(F.col("tag_name"), F.lit(f).alias("feature"), F.col(f).alias("v")))

    exploded = grouped.withColumn("kv", F.explode(F.array(*rows))).select(
        "equipment_id",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.concat_ws("__", F.col("kv.tag_name"), F.col("kv.feature")).alias("pivot_key"),
        F.col("kv.v").alias("pivot_value"),
    )

    return (
        exploded.groupBy("equipment_id", "window_start", "window_end")
        .pivot("pivot_key")
        .agg(F.first("pivot_value"))
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
