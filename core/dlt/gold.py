import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window
FEATURE_COLS = ["mean_15m", "stddev_15m", "slope_1h", "zscore_30d", "cumsum_24h"]
INDUSTRY = spark.conf.get("industry", "mining")
catalog = spark.conf.get("catalog_name", f"pdm_{INDUSTRY}")


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
    comment="Gold: scored predictions written by offline/online scoring jobs",
    table_properties={"quality": "gold"},
)
def pdm_predictions():
    # Keep schema managed in DLT, but avoid emitting synthetic placeholder rows.
    # Real rows are appended by ML scoring jobs into this table.
    vectors = dlt.read("feature_vectors")
    return vectors.where(F.lit(False)).select(
        "equipment_id",
        F.col("window_end").alias("prediction_timestamp"),
        F.lit(None).cast("double").alias("anomaly_score"),
        F.lit(None).cast("string").alias("anomaly_label"),
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


@dlt.table(
    name="financial_impact_events",
    comment="Gold: OT prediction events linked to maintenance-window and financial impact",
    table_properties={"quality": "gold"},
)
def financial_impact_events():
    predictions = dlt.read("pdm_predictions").where(F.col("anomaly_score").isNotNull())
    data_source = (
        dlt.read("ot_pi_aligned")
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("equipment_id").orderBy(F.col("ot_timestamp").desc_nulls_last())
            ),
        )
        .where(F.col("rn") == 1)
        .select("equipment_id", "data_source")
    )

    # Join against planning windows when present.
    maint_fqn = f"{catalog}.lakebase.maintenance_schedule"
    if spark.catalog.tableExists(maint_fqn):
        maint = spark.table(maint_fqn).select(
            "equipment_id",
            "shift_label",
            "maintenance_window_start",
            "maintenance_window_end",
            "crew_available",
        )
    else:
        maint = spark.createDataFrame(
            [],
            "equipment_id string, shift_label string, maintenance_window_start timestamp, maintenance_window_end timestamp, crew_available boolean",
        )

    # Keep one best planning window per equipment (nearest upcoming start).
    maint_ranked = (
        maint.withColumn(
            "_start_rank",
            F.when(F.col("maintenance_window_start").isNull(), F.lit(9999999999)).otherwise(
                F.abs(F.unix_timestamp(F.col("maintenance_window_start")) - F.unix_timestamp(F.current_timestamp()))
            ),
        )
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("equipment_id").orderBy(F.col("_start_rank").asc(), F.col("maintenance_window_start").asc_nulls_last())
            ),
        )
        .where(F.col("rn") == 1)
        .drop("_start_rank", "rn")
    )

    base = (
        predictions.alias("p")
        .join(data_source.alias("ds"), on="equipment_id", how="left")
        .join(maint_ranked.alias("m"), on="equipment_id", how="left")
        .withColumn("anomaly_score", F.col("p.anomaly_score"))
        .withColumn("rul_hours", F.coalesce(F.col("p.rul_hours"), F.lit(24.0)))
        .withColumn("severity", F.when(F.col("anomaly_score") >= 0.8, F.lit("critical")).otherwise(F.lit("warning")))
        .withColumn(
            "has_maintenance_window",
            F.col("m.maintenance_window_start").isNotNull() & F.col("m.maintenance_window_end").isNotNull(),
        )
        .withColumn("crew_available", F.coalesce(F.col("m.crew_available"), F.lit(False)))
        .withColumn(
            "event_type",
            F.when(F.col("has_maintenance_window") & F.col("crew_available"), F.lit("caught_early")).otherwise(
                F.lit("unplanned_failure")
            ),
        )
    )

    return (
        base.withColumn(
            "downtime_hours",
            F.when(F.col("severity") == "critical", F.lit(10.0)).otherwise(F.lit(6.0))
            * F.when(F.col("event_type") == "caught_early", F.lit(0.6)).otherwise(F.lit(1.3)),
        )
        .withColumn(
            "maintenance_cost",
            F.lit(4000.0)
            + (F.col("anomaly_score") * F.lit(22000.0))
            + F.when(F.col("severity") == "critical", F.lit(9000.0)).otherwise(F.lit(2500.0)),
        )
        .withColumn("production_loss", F.col("downtime_hours") * F.col("anomaly_score") * F.lit(13500.0))
        .withColumn("total_event_cost", F.col("maintenance_cost") + F.col("production_loss"))
        .withColumn(
            "expected_failure_cost",
            (F.col("maintenance_cost") * F.lit(2.7)) + (F.col("production_loss") * F.lit(2.5)),
        )
        .withColumn(
            "avoided_cost",
            F.when(
                F.col("event_type") == "caught_early",
                F.greatest(F.lit(0.0), F.col("expected_failure_cost") - F.col("total_event_cost")),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("source_table", F.lit(f"{catalog}.gold.financial_impact_events"))
        .withColumn(
            "data_source",
            F.coalesce(F.col("ds.data_source"), F.lit("UNKNOWN")),
        )
        .select(
            "equipment_id",
            F.col("prediction_timestamp").alias("prediction_timestamp"),
            "severity",
            "anomaly_score",
            "rul_hours",
            "event_type",
            F.col("m.shift_label").alias("shift_label"),
            F.col("m.maintenance_window_start").alias("maintenance_window_start"),
            F.col("m.maintenance_window_end").alias("maintenance_window_end"),
            "has_maintenance_window",
            "crew_available",
            F.round("downtime_hours", 2).alias("downtime_hours"),
            F.round("maintenance_cost", 2).alias("maintenance_cost"),
            F.round("production_loss", 2).alias("production_loss"),
            F.round("expected_failure_cost", 2).alias("expected_failure_cost"),
            F.round("avoided_cost", 2).alias("avoided_cost"),
            F.round("total_event_cost", 2).alias("total_event_cost"),
            "data_source",
            "source_table",
            F.current_timestamp().alias("_computed_at"),
        )
    )
