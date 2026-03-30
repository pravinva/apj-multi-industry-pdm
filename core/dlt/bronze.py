import dlt
from pyspark.sql import functions as F

INDUSTRY = spark.conf.get("industry", "mining")
# Canonical per-industry landing table for both simulator and connector ingest.
ZEROBUS_SOURCE_TABLE = spark.conf.get("zerobus_source_table", "pravin_zerobus")
PI_SOURCE_TABLE = spark.conf.get("pi_source_table", "pi_simulated_tags")
catalog = spark.conf.get("catalog_name", f"pdm_{INDUSTRY}")


@dlt.table(
    name="sensor_readings",
    comment="Single Bronze table: Zerobus landing + validation + normalization",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect("value_not_null", "value IS NOT NULL")
@dlt.expect("equipment_id_not_null", "equipment_id IS NOT NULL")
@dlt.expect("timestamp_not_null", "timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_quality_code", "quality_code IN ('0x00', '0x40', '0x80')")
@dlt.expect_or_drop("valid_value_range", "value BETWEEN -1e9 AND 1e9")
def sensor_readings():
    source_df = spark.readStream.format("delta").table(f"{catalog}.bronze.{ZEROBUS_SOURCE_TABLE}")
    return source_df.withColumn(
        "_ingested_at", F.current_timestamp()
    ).select(
        "site_id",
        "area_id",
        "unit_id",
        "equipment_id",
        "component_id",
        "tag_name",
        "value",
        "unit",
        "quality",
        "quality_code",
        "source_protocol",
        "timestamp",
        "_ingested_at",
    )


@dlt.table(
    name="pi_tag_readings",
    comment="Bronze PI-sim tag stream aligned to OT shape",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect("pi_value_not_null", "value IS NOT NULL")
@dlt.expect("pi_equipment_id_not_null", "equipment_id IS NOT NULL")
@dlt.expect("pi_tag_name_not_null", "tag_name IS NOT NULL")
@dlt.expect("pi_timestamp_not_null", "timestamp IS NOT NULL")
def pi_tag_readings():
    source_df = spark.readStream.format("delta").table(f"{catalog}.bronze.{PI_SOURCE_TABLE}")
    return source_df.withColumn(
        "_ingested_at", F.current_timestamp()
    ).select(
        "site_id",
        "area_id",
        "unit_id",
        "equipment_id",
        "component_id",
        "tag_name",
        "value",
        "unit",
        "quality",
        "quality_code",
        "source_protocol",
        "timestamp",
        "_ingested_at",
    )
