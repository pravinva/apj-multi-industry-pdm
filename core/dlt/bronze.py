import dlt
from pyspark.sql import functions as F

INDUSTRY = spark.conf.get("industry", "mining")
USE_SIMULATOR = spark.conf.get("use_simulator", "true").lower() == "true"
ZEROBUS_SOURCE_TABLE = spark.conf.get("zerobus_source_table", "_zerobus_staging")
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
    source_df = (
        spark.readStream.format("delta").table(f"{catalog}.bronze._simulator_staging")
        if USE_SIMULATOR
        else spark.readStream.format("delta").table(f"{catalog}.bronze.{ZEROBUS_SOURCE_TABLE}")
    )
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
