import dlt
from pyspark.sql import functions as F

from core.config.loader import load_config

INDUSTRY = spark.conf.get("industry", "mining")
USE_SIMULATOR = spark.conf.get("use_simulator", "true").lower() == "true"
config = load_config(INDUSTRY)
catalog = config["catalog"]


@dlt.table(
    name="sensor_readings_raw",
    comment="Raw OT sensor readings from Zerobus connector or OT simulator",
    table_properties={"quality": "bronze"},
)
@dlt.expect("value_not_null", "value IS NOT NULL")
@dlt.expect("equipment_id_not_null", "equipment_id IS NOT NULL")
@dlt.expect("timestamp_not_null", "timestamp IS NOT NULL")
def sensor_readings_raw():
    if USE_SIMULATOR:
        return spark.readStream.format("delta").table(f"{catalog}.bronze._simulator_staging")
    return spark.readStream.format("delta").table(f"{catalog}.bronze._zerobus_staging")


@dlt.table(
    name="sensor_readings",
    comment="Validated Bronze sensor readings with ISA-95 hierarchy and OPC-UA quality codes",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_drop("valid_quality_code", "quality_code IN ('0x00', '0x40', '0x80')")
@dlt.expect_or_drop("valid_value_range", "value BETWEEN -1e9 AND 1e9")
def sensor_readings():
    return dlt.read_stream("sensor_readings_raw").withColumn(
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
