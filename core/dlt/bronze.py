import dlt
from pyspark.sql import functions as F


@dlt.table(name="sensor_readings")
def sensor_readings():
    # Placeholder for WS2 implementation.
    return spark.sql("SELECT CAST(NULL AS STRING) AS site_id WHERE 1=0").withColumn("_ingested_at", F.current_timestamp())
