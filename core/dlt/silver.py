import dlt


@dlt.table(name="sensor_features")
def sensor_features():
    # Placeholder for WS3 implementation.
    return spark.sql("SELECT CAST(NULL AS STRING) AS equipment_id WHERE 1=0")
