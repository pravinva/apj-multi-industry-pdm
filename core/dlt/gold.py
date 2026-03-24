import dlt


@dlt.table(name="pdm_predictions")
def pdm_predictions():
    # Placeholder for WS3/WS4 implementation.
    return spark.sql("SELECT CAST(NULL AS STRING) AS equipment_id WHERE 1=0")
