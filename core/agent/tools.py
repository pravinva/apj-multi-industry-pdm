"""
Register all agent tools as Unity Catalog functions.
Run once at deploy time.
"""

from pyspark.sql import SparkSession


def register_all_tools(catalog: str, spark: SparkSession) -> None:
    tools = [
        _get_asset_sensor_history(catalog),
        _get_rul_prediction(catalog),
        _check_parts_inventory(catalog),
        _get_maintenance_schedule(catalog),
        _create_work_order(catalog),
        _estimate_production_impact(catalog),
    ]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.agent_tools")
    for sql in tools:
        spark.sql(sql)


def _get_asset_sensor_history(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.get_asset_sensor_history(
        equipment_id STRING COMMENT 'Asset identifier e.g. HT-012',
        sensor_name STRING COMMENT 'Sensor tag name e.g. engine_egt',
        hours_back INT COMMENT 'How many hours of history to return'
    )
    RETURNS TABLE (timestamp TIMESTAMP, value DOUBLE, quality STRING)
    COMMENT 'Returns recent sensor history for a specific asset and sensor'
    RETURN
      SELECT timestamp, value, quality
      FROM {catalog}.bronze.sensor_readings
      WHERE equipment_id = get_asset_sensor_history.equipment_id
        AND tag_name = get_asset_sensor_history.sensor_name
        AND timestamp >= current_timestamp() - INTERVAL get_asset_sensor_history.hours_back HOURS
      ORDER BY timestamp DESC
      LIMIT 500
    """


def _get_rul_prediction(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.get_rul_prediction(
      equipment_id STRING COMMENT 'Asset identifier'
    )
    RETURNS TABLE (
      anomaly_score DOUBLE,
      anomaly_label STRING,
      rul_hours DOUBLE,
      predicted_failure_date TIMESTAMP,
      top_contributing_sensor STRING,
      scored_at TIMESTAMP
    )
    COMMENT 'Returns latest RUL prediction and anomaly score for an asset'
    RETURN
      SELECT anomaly_score, anomaly_label, rul_hours, predicted_failure_date, top_contributing_sensor, _scored_at
      FROM {catalog}.gold.pdm_predictions
      WHERE equipment_id = get_rul_prediction.equipment_id
      ORDER BY _scored_at DESC
      LIMIT 1
    """


def _check_parts_inventory(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.check_parts_inventory(
      part_number STRING COMMENT 'Part number to check',
      quantity_required INT COMMENT 'Quantity needed',
      depot STRING COMMENT 'Depot/warehouse location'
    )
    RETURNS TABLE (
      part_number STRING, description STRING, quantity INT, location STRING, depot STRING, available BOOLEAN, lead_time_days INT
    )
    COMMENT 'Checks if a part is in stock at the specified depot'
    RETURN
      SELECT part_number, description, quantity, location, depot,
             quantity >= check_parts_inventory.quantity_required AS available,
             lead_time_days
      FROM {catalog}.lakebase.parts_inventory
      WHERE (part_number = check_parts_inventory.part_number OR check_parts_inventory.part_number = '*')
        AND (depot = check_parts_inventory.depot OR check_parts_inventory.depot = '*')
    """


def _get_maintenance_schedule(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.get_maintenance_schedule(
      equipment_id STRING COMMENT 'Asset identifier',
      lookahead_hours INT COMMENT 'How many hours ahead to check'
    )
    RETURNS TABLE (
      shift_label STRING, shift_start TIMESTAMP, shift_end TIMESTAMP,
      maintenance_window_start TIMESTAMP, maintenance_window_end TIMESTAMP, crew_available BOOLEAN
    )
    COMMENT 'Returns upcoming maintenance windows for an asset'
    RETURN
      SELECT shift_label, shift_start, shift_end, maintenance_window_start, maintenance_window_end, crew_available
      FROM {catalog}.lakebase.maintenance_schedule
      WHERE equipment_id = get_maintenance_schedule.equipment_id
        AND shift_start >= current_timestamp()
        AND shift_start <= current_timestamp() + INTERVAL get_maintenance_schedule.lookahead_hours HOURS
      ORDER BY shift_start
    """


def _create_work_order(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.create_work_order(
      equipment_id STRING COMMENT 'Asset to raise work order for',
      failure_mode STRING COMMENT 'Diagnosed failure mode',
      priority STRING COMMENT 'critical | high | medium | low',
      scheduled_time STRING COMMENT 'ISO 8601 datetime string',
      parts_required STRING COMMENT 'Comma-separated part numbers'
    )
    RETURNS TABLE (work_order_id STRING, status STRING, created_at TIMESTAMP)
    COMMENT 'Creates a maintenance work order and returns the work order ID'
    RETURN
      WITH ins AS (
        INSERT INTO {catalog}.lakebase.work_orders
          (work_order_id, equipment_id, failure_mode, priority, status, scheduled_time, parts_required)
        VALUES (
          concat('WO-', date_format(current_timestamp(), 'yyyyMMdd-HHmmss')),
          create_work_order.equipment_id,
          create_work_order.failure_mode,
          create_work_order.priority,
          'submitted',
          to_timestamp(create_work_order.scheduled_time),
          split(create_work_order.parts_required, ',')
        )
      )
      SELECT work_order_id, status, created_at
      FROM {catalog}.lakebase.work_orders
      WHERE equipment_id = create_work_order.equipment_id
      ORDER BY created_at DESC
      LIMIT 1
    """


def _estimate_production_impact(catalog: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {catalog}.agent_tools.estimate_production_impact(
      equipment_id STRING COMMENT 'Asset identifier',
      downtime_hours DOUBLE COMMENT 'Estimated downtime in hours',
      current_production_rate DOUBLE COMMENT 'Current production rate in units/hour'
    )
    RETURNS TABLE (
      equipment_id STRING, downtime_hours DOUBLE, production_loss DOUBLE, cost_estimate DOUBLE, currency STRING
    )
    COMMENT 'Estimates production impact and cost of a maintenance event'
    RETURN
      SELECT
        estimate_production_impact.equipment_id,
        estimate_production_impact.downtime_hours,
        estimate_production_impact.downtime_hours * estimate_production_impact.current_production_rate AS production_loss,
        estimate_production_impact.downtime_hours * estimate_production_impact.current_production_rate
          * COALESCE((SELECT cost_per_unit FROM {catalog}.bronze.asset_metadata
                      WHERE equipment_id = estimate_production_impact.equipment_id), 42000) AS cost_estimate,
        COALESCE((SELECT cost_currency FROM {catalog}.bronze.asset_metadata
                  WHERE equipment_id = estimate_production_impact.equipment_id), 'AUD') AS currency
    """
