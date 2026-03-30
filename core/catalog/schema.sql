-- Bronze: raw sensor readings (streaming)
CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.sensor_readings (
  site_id         STRING NOT NULL,
  area_id         STRING NOT NULL,
  unit_id         STRING NOT NULL,
  equipment_id    STRING NOT NULL,
  component_id    STRING,
  tag_name        STRING NOT NULL,
  value           DOUBLE NOT NULL,
  unit            STRING,
  quality         STRING NOT NULL,
  quality_code    STRING NOT NULL,
  source_protocol STRING NOT NULL,
  timestamp       TIMESTAMP NOT NULL,
  _ingested_at    TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.pravin_zerobus (
  site_id         STRING NOT NULL,
  area_id         STRING NOT NULL,
  unit_id         STRING NOT NULL,
  equipment_id    STRING NOT NULL,
  component_id    STRING,
  tag_name        STRING NOT NULL,
  value           DOUBLE NOT NULL,
  unit            STRING,
  quality         STRING NOT NULL,
  quality_code    STRING NOT NULL,
  source_protocol STRING NOT NULL,
  timestamp       TIMESTAMP NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.pi_simulated_tags (
  site_id         STRING NOT NULL,
  area_id         STRING NOT NULL,
  unit_id         STRING NOT NULL,
  equipment_id    STRING NOT NULL,
  component_id    STRING,
  tag_name        STRING NOT NULL,
  value           DOUBLE NOT NULL,
  unit            STRING,
  quality         STRING NOT NULL,
  quality_code    STRING NOT NULL,
  source_protocol STRING NOT NULL,
  timestamp       TIMESTAMP NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.silver.sensor_features (
  equipment_id      STRING NOT NULL,
  tag_name          STRING NOT NULL,
  window_start      TIMESTAMP NOT NULL,
  window_end        TIMESTAMP NOT NULL,
  mean_15m          DOUBLE,
  stddev_15m        DOUBLE,
  slope_1h          DOUBLE,
  zscore_30d        DOUBLE,
  quality_good_pct  DOUBLE,
  reading_count     INT,
  _processed_at     TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.silver.ot_pi_aligned (
  site_id            STRING NOT NULL,
  area_id            STRING NOT NULL,
  unit_id            STRING NOT NULL,
  equipment_id       STRING NOT NULL,
  tag_name           STRING NOT NULL,
  ot_timestamp       TIMESTAMP NOT NULL,
  ot_value           DOUBLE,
  ot_unit            STRING,
  ot_quality         STRING,
  pi_timestamp       TIMESTAMP,
  pi_value           DOUBLE,
  pi_unit            STRING,
  pi_quality         STRING,
  time_delta_seconds BIGINT,
  data_source        STRING,
  _processed_at      TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.gold.pdm_predictions (
  equipment_id              STRING NOT NULL,
  prediction_timestamp      TIMESTAMP NOT NULL,
  anomaly_score             DOUBLE NOT NULL,
  anomaly_label             STRING NOT NULL,
  rul_hours                 DOUBLE,
  predicted_failure_date    TIMESTAMP,
  top_contributing_sensor   STRING,
  top_contributing_score    DOUBLE,
  model_version_anomaly     STRING,
  model_version_rul         STRING,
  _scored_at                TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.gold.financial_impact_events (
  equipment_id              STRING NOT NULL,
  prediction_timestamp      TIMESTAMP NOT NULL,
  severity                  STRING,
  anomaly_score             DOUBLE,
  rul_hours                 DOUBLE,
  event_type                STRING,
  shift_label               STRING,
  maintenance_window_start  TIMESTAMP,
  maintenance_window_end    TIMESTAMP,
  has_maintenance_window    BOOLEAN,
  crew_available            BOOLEAN,
  downtime_hours            DOUBLE,
  maintenance_cost          DOUBLE,
  production_loss           DOUBLE,
  expected_failure_cost     DOUBLE,
  avoided_cost              DOUBLE,
  total_event_cost          DOUBLE,
  data_source               STRING,
  source_table              STRING,
  _computed_at              TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.parts_inventory (
  part_number    STRING NOT NULL,
  description    STRING,
  quantity       INT NOT NULL DEFAULT 0,
  location       STRING,
  depot          STRING,
  unit_cost      DOUBLE,
  currency       STRING DEFAULT 'AUD',
  reorder_point  INT DEFAULT 1,
  lead_time_days INT,
  last_updated   TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.work_orders (
  work_order_id    STRING NOT NULL,
  equipment_id     STRING NOT NULL,
  failure_mode     STRING,
  priority         STRING NOT NULL,
  status           STRING NOT NULL DEFAULT 'draft',
  scheduled_time   TIMESTAMP,
  parts_required   ARRAY<STRING>,
  estimated_hours  DOUBLE,
  created_at       TIMESTAMP DEFAULT current_timestamp(),
  created_by       STRING DEFAULT current_user(),
  updated_at       TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.maintenance_schedule (
  equipment_id               STRING NOT NULL,
  shift_label                STRING NOT NULL,
  shift_start                TIMESTAMP NOT NULL,
  shift_end                  TIMESTAMP NOT NULL,
  planned_downtime_hours     DOUBLE DEFAULT 0,
  maintenance_window_start   TIMESTAMP,
  maintenance_window_end     TIMESTAMP,
  crew_available             BOOLEAN DEFAULT true
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.lakebase.asset_feature_vectors (
  equipment_id   STRING NOT NULL,
  feature_vector ARRAY<DOUBLE> NOT NULL,
  feature_names  ARRAY<STRING> NOT NULL,
  computed_at    TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.asset_metadata (
  equipment_id    STRING NOT NULL,
  site_id         STRING NOT NULL,
  area_id         STRING NOT NULL,
  unit_id         STRING NOT NULL,
  asset_type      STRING NOT NULL,
  asset_model     STRING,
  industry        STRING NOT NULL,
  cost_per_unit   DOUBLE,
  cost_currency   STRING,
  created_at      TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;
