-- SAP/BDC-shaped bronze landing (session/batch IDs mimic IDoc/BDC audit).
-- Replace ${catalog_name} at deploy time (see RUNME_BOOTSTRAP_ALL._render_erp_bdc_schema_sql).

CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.erp_bdc_cost_centers (
  cost_center        STRING NOT NULL COMMENT 'SAP CSKS-KOSTL (trimmed)',
  plant_code         STRING NOT NULL COMMENT 'SAP T001W-WERKS',
  controlling_area   STRING COMMENT 'SAP TKA01-KOKRS',
  description        STRING,
  industry           STRING NOT NULL,
  valid_from         DATE,
  bdc_session_id     STRING COMMENT 'BDC / batch interface session id',
  _bronze_ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog_name}.bronze.erp_bdc_work_orders (
  aufnr              STRING NOT NULL COMMENT 'SAP AFIH-AUFNR maintenance order',
  equipment_id       STRING NOT NULL,
  site_id            STRING NOT NULL COMMENT 'ISA site key (matches GEO / asset_metadata.site_id)',
  plant_code         STRING NOT NULL,
  work_center        STRING COMMENT 'SAP CRHD-ARBPL',
  cost_center        STRING COMMENT 'SAP CSKS-KOSTL',
  order_type         STRING COMMENT 'SAP AUFK-AUART e.g. PM01',
  sap_system_status STRING COMMENT 'REL, TECO, ...',
  sap_user_status   STRING,
  priority_sap      STRING COMMENT '1=critical style',
  planned_cost      DOUBLE,
  actual_cost       DOUBLE,
  currency          STRING,
  short_text        STRING,
  bdc_session_id    STRING,
  bdc_batch_id      STRING,
  industry          STRING NOT NULL,
  _bronze_ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;
