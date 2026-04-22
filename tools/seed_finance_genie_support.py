from __future__ import annotations

import os
import sys
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service

INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
DEFAULT_WAREHOUSE_ID = "4b9b953939869799"


@dataclass
class SqlRunner:
    client: WorkspaceClient
    warehouse_id: str

    def run(self, statement: str) -> None:
        resp = self.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            wait_timeout="50s",
            disposition=sql_service.Disposition.INLINE,
        )
        state = resp.status.state if resp.status else None
        if state != sql_service.StatementState.SUCCEEDED:
            err = resp.status.error if resp.status else None
            msg = getattr(err, "message", "") if err is not None else ""
            raise RuntimeError(f"SQL failed [{state}] {msg}\n{statement[:600]}")

    def run_soft(self, statement: str) -> None:
        try:
            self.run(statement)
        except Exception:
            return


def _resolve_warehouse_id(client: WorkspaceClient) -> str:
    running: list[str] = []
    for wh in client.warehouses.list():
        if str(getattr(wh, "state", "") or "").upper() == "RUNNING":
            wid = str(getattr(wh, "id", "") or "").strip()
            if wid:
                running.append(wid)
    if running:
        return running[0]
    for wh in client.warehouses.list():
        wid = str(getattr(wh, "id", "") or "").strip()
        if wid:
            return wid
    raise RuntimeError("No SQL warehouse found in workspace.")


def seed_industry(runner: SqlRunner, industry: str) -> None:
    catalog = f"pdm_{industry}"
    print(f"[seed] industry={industry} catalog={catalog}")

    runner.run_soft(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    runner.run_soft(f"CREATE SCHEMA IF NOT EXISTS {catalog}.finance")
    runner.run_soft(f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
    runner.run_soft(f"GRANT USE SCHEMA ON SCHEMA {catalog}.finance TO `account users`")

    runner.run(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.finance.pm_financial_daily (
          ds DATE,
          industry STRING,
          currency STRING,
          avoided_downtime_cost DOUBLE,
          avoided_quality_cost DOUBLE,
          avoided_energy_cost DOUBLE,
          intervention_cost DOUBLE,
          platform_cost DOUBLE,
          ebit_saved DOUBLE,
          net_benefit DOUBLE,
          baseline_monthly_ebit DOUBLE,
          unplanned_downtime_cost DOUBLE,
          unplanned_downtime_hours DOUBLE,
          maintenance_cost_total DOUBLE,
          units_produced DOUBLE,
          maintenance_cost_per_unit DOUBLE,
          recovered_capacity_units DOUBLE,
          recovered_capacity_pct DOUBLE,
          updated_at TIMESTAMP
        ) USING DELTA
        """
    )
    for col, dtype in [
        ("unplanned_downtime_cost", "DOUBLE"),
        ("unplanned_downtime_hours", "DOUBLE"),
        ("maintenance_cost_total", "DOUBLE"),
        ("units_produced", "DOUBLE"),
        ("maintenance_cost_per_unit", "DOUBLE"),
        ("recovered_capacity_units", "DOUBLE"),
        ("recovered_capacity_pct", "DOUBLE"),
    ]:
        runner.run_soft(f"ALTER TABLE {catalog}.finance.pm_financial_daily ADD COLUMNS ({col} {dtype})")

    runner.run(
        f"""
        UPDATE {catalog}.finance.pm_financial_daily
        SET
          unplanned_downtime_cost = ROUND(
            GREATEST(
              COALESCE(
                unplanned_downtime_cost,
                COALESCE(avoided_downtime_cost, 0) * (
                  1.25 + (
                    CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(industry, ''), 'udc')) % 1000 AS DOUBLE) / 1000.0
                  ) * 0.55
                )
              ),
              0
            ),
            2
          ),
          unplanned_downtime_hours = ROUND(
            GREATEST(
              COALESCE(
                unplanned_downtime_hours,
                (
                  COALESCE(avoided_downtime_cost, 0)
                  * (
                    1.12 + (
                      CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(industry, ''), 'udh')) % 1000 AS DOUBLE) / 1000.0
                    ) * 0.42
                  )
                )
                / (
                  1200 + (
                    CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(currency, ''), 'udden')) % 1000 AS DOUBLE) / 1000.0
                  ) * 900
                )
              ),
              0
            ),
            2
          ),
          maintenance_cost_total = ROUND(
            GREATEST(
              COALESCE(maintenance_cost_total, COALESCE(intervention_cost, 0) + COALESCE(platform_cost, 0) + COALESCE(avoided_energy_cost, 0) * 0.12),
              0
            ),
            2
          ),
          units_produced = ROUND(
            GREATEST(
              COALESCE(
                units_produced,
                (COALESCE(baseline_monthly_ebit, 1000000) / 900.0)
                * (
                  0.72 + (
                    CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(industry, ''), 'units')) % 1000 AS DOUBLE) / 1000.0
                  ) * 0.56
                )
              ),
              1200.0
            ),
            2
          ),
          maintenance_cost_per_unit = ROUND(
            COALESCE(maintenance_cost_total, COALESCE(intervention_cost, 0) + COALESCE(platform_cost, 0) + COALESCE(avoided_energy_cost, 0) * 0.12)
            / GREATEST(
              COALESCE(
                units_produced,
                (COALESCE(baseline_monthly_ebit, 1000000) / 900.0)
                * (
                  0.72 + (
                    CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(industry, ''), 'units')) % 1000 AS DOUBLE) / 1000.0
                  ) * 0.56
                )
              ),
              1.0
            ),
            6
          ),
          recovered_capacity_units = ROUND(
            GREATEST(
              COALESCE(
                recovered_capacity_units,
                LEAST(
                  GREATEST(
                    COALESCE(
                      units_produced,
                      (COALESCE(baseline_monthly_ebit, 1000000) / 900.0)
                      * (
                        0.72 + (
                          CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(industry, ''), 'units')) % 1000 AS DOUBLE) / 1000.0
                        ) * 0.56
                      )
                    ),
                    1200.0
                  ) * 0.24,
                  COALESCE(avoided_downtime_cost, 0) / 220.0
                )
              ),
              0
            ),
            2
          ),
          recovered_capacity_pct = ROUND(
            100.0 * GREATEST(
              COALESCE(
                recovered_capacity_units,
                LEAST(
                  GREATEST(
                    COALESCE(
                      units_produced,
                      (COALESCE(baseline_monthly_ebit, 1000000) / 900.0)
                      * (
                        0.72 + (
                          CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(industry, ''), 'units')) % 1000 AS DOUBLE) / 1000.0
                        ) * 0.56
                      )
                    ),
                    1200.0
                  ) * 0.24,
                  COALESCE(avoided_downtime_cost, 0) / 220.0
                )
              ),
              0
            )
            / GREATEST(
              COALESCE(
                units_produced,
                (COALESCE(baseline_monthly_ebit, 1000000) / 900.0)
                * (
                  0.72 + (
                    CAST(ABS(XXHASH64(CAST(ds AS STRING), COALESCE(industry, ''), 'units')) % 1000 AS DOUBLE) / 1000.0
                  ) * 0.56
                )
              ),
              1.0
            ),
            3
          ),
          updated_at = CURRENT_TIMESTAMP()
        WHERE LOWER(industry) = LOWER('{industry}')
        """
    )

    runner.run(
        f"""
        CREATE OR REPLACE TABLE {catalog}.finance.pm_line_risk_30d
        USING DELTA AS
        WITH asset_lines AS (
          SELECT
            COALESCE(site_id, 'unknown') AS site_id,
            COALESCE(unit_id, area_id, 'line_1') AS line_id,
            COUNT(DISTINCT equipment_id) AS equipment_count
          FROM {catalog}.bronze.asset_metadata
          WHERE LOWER(industry) = LOWER('{industry}')
          GROUP BY 1, 2
        ),
        event_30d AS (
          SELECT
            COALESCE(site_id, 'unknown') AS site_id,
            COALESCE(unit_id, area_id, 'line_1') AS line_id,
            SUM(CASE WHEN LOWER(severity) = 'critical' THEN 1 ELSE 0 END) AS critical_events_30d,
            SUM(CASE WHEN LOWER(severity) = 'warning' THEN 1 ELSE 0 END) AS warning_events_30d,
            COUNT(*) AS total_events_30d,
            ROUND(SUM(COALESCE(expected_failure_cost, 0)), 2) AS ebit_at_risk_30d,
            ROUND(SUM(COALESCE(avoided_cost, 0)), 2) AS avoided_cost_30d
          FROM {catalog}.gold.financial_impact_events
          WHERE prediction_timestamp >= TIMESTAMPADD(DAY, -30, CURRENT_TIMESTAMP())
          GROUP BY 1, 2
        ),
        all_lines AS (
          SELECT site_id, line_id FROM asset_lines
          UNION
          SELECT site_id, line_id FROM event_30d
        )
        SELECT
          CURRENT_DATE() AS ds,
          l.site_id,
          l.line_id,
          COALESCE(a.equipment_count, 0) AS equipment_count,
          COALESCE(e.critical_events_30d, 0) AS critical_events_30d,
          COALESCE(e.warning_events_30d, 0) AS warning_events_30d,
          COALESCE(e.total_events_30d, 0) AS total_events_30d,
          ROUND(
            LEAST(
              100.0,
              (COALESCE(e.critical_events_30d, 0) * 8.0)
              + (COALESCE(e.warning_events_30d, 0) * 3.0)
              + ((COALESCE(e.ebit_at_risk_30d, 0) / GREATEST(1.0, COALESCE(a.equipment_count, 1) * 15000.0)) * 35.0)
            ),
            2
          ) AS risk_score,
          ROUND(COALESCE(e.ebit_at_risk_30d, 0), 2) AS ebit_at_risk_30d,
          ROUND(COALESCE(e.avoided_cost_30d, 0), 2) AS avoided_cost_30d,
          CURRENT_TIMESTAMP() AS updated_at
        FROM all_lines l
        LEFT JOIN asset_lines a ON l.site_id = a.site_id AND l.line_id = a.line_id
        LEFT JOIN event_30d e ON l.site_id = e.site_id AND l.line_id = e.line_id
        """
    )

    runner.run(
        f"""
        CREATE OR REPLACE TABLE {catalog}.finance.supplier_quality_failure_daily
        USING DELTA AS
        WITH inv AS (
          SELECT
            COALESCE(site_id, 'unknown') AS site_id,
            COALESCE(unit_id, area_id, 'line_1') AS line_id,
            COALESCE(equipment_id, 'unknown_equipment') AS equipment_id,
            COALESCE(part_number, CONCAT('PART-', SUBSTR(MD5(COALESCE(description, 'x')), 1, 8))) AS part_number,
            COALESCE(unit_cost, 25.0) AS unit_cost,
            COALESCE(lead_time_days, 7) AS lead_time_days
          FROM {catalog}.lakebase.parts_inventory
        ),
        supplier_map AS (
          SELECT
            *,
            CONCAT('SUP-', LPAD(CAST((ABS(XXHASH64(part_number)) % 12) + 1 AS STRING), 2, '0')) AS supplier_id,
            ROUND(20 + (ABS(XXHASH64(CONCAT(part_number, ':ppm'))) % 360), 2) AS defect_rate_ppm,
            ROUND(70 + ((ABS(XXHASH64(CONCAT(part_number, ':qc'))) % 2800) / 100.0), 2) AS incoming_qc_score
          FROM inv
        ),
        fail_30d AS (
          SELECT
            equipment_id,
            SUM(CASE WHEN LOWER(severity) = 'critical' THEN 1 ELSE 0 END) AS critical_failures_30d,
            SUM(CASE WHEN LOWER(severity) IN ('critical', 'warning') THEN 1 ELSE 0 END) AS alert_failures_30d
          FROM {catalog}.gold.financial_impact_events
          WHERE prediction_timestamp >= TIMESTAMPADD(DAY, -30, CURRENT_TIMESTAMP())
          GROUP BY equipment_id
        )
        SELECT
          CURRENT_DATE() AS ds,
          sm.site_id,
          sm.line_id,
          sm.supplier_id,
          sm.part_number,
          sm.defect_rate_ppm,
          sm.incoming_qc_score,
          COALESCE(f.critical_failures_30d, 0) AS critical_failures_30d,
          COALESCE(f.alert_failures_30d, 0) AS alert_failures_30d,
          GREATEST(8, CAST((ABS(XXHASH64(CONCAT(sm.part_number, sm.site_id))) % 44) + 6 AS INT)) AS installs_30d,
          ROUND(
            COALESCE(f.alert_failures_30d, 0)
            / GREATEST(1.0, CAST((ABS(XXHASH64(CONCAT(sm.part_number, sm.site_id))) % 44) + 6 AS DOUBLE)),
            4
          ) AS failure_rate_30d,
          ROUND(
            (sm.defect_rate_ppm / 1000.0) * 0.58 + ((100.0 - sm.incoming_qc_score) / 100.0) * 0.42,
            4
          ) AS quality_risk_index,
          ROUND(
            ((sm.defect_rate_ppm / 1000.0) * 0.58 + ((100.0 - sm.incoming_qc_score) / 100.0) * 0.42)
            * (1.0 + (COALESCE(f.alert_failures_30d, 0) * 0.06)),
            4
          ) AS correlation_signal,
          CURRENT_TIMESTAMP() AS updated_at
        FROM supplier_map sm
        LEFT JOIN fail_30d f ON sm.equipment_id = f.equipment_id
        """
    )

    for table in [
        "pm_financial_daily",
        "pm_line_risk_30d",
        "supplier_quality_failure_daily",
    ]:
        runner.run_soft(f"GRANT SELECT ON TABLE {catalog}.finance.{table} TO `account users`")
        runner.run_soft(f"GRANT ALL PRIVILEGES ON TABLE {catalog}.finance.{table} TO `account users`")

    print(f"[seed] completed {industry}")


def main() -> int:
    client = WorkspaceClient(profile="DEFAULT")
    requested = (
        os.getenv("OT_PDM_WAREHOUSE_ID", "").strip()
        or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "").strip()
        or DEFAULT_WAREHOUSE_ID
    )
    warehouse_id = requested
    try:
        # Probe selected warehouse quickly; if it no longer exists, fall back.
        probe = SqlRunner(client=client, warehouse_id=warehouse_id)
        probe.run("SELECT 1")
    except Exception:
        warehouse_id = _resolve_warehouse_id(client)
    print(f"[seed] using warehouse_id={warehouse_id}")
    runner = SqlRunner(client=client, warehouse_id=warehouse_id)
    for industry in INDUSTRIES:
        seed_industry(runner, industry)
    print("[seed] all industries completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
