# ERP / SAP BDC–shaped ingestion (demo path)

This branch adds a **credible demo pipeline**: SAP-style maintenance orders and cost centers land in **bronze**, hydrate **`lakebase.work_orders`** (operational store), and the **Finance / Executive** view reads work orders from Lakebase when data exists.

## Layers

| Layer | Objects | Role |
|--------|---------|------|
| **Bronze** | `bronze.erp_bdc_work_orders`, `bronze.erp_bdc_cost_centers` | Raw / BDC-shaped landing (`aufnr`, `bdc_session_id`, `bdc_batch_id`, SAP status fields). |
| **Lakebase ODS** | `lakebase.work_orders` (extended columns) | Curated operational work orders used by the app and adoption metrics (`open` / `scheduled` counts by `site_id`). |
| **App** | `OT_PDM_EXECUTIVE_WO_SOURCE`, `_executive_work_orders_from_lakebase` | Loads executive WO list from Lakebase when `auto` (default) and rows exist; else falls back to the synthetic model. |
| **UI** | Finance page | Shows **ERP ingest** banner when `work_order_source === lakebase_ods`, plus `source_system` and `bdc_session_id` per row. |

## 1. DDL (Unity Catalog)

- **File:** `core/catalog/erp_bdc_schema.sql` (substitute `${catalog_name}` → your catalog, e.g. `pdm_mining`).
- **Also:** `RUNME_BOOTSTRAP_ALL.py` calls `core/erp_bdc/seed_demo.apply_erp_bdc_schema` for each industry bootstrap.

## 2. Lakebase column extensions

`core/erp_bdc/seed_demo.ensure_lakebase_columns` (invoked from `RUNME_BOOTSTRAP_ALL._ensure_lakebase_columns`) adds (idempotent):

- `work_center`, `cost_center`, `plant_code`
- `expected_failure_cost`, `intervention_cost`, `net_ebit_impact`
- `avoided_downtime_cost`, `avoided_quality_cost`, `avoided_energy_cost`
- `failure_probability`, `rul_hours`
- `source_system`, `bdc_session_id`, `amount_currency`

Existing `create_work_order` agent paths continue to work; new columns stay `NULL` unless populated.

## 3. Seed / hydrate (simulate BDC → ODS)

**Notebook / job:** run `RUNME_BOOTSTRAP_ALL.py` (or the workspace notebook) for each industry.

It calls `seed_erp_bdc_demo` in `core/erp_bdc/seed_demo.py` **after** `asset_metadata` is seeded:

1. Deletes prior demo rows: `lakebase.work_orders` where `source_system = 'SAP_BDC_DEMO'`, and matching bronze rows.
2. Inserts **`bronze.erp_bdc_cost_centers`** (SAP-style cost centers per plant).
3. Inserts **`bronze.erp_bdc_work_orders`** (PM01-style orders with BDC session/batch ids).
4. **`INSERT … SELECT` UNION** into **`lakebase.work_orders`** with `status` in (`open`, `scheduled`), financial fields, and `source_system = 'SAP_BDC_DEMO'`.

`site_id` on work orders matches **ISA site keys** from `config.yaml` (e.g. `odisha_hub`) so **adoption** `GROUP BY site_id` aligns with `GEO_SITES`.

## 4. App configuration

| Env | Values | Meaning |
|-----|--------|-----------|
| `OT_PDM_EXECUTIVE_WO_SOURCE` | `auto` (default) | Use Lakebase rows when present; else synthetic WO model. |
| | `lakebase` | Executive list **only** from Lakebase (may be empty). |
| | `synthetic` | Always legacy `_executive_work_orders` (ignores Lakebase). |

Declared in `app/app.yaml` for Databricks Apps.

## 5. Job: produce or refresh without full bootstrap

After `databricks bundle deploy`, a job is defined as **`ot-pdm-erp-bdc-seed-refresh`** (`resources.jobs.ot_pdm_erp_bdc_seed_job` in `databricks.yml`).

| Action | How |
|--------|-----|
| **Run all five skins** | *Run now* on the job (default parameters refresh `mining,energy,water,automotive,semiconductor`). |
| **One industry** | Edit task parameters to `--industry` `mining` (remove `--industries` line) or run locally: `python core/erp_bdc/run_erp_bdc_seed_job.py --industry mining`. |

The job runs `core/erp_bdc/run_erp_bdc_seed_job.py`, which calls `run_refresh_for_industry` in `core/erp_bdc/seed_demo.py`: creates bronze ERP tables if needed, ensures `lakebase.work_orders` + ALTER columns, then replaces demo rows (`source_system = 'SAP_BDC_DEMO'`).

**Schedule (optional):** in the job UI add a schedule (e.g. weekly) if you want periodic refresh of demo ERP data.

**Note:** Executive UI still joins to `bronze.asset_metadata` for the current industry. Ensure assets exist for that catalog (from workspace bootstrap or your own seed) so work orders appear in Finance.

## 6. Deploy this branch

1. Merge or deploy from **`feature/erp-bdc-work-orders-demo`**.
2. Run **bootstrap** once per workspace if catalogs/tables are missing (`RUNME_BOOTSTRAP_ALL`).
3. **`databricks bundle deploy`** to register **`ot-pdm-erp-bdc-seed-refresh`**; run it whenever you need to **refresh** BDC/Lakebase demo data.
4. Redeploy the app (`databricks sync` + `databricks apps deploy`).
5. Open **Finance** → confirm **ERP ingest** banner and **SAP_BDC_DEMO** / BDC session on work order rows when Lakebase is populated.

## 7. Production-style evolution

- Replace demo seed with **real BDC / IDoc / CPI jobs** writing to `bronze.erp_bdc_*`.
- Add a **scheduled job** (SQL / DLT) to **MERGE** bronze → `lakebase.work_orders` with idempotent keys on `work_order_id`.
- Optionally add **silver** validation (referential checks to `asset_metadata`, status normalization).
