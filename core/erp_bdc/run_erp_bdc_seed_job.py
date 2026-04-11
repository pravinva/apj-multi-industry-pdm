"""
Databricks job: refresh SAP/BDC-shaped bronze + Lakebase ODS demo work orders.

Run manually after deploy or on a schedule. Idempotent for rows tagged source_system = SAP_BDC_DEMO.

Usage (cluster / job):
  python core/erp_bdc/run_erp_bdc_seed_job.py --industries mining,energy,water,automotive,semiconductor
  python core/erp_bdc/run_erp_bdc_seed_job.py --industry mining
"""
from __future__ import annotations

import sys
from argparse import ArgumentParser
from pathlib import Path


def _repo_root() -> Path:
    candidates: list[Path] = []
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2])
    cwd = Path.cwd().resolve()
    candidates.extend([cwd, *cwd.parents])
    for root in candidates:
        if (root / "industries").exists() and (root / "core" / "erp_bdc").exists():
            return root
    return Path.cwd().resolve()


def main() -> None:
    parser = ArgumentParser(description="Refresh ERP/BDC demo landing + Lakebase work orders")
    parser.add_argument(
        "--industries",
        default=None,
        help="Comma-separated industry keys (default: all five skins)",
    )
    parser.add_argument(
        "--industry",
        default=None,
        help="Single industry (overrides --industries when set)",
    )
    args = parser.parse_args()

    root = _repo_root()
    root_s = str(root)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)

    from pyspark.sql import SparkSession

    from core.erp_bdc.seed_demo import run_refresh_for_industry

    spark = SparkSession.builder.getOrCreate()

    if args.industry:
        industries = [args.industry.strip().lower()]
    elif args.industries:
        industries = [i.strip().lower() for i in args.industries.split(",") if i.strip()]
    else:
        industries = ["mining", "energy", "water", "automotive", "semiconductor"]

    for ind in industries:
        print(f"[erp-bdc-seed] refreshing industry={ind}")
        try:
            run_refresh_for_industry(spark, root, ind)
        except Exception as e:
            print(f"[erp-bdc-seed] FAILED industry={ind}: {e}")
            raise

    print("[erp-bdc-seed] done")


if __name__ == "__main__":
    main()
