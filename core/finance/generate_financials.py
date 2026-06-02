"""
Regenerate synthetic finance history for executive / Finance UI (daily + site-level).

Safe to run on a schedule: replaces rows per industry for the requested window ending today,
so opening the demo months later still shows ~2–3 years through the last job run.

Run (cluster / job):
  python core/finance/generate_financials.py --industry all --days 1095
  python core/finance/generate_financials.py --industry mining --days 730
"""
from __future__ import annotations

import math
import random
import sys
from argparse import ArgumentParser
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INDUSTRY_PROFILES = {
    "mining": {"currency": "AUD", "baseline_monthly_ebit": 42_000_000.0},
    "energy": {"currency": "AUD", "baseline_monthly_ebit": 28_000_000.0},
    "water": {"currency": "AUD", "baseline_monthly_ebit": 9_000_000.0},
    "automotive": {"currency": "JPY", "baseline_monthly_ebit": 620_000_000.0},
    "semiconductor": {"currency": "USD", "baseline_monthly_ebit": 55_000_000.0},
}

SITE_CURRENCY_OVERRIDES: dict[str, str] = {
    "gangwon_mine": "KRW",
    "seoul_hub": "KRW",
    "ulsan_plant": "KRW",
    "giheung_fab": "KRW",
    "jeju_grid": "KRW",
    "nagoya": "JPY",
    "naka_fab": "JPY",
    "odisha_hub": "INR",
    "chennai_hub": "INR",
    "pune_plant": "INR",
    "bengaluru_fab": "INR",
    "gujarat_grid": "INR",
    "kalimantan_hub": "SGD",
    "singapore_hub": "SGD",
    "bangkok_plant": "SGD",
    "penang_fab": "SGD",
    "vietnam_delta": "SGD",
}


def _repo_root() -> Path:
    candidates: list[Path] = []
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2])
    cwd = Path.cwd().resolve()
    candidates.extend([cwd, *cwd.parents])
    for root in candidates:
        if (root / "industries").exists() and (root / "core" / "finance").exists():
            return root
    return Path.cwd().resolve()


def _parse_args() -> ArgumentParser:
    p = ArgumentParser(description="Refresh finance.pm_financial_daily and pm_site_financial_daily")
    p.add_argument("--industry", default="mining", help="Industry key or 'all' for every skin")
    p.add_argument("--catalog", default=None, help="Override catalog (single-industry runs only)")
    p.add_argument("--days", type=int, default=730, help="Calendar days of history ending today (min 30)")
    p.add_argument(
        "--skip_site_finance",
        action="store_true",
        help="Only refresh pm_financial_daily (skip site-level table)",
    )
    return p


def _site_currency(site_id: str, default_currency: str) -> str:
    return SITE_CURRENCY_OVERRIDES.get(site_id, default_currency)


def _load_industry_cfg(root: Path, industry: str) -> dict[str, Any] | None:
    path = root / "industries" / industry / "config.yaml"
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def _daily_rows(industry: str, days: int) -> list[dict]:
    profile = INDUSTRY_PROFILES.get(industry, INDUSTRY_PROFILES["mining"])
    baseline = float(profile["baseline_monthly_ebit"])
    currency = str(profile["currency"])
    end = date.today()
    start = end - timedelta(days=max(30, int(days)))
    seed = random.Random(f"{industry}:finance:daily:v1")
    day = start
    rows: list[dict] = []

    base_daily = baseline / 30.0
    while day <= end:
        day_idx = (day - start).days
        season = 1.0 + 0.12 * math.sin((2 * math.pi * day_idx) / 30.0) + 0.06 * math.sin(
            (2 * math.pi * day_idx) / 365.0
        )
        drift = 1.0 + (0.0003 * day_idx)
        noise = seed.uniform(0.87, 1.13)
        ebit_saved = max(0.0, base_daily * 0.009 * season * drift * noise)

        avoided_downtime = ebit_saved * seed.uniform(1.48, 1.72)
        avoided_quality = ebit_saved * seed.uniform(0.10, 0.20)
        avoided_energy = ebit_saved * seed.uniform(0.11, 0.22)
        intervention_cost = ebit_saved * seed.uniform(0.20, 0.34)
        platform_cost = ebit_saved * seed.uniform(0.08, 0.16)
        net_benefit = avoided_downtime + avoided_quality + avoided_energy - intervention_cost - platform_cost
        ebit_saved = max(0.0, net_benefit)

        rows.append(
            {
                "ds": day,
                "industry": industry,
                "currency": currency,
                "avoided_downtime_cost": round(avoided_downtime, 2),
                "avoided_quality_cost": round(avoided_quality, 2),
                "avoided_energy_cost": round(avoided_energy, 2),
                "intervention_cost": round(intervention_cost, 2),
                "platform_cost": round(platform_cost, 2),
                "ebit_saved": round(ebit_saved, 2),
                "net_benefit": round(net_benefit, 2),
                "baseline_monthly_ebit": round(baseline, 2),
                "updated_at": pd.Timestamp.utcnow(),
            }
        )
        day += timedelta(days=1)
    return rows


def _site_financial_rows(industry: str, cfg: dict[str, Any], days: int) -> list[dict[str, Any]]:
    profile = INDUSTRY_PROFILES.get(industry, INDUSTRY_PROFILES["mining"])
    default_cur = str(profile["currency"])
    assets = (cfg.get("simulator", {}) or {}).get("assets", [])
    site_stats: dict[str, dict[str, int]] = {}
    for a in assets:
        site = str(a.get("site", ""))
        sev = float(a.get("fault_severity", 0.0) or 0.0)
        s = site_stats.setdefault(site, {"critical": 0, "warning": 0, "total": 0})
        s["total"] += 1
        if sev >= 0.8:
            s["critical"] += 1
        elif sev >= 0.5:
            s["warning"] += 1

    if not site_stats:
        return []

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(30, int(days)))
    rng = random.Random(f"{industry}:site-finance:v2")
    rows: list[dict[str, Any]] = []
    day = start
    while day <= end:
        for site_id, st in site_stats.items():
            critical = st["critical"]
            warning = st["warning"]
            risk_weight = (critical * 1.0) + (warning * 0.45)
            avoided = max(0.0, rng.uniform(900.0, 2200.0) * (1.0 + risk_weight))
            intervention = max(0.0, avoided * rng.uniform(0.22, 0.45))
            rows.append(
                {
                    "ds": day,
                    "industry": industry,
                    "site_id": site_id,
                    "currency": _site_currency(site_id, default_cur),
                    "avoided_cost": round(avoided, 2),
                    "intervention_cost": round(intervention, 2),
                    "net_benefit": round(avoided - intervention, 2),
                    "critical_assets": critical,
                    "warning_assets": warning,
                    "updated_at": pd.Timestamp.utcnow(),
                }
            )
        day += timedelta(days=1)
    return rows


def main() -> None:
    args = _parse_args().parse_args()
    spark = SparkSession.builder.getOrCreate()

    requested = str(args.industry or "mining").strip().lower()
    industries = list(INDUSTRY_PROFILES.keys()) if requested in {"all", "*"} else [requested]

    root = _repo_root()

    for industry in industries:
        if industry not in INDUSTRY_PROFILES:
            print(f"[finance] skip unknown industry={industry}", file=sys.stderr)
            continue

        catalog = args.catalog if (len(industries) == 1 and args.catalog) else f"pdm_{industry}"
        rows = _daily_rows(industry, args.days)
        if not rows:
            continue

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.finance")
        spark.sql(
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
              updated_at TIMESTAMP
            ) USING DELTA
            """
        )
        spark.sql(f"DELETE FROM {catalog}.finance.pm_financial_daily WHERE industry = '{industry}'")
        pdf = pd.DataFrame(rows)
        pdf["ds"] = pd.to_datetime(pdf["ds"]).dt.date
        df = spark.createDataFrame(pdf).withColumn("ds", F.to_date(F.col("ds")))
        df.write.mode("append").saveAsTable(f"{catalog}.finance.pm_financial_daily")
        print(f"[finance] pm_financial_daily refreshed industry={industry} catalog={catalog} days={args.days}")

        if args.skip_site_finance:
            continue

        cfg = _load_industry_cfg(root, industry)
        if not cfg:
            print(f"[finance] skip pm_site_financial_daily (no config.yaml) industry={industry}")
            continue

        site_rows = _site_financial_rows(industry, cfg, args.days)
        if not site_rows:
            print(f"[finance] skip pm_site_financial_daily (no sites) industry={industry}")
            continue

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {catalog}.finance.pm_site_financial_daily (
              ds DATE,
              industry STRING,
              site_id STRING,
              currency STRING,
              avoided_cost DOUBLE,
              intervention_cost DOUBLE,
              net_benefit DOUBLE,
              critical_assets INT,
              warning_assets INT,
              updated_at TIMESTAMP
            ) USING DELTA
            """
        )
        spark.sql(f"DELETE FROM {catalog}.finance.pm_site_financial_daily WHERE industry = '{industry}'")
        spdf = pd.DataFrame(site_rows)
        spdf["ds"] = pd.to_datetime(spdf["ds"]).dt.date
        sdf = spark.createDataFrame(spdf).withColumn("ds", F.to_date(F.col("ds")))
        sdf.write.mode("append").saveAsTable(f"{catalog}.finance.pm_site_financial_daily")
        print(f"[finance] pm_site_financial_daily refreshed industry={industry} catalog={catalog}")


if __name__ == "__main__":
    main()
