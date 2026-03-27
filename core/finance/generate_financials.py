import math
import random
from argparse import ArgumentParser
from datetime import date, timedelta

import pandas as pd
from pyspark.sql import functions as F


INDUSTRY_PROFILES = {
    "mining": {"currency": "AUD", "baseline_monthly_ebit": 42_000_000.0},
    "energy": {"currency": "AUD", "baseline_monthly_ebit": 28_000_000.0},
    "water": {"currency": "AUD", "baseline_monthly_ebit": 9_000_000.0},
    "automotive": {"currency": "JPY", "baseline_monthly_ebit": 620_000_000.0},
    "semiconductor": {"currency": "USD", "baseline_monthly_ebit": 55_000_000.0},
}


def _parse_args() -> ArgumentParser:
    p = ArgumentParser()
    p.add_argument("--industry", default="mining")
    p.add_argument("--catalog", default=None)
    p.add_argument("--days", type=int, default=730)
    return p


def _daily_rows(industry: str, days: int) -> list[dict]:
    profile = INDUSTRY_PROFILES.get(industry, INDUSTRY_PROFILES["mining"])
    baseline = float(profile["baseline_monthly_ebit"])
    currency = str(profile["currency"])
    end = date.today()
    start = end - timedelta(days=max(30, int(days)))
    seed = random.Random(f"{industry}:finance:daily:v1")
    day = start
    rows: list[dict] = []

    # Daily savings roughly centered around 0.4% to 1.2% of baseline monthly EBIT / 30.
    base_daily = baseline / 30.0
    while day <= end:
        day_idx = (day - start).days
        season = 1.0 + 0.12 * math.sin((2 * math.pi * day_idx) / 30.0) + 0.06 * math.sin((2 * math.pi * day_idx) / 365.0)
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


def main() -> None:
    args = _parse_args().parse_args()
    requested = str(args.industry or "mining").strip().lower()
    industries = list(INDUSTRY_PROFILES.keys()) if requested in {"all", "*"} else [requested]

    for industry in industries:
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


if __name__ == "__main__":
    main()

