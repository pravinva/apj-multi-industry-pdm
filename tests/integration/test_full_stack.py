"""
End-to-end integration tests scaffold.
These tests can run against a live Databricks workspace when Spark/session fixtures are provided.
"""

import os

import pytest

from core.config.loader import load_config

INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]


@pytest.mark.parametrize("industry", INDUSTRIES)
def test_config_loads(industry):
    cfg = load_config(industry)
    assert cfg["industry"] == industry
    assert len(cfg["simulator"]["assets"]) >= 4


@pytest.mark.skipif(
    not os.environ.get("INTEGRATION_LIVE", "").lower() == "true",
    reason="Live integration disabled. Set INTEGRATION_LIVE=true with Spark/app fixtures.",
)
@pytest.mark.parametrize("industry", INDUSTRIES)
def test_live_table_contracts(spark, industry):
    cfg = load_config(industry)
    catalog = cfg["catalog"]
    assert spark.table(f"{catalog}.bronze.sensor_readings").count() >= 0
    assert spark.table(f"{catalog}.silver.sensor_features").count() >= 0
    assert spark.table(f"{catalog}.gold.pdm_predictions").count() >= 0


@pytest.mark.skipif(
    not os.environ.get("INTEGRATION_LIVE", "").lower() == "true",
    reason="Live integration disabled. Set INTEGRATION_LIVE=true with HTTP client fixture.",
)
def test_app_api_endpoints(httpx_client, base_url):
    endpoints = ["/api/fleet/assets", "/api/fleet/kpis", "/api/stream/latest"]
    for ep in endpoints:
        r = httpx_client.get(base_url + ep)
        assert r.status_code == 200
        assert r.json() is not None
