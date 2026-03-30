from core.zerobus_ingest.connector import build_connector_config


def test_connector_defaults_enable_all_protocol_paths():
    cfg = build_connector_config("mining", "pdm_mining")
    names = {s["name"] for s in cfg["sources"]}
    assert "simulator-opcua" in names
    assert "simulator-mqtt-json" in names
    assert "simulator-mqtt-sparkplug" in names
    assert "simulator-modbus" in names
    assert "simulator-canbus" in names


def test_connector_writes_to_bronze_staging():
    cfg = build_connector_config("energy", "pdm_energy")
    db = cfg["databricks"]
    assert db["catalog"] == "pdm_energy"
    assert db["schema"] == "bronze"
    assert db["target_table"] == "pravin_zerobus"
