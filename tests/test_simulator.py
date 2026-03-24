from core.config.loader import load_config
from core.simulator.fault_injection import FaultInjector


def test_all_industries_load():
    for industry in ["mining", "energy", "water", "automotive", "semiconductor"]:
        cfg = load_config(industry)
        assert cfg["industry"] == industry
        assert len(cfg["simulator"]["assets"]) >= 4


def test_fault_injection_drives_sensor_above_warning():
    cfg = load_config("mining")
    asset = next(a for a in cfg["simulator"]["assets"] if a["id"] == "HT-012")
    sensors = cfg["sensors"][asset["type"]]
    injector = FaultInjector(asset, sensors)
    sensor = next(s for s in sensors if s["name"] == "engine_egt")
    base = sum(sensor["normal_range"]) / 2.0
    value, quality, code = injector.compute(
        "engine_egt", base, cfg["simulator"]["noise_factor"], additional_elapsed_h=1200
    )
    assert value >= sensor["warning_threshold"]
    assert quality in {"uncertain", "bad"}
    assert code in {"0x40", "0x80"}


def test_quality_code_assignment():
    cfg = load_config("mining")
    asset = next(a for a in cfg["simulator"]["assets"] if a["id"] == "HT-012")
    sensors = cfg["sensors"][asset["type"]]
    injector = FaultInjector(asset, sensors)
    sensor = next(s for s in sensors if s["name"] == "engine_egt")
    value, quality, code = injector.compute(
        "engine_egt",
        sensor["critical_threshold"] + 10,
        cfg["simulator"]["noise_factor"],
        additional_elapsed_h=200,
    )
    assert value >= sensor["critical_threshold"]
    assert quality == "bad"
    assert code == "0x80"
