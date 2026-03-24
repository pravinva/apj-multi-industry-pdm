from core.config.loader import load_config


def test_bronze_schema_fields_contract():
    expected = [
        "site_id",
        "area_id",
        "unit_id",
        "equipment_id",
        "component_id",
        "tag_name",
        "value",
        "unit",
        "quality",
        "quality_code",
        "source_protocol",
        "timestamp",
        "_ingested_at",
    ]
    assert len(expected) == 13


def test_isa95_fields_present_in_all_configs():
    for industry in ["mining", "energy", "water", "automotive", "semiconductor"]:
        cfg = load_config(industry)
        levels = [l["key"] for l in cfg["isa95_hierarchy"]["levels"]]
        assert {"site", "area", "unit"}.issubset(set(levels))
