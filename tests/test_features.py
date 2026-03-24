from core.config.loader import load_config


def test_all_industries_define_required_feature_formulas():
    allowed = {"mean", "stddev", "slope", "zscore", "cumsum"}
    for industry in ["mining", "energy", "water", "automotive", "semiconductor"]:
        cfg = load_config(industry)
        formulas = {f["formula"] for f in cfg["features"]}
        assert formulas.issubset(allowed)
        assert len(cfg["features"]) >= 5
