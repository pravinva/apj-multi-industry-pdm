import pandas as pd

from core.ml.rul_model import generate_rul_labels


def test_generate_rul_labels_healthy_asset():
    df = pd.DataFrame({"f1": [1, 2, 3]})
    y = generate_rul_labels(df, {})
    assert len(y) == 3
    assert all(v == 9999.0 for v in y.values)


def test_generate_rul_labels_fault_asset_monotonic_non_negative():
    df = pd.DataFrame({"f1": list(range(20))})
    y = generate_rul_labels(
        df, {"inject_fault": "bearing_wear", "fault_severity": 0.8, "fault_start_offset_hours": -6}
    )
    assert (y >= 0).all()
    assert y.iloc[0] >= y.iloc[-1]
