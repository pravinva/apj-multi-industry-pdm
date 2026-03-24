from core.simulator.semiconductor_physics import (
    chamber_contamination,
    focus_drift_coupled,
    overlay_thermal_drift,
)


def test_contamination_event_grows_post_onset():
    base = 4.0
    early = chamber_contamination(base, 0.9, elapsed_h=0.2, noise=0.0, onset_hour=1.0)
    late = chamber_contamination(base, 0.9, elapsed_h=6.0, noise=0.0, onset_hour=1.0)
    assert early >= base
    assert late > early


def test_overlay_and_focus_show_drift():
    overlay_1 = overlay_thermal_drift(2.1, 0.7, elapsed_h=1.0, noise=0.0)
    overlay_2 = overlay_thermal_drift(2.1, 0.7, elapsed_h=8.0, noise=0.0)
    focus_1 = focus_drift_coupled(1.2, 0.7, elapsed_h=1.0, noise=0.0)
    focus_2 = focus_drift_coupled(1.2, 0.7, elapsed_h=8.0, noise=0.0)
    assert overlay_2 > overlay_1
    assert focus_2 > focus_1
