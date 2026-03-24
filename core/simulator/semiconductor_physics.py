import math
import random


def chamber_contamination(
    base: float,
    severity: float,
    elapsed_h: float,
    noise: float,
    onset_hour: float | None = None,
) -> float:
    if onset_hour is None:
        onset_probability_per_hour = severity * 0.5
        onset_hour = -math.log(max(1e-9, random.random())) / max(onset_probability_per_hour, 0.01)

    if elapsed_h < onset_hour:
        return base + abs(random.gauss(0, noise * base))

    hours_since_onset = elapsed_h - onset_hour
    growth_rate = severity * 0.15
    value = base * math.exp(growth_rate * hours_since_onset)
    return max(base, value + abs(random.gauss(0, noise * max(base, value))))


def overlay_thermal_drift(
    base: float,
    severity: float,
    elapsed_h: float,
    noise: float,
    hvac_period_h: float = 12.0,
    hvac_amplitude_factor: float = 0.008,
) -> float:
    hvac_amplitude = base * hvac_amplitude_factor * severity
    hvac = hvac_amplitude * math.sin(2 * math.pi * elapsed_h / hvac_period_h)
    linear_rate = severity * 0.25
    linear_drift = linear_rate * elapsed_h
    value = base + hvac + linear_drift
    return max(0.0, value + random.gauss(0, noise * max(1.0, base)))


def focus_drift_coupled(
    base: float,
    severity: float,
    elapsed_h: float,
    noise: float,
    overlay_elapsed_h: float | None = None,
) -> float:
    phase_lag = 3.0
    effective_elapsed = max(0.0, elapsed_h - phase_lag)
    hvac_amplitude = base * 0.015 * severity
    hvac = hvac_amplitude * math.sin(2 * math.pi * effective_elapsed / 14.0)
    linear_rate = severity * 0.08
    linear_drift = linear_rate * elapsed_h
    value = base + hvac + linear_drift
    return max(0.0, value + random.gauss(0, noise * max(1.0, base)))
