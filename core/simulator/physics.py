import math
import random


def bearing_wear(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    degradation = min(1.0, severity * (1 - math.exp(-elapsed_h / 200)))
    headroom = 3.0
    value = base * (1 + degradation * headroom)
    return value + random.gauss(0, noise * base)


def thermal_overheat(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    ceiling = base * 1.35
    tau = 400
    degradation = severity * (1 - math.exp(-elapsed_h / tau))
    value = base + degradation * (ceiling - base)
    return value + random.gauss(0, noise * base)


def pressure_drop(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    degradation = min(1.0, severity * elapsed_h / 150)
    floor = base * 0.65
    value = base - degradation * (base - floor)
    return value + random.gauss(0, noise * base)


def recipe_drift(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    drift = severity * elapsed_h * 0.001 * base
    value = base + drift
    return value + random.gauss(0, noise * base)


def contamination_event(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    growth_rate = severity * 0.08
    value = base * math.exp(growth_rate * elapsed_h)
    return max(base, value) + abs(random.gauss(0, noise * base))


def thermal_drift_sinusoidal(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    hvac_amplitude = 0.02 * severity
    hvac = hvac_amplitude * math.sin(2 * math.pi * elapsed_h / 12)
    linear_drift = severity * elapsed_h * 0.002
    value = base + hvac + linear_drift
    return value + random.gauss(0, noise * base)


def wear_index(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    value = base + severity * min(1.0, elapsed_h / 500)
    return min(1.0, value + random.gauss(0, noise * 0.01))


def pipe_leak_acoustic(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    sigmoid = severity / (1 + math.exp(-0.05 * (elapsed_h - 24)))
    value = base + sigmoid * 0.8
    return min(1.0, value + random.gauss(0, noise * 0.05))


PHYSICS_MAP = {
    "bearing_wear": bearing_wear,
    "engine_overheat": thermal_overheat,
    "thermal_runaway": thermal_overheat,
    "servo_degradation": thermal_overheat,
    "die_wear": recipe_drift,
    "tyre_blowout": pressure_drop,
    "pipe_leak": pipe_leak_acoustic,
    "cavitation": pressure_drop,
    "chamber_contamination": contamination_event,
    "overlay_drift": thermal_drift_sinusoidal,
    "focus_drift": thermal_drift_sinusoidal,
    "recipe_drift": recipe_drift,
    "pad_wear": wear_index,
    "laser_degradation": pressure_drop,
    "gearbox_degradation": bearing_wear,
    "transformer_thermal_failure": thermal_overheat,
}
