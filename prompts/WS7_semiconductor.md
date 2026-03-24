# Workstream 7 — Semiconductor Physics Models + Skin Completion

## Owner
Niels Peter Lassen (validation) + Satoshi Kuramitsu (implementation)

## Depends on
WS2 (physics.py base), WS1 (semiconductor config.yaml)

## Deliverables
- `core/simulator/semiconductor_physics.py` — two new physics models
- `industries/semiconductor/config.yaml` — complete (WS1 task, but Niels validates)
- `industries/semiconductor/system_prompt.txt` — Niels validates against Renesas fab reality
- `industries/semiconductor/genie_questions.json` — 5 questions in fab language
- `industries/semiconductor/seed/parts_inventory.json` — Renesas-accurate part numbers

---

## Context

The semiconductor skin requires two physics models that do not exist in the base simulator (WS2):
1. **Contamination event** (ET-04 etch chamber): Poisson-triggered, then exponential particle count growth
2. **Thermal coupling** (LT-11 stepper): sinusoidal HVAC overlay with slow linear drift

Everything else in the semiconductor skin (CMP pad wear, IN-02 laser degradation) maps to existing physics models.

---

## Task 7.1 — Contamination event physics

The particle count failure mode on plasma etch chambers does not follow a monotonic degradation curve. It has three phases:
1. **Quiescent** (normal operation): particle count near baseline, low noise
2. **Onset** (Poisson-triggered contamination event): step-change increase, triggered stochastically
3. **Growth** (exponential): particle count grows exponentially if untreated

```python
# core/simulator/semiconductor_physics.py
import math, random
import numpy as np

def chamber_contamination(
    base: float,
    severity: float,
    elapsed_h: float,
    noise: float,
    onset_hour: float = None  # hour at which contamination event was triggered
) -> float:
    """
    Plasma etch chamber particle count.
    Three phases: quiescent → onset (Poisson trigger) → exponential growth.

    Args:
        base: baseline particle count (e.g. 4 /cm²)
        severity: fault severity 0–1 (controls growth rate post-onset)
        elapsed_h: hours since fault_start_offset_hours
        noise: noise factor
        onset_hour: if None, use Poisson model to determine onset stochastically
    """
    if onset_hour is None:
        # Poisson model: onset probability increases with severity
        # Expected onset at elapsed_h = 1/severity (e.g. severity=0.9 → onset within ~1h)
        onset_probability_per_hour = severity * 0.5
        onset_hour = -math.log(random.random()) / max(onset_probability_per_hour, 0.01)

    if elapsed_h < onset_hour:
        # Quiescent phase: base + small noise
        return base + abs(random.gauss(0, noise * base))
    else:
        # Exponential growth post-onset
        hours_since_onset = elapsed_h - onset_hour
        growth_rate = severity * 0.15  # ~15% per hour at full severity
        value = base * math.exp(growth_rate * hours_since_onset)
        # Add proportional noise (particle counts are noisy)
        return max(base, value + abs(random.gauss(0, noise * value)))


def overlay_thermal_drift(
    base: float,
    severity: float,
    elapsed_h: float,
    noise: float,
    hvac_period_h: float = 12.0,
    hvac_amplitude_factor: float = 0.008
) -> float:
    """
    Photolithography stepper overlay error / focus drift.
    Two coupled components:
    1. Sinusoidal HVAC thermal cycle (12-hour period, ±hvac_amplitude)
    2. Slow linear drift from lens thermal accumulation

    Args:
        base: baseline overlay error (e.g. 2.1 nm)
        severity: controls rate of linear drift
        elapsed_h: hours since fault onset
        noise: noise factor
        hvac_period_h: HVAC cycle period in hours (default 12h)
        hvac_amplitude_factor: HVAC amplitude as fraction of base
    """
    # Component 1: HVAC sinusoidal cycle
    hvac_amplitude = base * hvac_amplitude_factor * severity
    hvac = hvac_amplitude * math.sin(2 * math.pi * elapsed_h / hvac_period_h)

    # Component 2: slow linear thermal accumulation
    # At severity=1.0, adds ~0.3nm/hour (reaches 5nm spec limit from 2.1nm base in ~10h)
    linear_rate = severity * 0.25  # nm per hour
    linear_drift = linear_rate * elapsed_h

    value = base + hvac + linear_drift
    return max(0, value + random.gauss(0, noise * base))


def focus_drift_coupled(
    base: float,
    severity: float,
    elapsed_h: float,
    noise: float,
    overlay_elapsed_h: float = None
) -> float:
    """
    Focus drift is thermally coupled to overlay error but with a different time constant.
    Use a longer HVAC period and slower linear drift than overlay.
    overlay_elapsed_h: if provided, apply a phase offset to represent coupling.
    """
    # Focus drift has longer thermal time constant — phase-lagged version of overlay
    phase_lag = 3.0  # hours
    effective_elapsed = max(0, elapsed_h - phase_lag)

    hvac_amplitude = base * 0.015 * severity
    hvac = hvac_amplitude * math.sin(2 * math.pi * effective_elapsed / 14.0)

    linear_rate = severity * 0.08
    linear_drift = linear_rate * elapsed_h

    value = base + hvac + linear_drift
    return max(0, value + random.gauss(0, noise * base))
```

Update `PHYSICS_MAP` in `core/simulator/physics.py` to include:
```python
from core.simulator.semiconductor_physics import (
    chamber_contamination, overlay_thermal_drift, focus_drift_coupled
)
PHYSICS_MAP.update({
    "chamber_contamination": chamber_contamination,
    "overlay_drift": overlay_thermal_drift,
    "focus_drift": focus_drift_coupled,
})
```

---

## Task 7.2 — Semiconductor config.yaml validation

Niels Peter Lassen must validate the following fields against Renesas Naka fab reality before the config is finalised:

1. **SEMI E10 hierarchy**: confirm the correct level names for Renesas fab (Fab / Bay / Tool Class / Equipment / Chamber/Module)
2. **Asset IDs**: confirm naming convention used in Renesas fab (e.g. "ET-04" format vs "ETCH-BAY3-04")
3. **Sensor names**: confirm which sensors are actually monitored in Renesas etch tools (particle count, chamber pressure, RF power, gas flows)
4. **Warning/critical thresholds**: confirm actual process spec limits (e.g. particle spec = 20/cm² or different)
5. **Cost per wafer lot**: confirm actual wafer value for the Renesas process node
6. **Agent terminology**: confirm Japanese field labels are accurate for Renesas fab floor usage
7. **Part numbers**: confirm actual consumable part numbers used at Naka factory

Niels validates by reviewing config with a Renesas account contact or Thomas Yoshihara (Renesas SA).

---

## Task 7.3 — Semiconductor system prompt validation

`industries/semiconductor/system_prompt.txt` must be reviewed by Niels against actual Renesas fab operations. Key accuracy requirements:

- Correct tool naming convention (ASML PAS 5500 / Lam Research etch etc)
- Correct process node context (Renesas primarily 40nm–180nm automotive MCU)
- Correct cost framing (USD per wafer lot, yield loss %)
- Correct regulatory context (SEMI E10/E58 equipment availability standards)
- Japanese terminology accurate for a bilingual Renesas fab floor
- SEMI E10 state machine references (Productive time, Standby time, Engineering time, etc)

---

## Task 7.4 — Seed data: parts_inventory.json

Create `industries/semiconductor/seed/parts_inventory.json` with realistic Renesas Naka fab parts. Minimum:

```json
[
  {
    "part_number": "ESC-ET04-A",
    "description": "Electrostatic chuck — Chamber A (ET-04)",
    "quantity": 1,
    "location": "チャンバー部品棚 Bay 3",
    "depot": "Naka Factory Stores",
    "unit_cost": 28000,
    "currency": "USD",
    "lead_time_days": 14
  },
  {
    "part_number": "CLEAN-WET-CF",
    "description": "Wet clean chemical kit — CF process",
    "quantity": 3,
    "location": "クリーニング棚 D-02",
    "depot": "Naka Factory Stores",
    "unit_cost": 1400,
    "currency": "USD",
    "lead_time_days": 2
  }
  // ... add all parts shown in PARTS_DATA.ET-04 and PARTS_DATA.LT-11 in ot_pdm_app_layout.html
]
```

---

## Task 7.5 — Genie questions for semiconductor

`industries/semiconductor/genie_questions.json`:
```json
[
  "Which etch tools had particle counts above spec in the last 24 hours?",
  "What is the total wafer lot exposure across all critical tools today?",
  "Show me overlay error trend for LT-11 over the last 7 days",
  "Which tools are predicted to require maintenance within the next 48 hours?",
  "What is the current fab availability (稼働率) compared to last week?"
]
```

---

## Success criteria

- Semiconductor simulator emits particle count with visible onset event (step-change then exponential) when fault_severity > 0.5
- Overlay error shows sinusoidal pattern plus linear trend — NOT a monotonic curve
- Focus drift is phase-lagged relative to overlay error
- Niels has signed off on config.yaml accuracy against Renesas fab
- System prompt tested with at least 3 evaluation questions
- All parts in seed data match what is shown in the app's parts stock modal
