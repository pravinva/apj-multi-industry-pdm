# EV Cooling and Pump Service Manual (Excerpt)

Equipment Class: Coolant pump, chiller loop, thermal management valves
Revision: C1.7

## Failure Mode Guidance
- Failure mode: engine_overheat
- Typical indicators: coolant_temp rise, pressure oscillation, flow instability
- Temperature limit: 105 C
- Vibration limit: 7.0 mm/s

## Inspection and Repair
- Inspection interval: every 120 operating hours
- Torque: 18 N·m for pump housing fasteners
- Part number: PMP-EV-1120
- Part number: GSK-THERM-44

## Parts and Readiness
- Reorder point guideline: 3 pump cartridges per assembly zone
- Lead time for PMP-EV-1120: 12 days
- If expected failure cost exceeds intervention cost by >20%, prioritize planned intervention.

## Prescriptive Action Notes
- For coolant_temp breaches with anomaly score >= 0.75, trigger immediate planned shutdown window.
- Validate replacement part lot and calibration procedure before restart.
- Capture post-maintenance temperature profile for MLOps drift monitoring.
