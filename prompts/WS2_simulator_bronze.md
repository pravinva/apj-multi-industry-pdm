# Workstream 2 — OT Simulator + Zerobus Bronze DLT Ingest

## Owner
Satoshi Kuramitsu

## Depends on
WS1 (config loader, Unity Catalog schema, asset_metadata table)

## Deliverables
- `core/simulator/engine.py` — physics-based OT simulator
- `core/simulator/fault_injection.py` — fault models per failure mode
- `core/simulator/physics.py` — coupled sensor physics models
- `core/dlt/bronze.py` — DLT Bronze pipeline (streaming ingest, quality codes)
- `core/zerobus_ingest/connector.py` — Zerobus live connector wrapper
- Tests: `tests/test_simulator.py`, `tests/test_bronze.py`

---

## Context

The simulator and the Zerobus connector are two alternative **first-mile** paths that produce identical Bronze table output. `USE_SIMULATOR=true` (default for FEVM buildathon demo) uses the simulator. `USE_SIMULATOR=false` uses the real `unified-ot-zerobus-connector` which runs on edge hardware in the customer OT network and is not deployable inside FEVM.

Both paths write to `{catalog}.bronze.sensor_readings` with the exact schema defined in WS1.

---

## Task 2.1 — Physics models (core/simulator/physics.py)

Each function takes `(base_value, severity, elapsed_hours, noise_factor)` and returns a simulated current reading.

```python
def bearing_wear(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Vibration RMS degradation.
    Follows a polynomial curve: slow initial rise, accelerating near failure.
    severity=1.0 means imminent failure (value at ~3× warning threshold).
    """
    # degradation_factor rises from 0 to 1 as severity * elapsed_h accumulates
    degradation = min(1.0, severity * (1 - math.exp(-elapsed_h / 200)))
    headroom = 3.0  # multiplier above base at full degradation
    value = base * (1 + degradation * headroom)
    return value + random.gauss(0, noise * base)

def thermal_overheat(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Temperature rise: linear trend with thermal lag (exponential approach to ceiling).
    Primary for engine EGT, coolant temp, bearing temp.
    """
    ceiling = base * 1.35
    tau = 400  # time constant hours
    degradation = severity * (1 - math.exp(-elapsed_h / tau))
    value = base + degradation * (ceiling - base)
    return value + random.gauss(0, noise * base)

def pressure_drop(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Pressure declining (suction pressure, tyre pressure, line pressure).
    dir=-1 sensor: lower is worse.
    """
    degradation = min(1.0, severity * elapsed_h / 150)
    floor = base * 0.65
    value = base - degradation * (base - floor)
    return value + random.gauss(0, noise * base)

def recipe_drift(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Semiconductor: slow linear process parameter drift.
    Etch rate, RF power, overlay error.
    """
    drift = severity * elapsed_h * 0.001 * base
    value = base + drift
    return value + random.gauss(0, noise * base)

def contamination_event(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Semiconductor: particle count — exponential growth after contamination onset.
    """
    growth_rate = severity * 0.08
    value = base * math.exp(growth_rate * elapsed_h)
    return max(base, value) + abs(random.gauss(0, noise * base))

def thermal_drift_sinusoidal(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Semiconductor: lens thermal drift — sinusoidal HVAC cycle (12h period)
    plus slow linear component (overlay error, focus drift).
    """
    hvac_amplitude = 0.02 * severity
    hvac = hvac_amplitude * math.sin(2 * math.pi * elapsed_h / 12)
    linear_drift = severity * elapsed_h * 0.002
    value = base + hvac + linear_drift
    return value + random.gauss(0, noise * base)

def wear_index(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Monotonic index rising from 0 to 1 (pad wear, tool wear, pipe acoustic).
    """
    value = base + severity * min(1.0, elapsed_h / 500)
    return min(1.0, value + random.gauss(0, noise * 0.01))

def pipe_leak_acoustic(base: float, severity: float, elapsed_h: float, noise: float) -> float:
    """
    Water: acoustic index — sigmoid onset then slow growth.
    Different profile from bearing wear (more step-change character).
    """
    sigmoid = severity / (1 + math.exp(-0.05 * (elapsed_h - 24)))
    value = base + sigmoid * 0.8
    return min(1.0, value + random.gauss(0, noise * 0.05))
```

Map failure modes to physics functions in a dispatch table:
```python
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
    "laser_degradation": pressure_drop,  # laser power declining
    "gearbox_degradation": bearing_wear,
    "transformer_thermal_failure": thermal_overheat,
}
```

---

## Task 2.2 — Fault injection (core/simulator/fault_injection.py)

```python
class FaultInjector:
    """
    Given an asset config dict and current elapsed time (hours since fault_start),
    computes the faulted value for each sensor.
    """
    def __init__(self, asset_config: dict, sensor_configs: list[dict]):
        self.fault_mode = asset_config.get("inject_fault")
        self.severity = asset_config.get("fault_severity", 0.0)
        self.elapsed_h = abs(asset_config.get("fault_start_offset_hours", 0))
        self.sensor_map = {s["name"]: s for s in sensor_configs}

    def compute(self, sensor_name: str, base_value: float, noise_factor: float,
                additional_elapsed_h: float = 0.0) -> tuple[float, str, str]:
        """
        Returns (value, quality_label, quality_code).
        quality_code follows OPC-UA: 0x00=good, 0x40=uncertain, 0x80=bad.
        """
        sensor = self.sensor_map.get(sensor_name, {})
        affected = self.fault_mode and sensor.get("failure_mode") == self.fault_mode
        elapsed = self.elapsed_h + additional_elapsed_h

        if affected and self.fault_mode:
            physics_fn = PHYSICS_MAP.get(self.fault_mode, bearing_wear)
            value = physics_fn(base_value, self.severity, elapsed, noise_factor)
        else:
            value = base_value + random.gauss(0, noise_factor * base_value)

        # Determine OPC-UA quality code
        dir_ = sensor.get("dir", 1)
        crit = sensor.get("critical_threshold")
        warn = sensor.get("warning_threshold")

        if crit is not None:
            if (dir_ > 0 and value >= crit) or (dir_ < 0 and value <= crit):
                return value, "bad", "0x80"
        if warn is not None:
            if (dir_ > 0 and value >= warn) or (dir_ < 0 and value <= warn):
                return value, "uncertain", "0x40"
        return value, "good", "0x00"
```

---

## Task 2.3 — Simulator engine (core/simulator/engine.py)

```python
class OTSimulator:
    """
    Physics-based OT simulator. Emits rows to a Delta table at the configured tick interval.
    Designed to be run as a Databricks job or notebook.
    Usage:
        sim = OTSimulator(config, spark, catalog="pdm_mining")
        sim.run(max_ticks=None)  # runs indefinitely, or N ticks for testing
    """
    def __init__(self, config: dict, spark, catalog: str):
        self.config = config
        self.spark = spark
        self.catalog = catalog
        self.tick_interval_s = config["simulator"]["tick_interval_ms"] / 1000
        self.noise = config["simulator"]["noise_factor"]
        self._injectors: dict[str, FaultInjector] = {}
        self._build_injectors()

    def _build_injectors(self):
        for asset in self.config["simulator"]["assets"]:
            sensors = self.config["sensors"].get(asset["type"], [])
            self._injectors[asset["id"]] = FaultInjector(asset, sensors)

    def emit_tick(self) -> list[dict]:
        """Generate one tick of sensor readings across all assets. Returns list of row dicts."""
        rows = []
        now = datetime.utcnow()
        isa_levels = [l["key"] for l in self.config["isa95_hierarchy"]["levels"]]

        for asset in self.config["simulator"]["assets"]:
            injector = self._injectors[asset["id"]]
            sensors = self.config["sensors"].get(asset["type"], [])

            for sensor in sensors:
                # Simulate PLC scan: not all sensors fire every tick (realistic)
                if random.random() < 0.7:  # 70% chance each sensor fires per tick
                    base = self._get_base_value(sensor)
                    value, quality_label, quality_code = injector.compute(
                        sensor["name"], base, self.noise
                    )
                    rows.append({
                        "site_id":        asset.get("site", ""),
                        "area_id":        asset.get("area", ""),
                        "unit_id":        asset.get("unit", ""),
                        "equipment_id":   asset["id"],
                        "component_id":   None,
                        "tag_name":       sensor["name"],
                        "value":          float(value),
                        "unit":           sensor["unit"],
                        "quality":        quality_label,
                        "quality_code":   quality_code,
                        "source_protocol": self.config["simulator"].get("protocol", "OPC-UA"),
                        "timestamp":      now,
                    })
        return rows

    def _get_base_value(self, sensor: dict) -> float:
        lo, hi = sensor["normal_range"]
        return (lo + hi) / 2

    def run(self, max_ticks: int | None = None):
        """Run the simulator, writing batches to Delta every tick."""
        from pyspark.sql import Row
        schema = self._get_schema()
        tick = 0
        while max_ticks is None or tick < max_ticks:
            rows = self.emit_tick()
            if rows:
                df = self.spark.createDataFrame([Row(**r) for r in rows], schema=schema)
                df.write.format("delta").mode("append").saveAsTable(
                    f"{self.catalog}.bronze.sensor_readings"
                )
            time.sleep(self.tick_interval_s)
            tick += 1
```

---

## Task 2.4 — Bronze DLT pipeline (core/dlt/bronze.py)

Use the DLT Python API. The Bronze pipeline reads from the simulator output (or real Zerobus stream) and applies schema enforcement and data quality expectations.

```python
import dlt
from pyspark.sql import functions as F
from core.config.loader import load_config
import os

INDUSTRY = spark.conf.get("industry", "mining")
USE_SIMULATOR = spark.conf.get("use_simulator", "true").lower() == "true"
config = load_config(INDUSTRY)
catalog = config["catalog"]

@dlt.table(
    name="sensor_readings_raw",
    comment="Raw OT sensor readings from Zerobus connector or OT simulator",
    table_properties={"quality": "bronze"}
)
@dlt.expect("value_not_null", "value IS NOT NULL")
@dlt.expect("equipment_id_not_null", "equipment_id IS NOT NULL")
@dlt.expect("timestamp_not_null", "timestamp IS NOT NULL")
def sensor_readings_raw():
    if USE_SIMULATOR:
        # Read from simulator output table (simulator writes to a staging table)
        return spark.readStream.format("delta").table(f"{catalog}.bronze._simulator_staging")
    else:
        # Read from Zerobus connector Delta table
        return spark.readStream.format("delta").table(f"{catalog}.bronze._zerobus_staging")

@dlt.table(
    name="sensor_readings",
    comment="Validated Bronze sensor readings with ISA-95 hierarchy and OPC-UA quality codes",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_quality_code", "quality_code IN ('0x00', '0x40', '0x80')")
@dlt.expect_or_drop("valid_value_range", "value BETWEEN -1e9 AND 1e9")
def sensor_readings():
    return (
        dlt.read_stream("sensor_readings_raw")
        .withColumn("_ingested_at", F.current_timestamp())
        .select(
            "site_id", "area_id", "unit_id", "equipment_id", "component_id",
            "tag_name", "value", "unit", "quality", "quality_code",
            "source_protocol", "timestamp", "_ingested_at"
        )
    )
```

---

## Task 2.5 — Zerobus connector wrapper (core/zerobus_ingest/connector.py)

This wraps the `pravinva/unified-ot-zerobus-connector` for use when `USE_SIMULATOR=false`. It reads from the Zerobus output and writes to the `_zerobus_staging` Delta table that the Bronze DLT pipeline reads from.

```python
"""
Wrapper for the unified-ot-zerobus-connector.
Run as a long-running Databricks job when USE_SIMULATOR=false.
Requires network access to the customer OT network — NOT available in FEVM.

Installation:
    pip install unified-ot-zerobus-connector

Configuration:
    Set OT_ENDPOINT, OT_PROTOCOL, OT_SECURITY in Databricks secrets:
    databricks secrets put-secret --scope ot-pdm --key OT_ENDPOINT --string-value "opc.tcp://..."
"""
import os
from zerobus import ZerobusConnector, ISA95Mapper
from core.config.loader import load_config

def start_connector(industry: str, catalog: str, spark):
    config = load_config(industry)
    isa95_levels = [l["key"] for l in config["isa95_hierarchy"]["levels"]]

    endpoint = dbutils.secrets.get("ot-pdm", "OT_ENDPOINT")
    protocol = dbutils.secrets.get("ot-pdm", "OT_PROTOCOL")

    mapper = ISA95Mapper(levels=isa95_levels)

    connector = ZerobusConnector(
        endpoint=endpoint,
        protocol=protocol,
        mapper=mapper,
        output_table=f"{catalog}.bronze._zerobus_staging",
        spark=spark,
        quality_codes=True,   # preserve OPC-UA quality codes — critical
    )
    connector.run()  # blocking
```

---

## Task 2.6 — Tests

```python
# tests/test_simulator.py
def test_healthy_asset_stays_in_range():
    """Healthy asset (no fault injection) stays within normal range."""

def test_fault_injection_drives_sensor_above_warning():
    """Asset with severity=0.8 should exceed warning threshold within 72 simulated hours."""

def test_quality_code_assignment():
    """Sensor above critical threshold gets quality_code=0x80."""

def test_all_industries_load():
    """Config loads without error for all 5 industries."""

def test_bronze_schema_matches_ddl():
    """Emitted rows have exactly the columns defined in schema.sql."""

def test_pyspark_simulator_writes_to_delta(spark):
    """Simulator writes to Delta table with correct schema in test catalog."""
```

---

## Success criteria

- Simulator runs for all 5 industries without error
- Fault injection produces sensor values above warning/critical thresholds at expected severity
- Bronze DLT pipeline passes all data quality expectations
- Bronze table populated within 60 seconds of simulator start
- OPC-UA quality codes present on every row
- ISA-95 fields (site_id, area_id, unit_id) populated correctly from config
