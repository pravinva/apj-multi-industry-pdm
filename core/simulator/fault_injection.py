import random

from core.simulator.physics import PHYSICS_MAP, bearing_wear


class FaultInjector:
    """
    Computes faulted value and OPC-UA quality flags per sensor.
    """

    def __init__(self, asset_config: dict, sensor_configs: list[dict]):
        self.fault_mode = asset_config.get("inject_fault")
        self.severity = asset_config.get("fault_severity", 0.0)
        self.elapsed_h = abs(asset_config.get("fault_start_offset_hours", 0))
        self.sensor_map = {s["name"]: s for s in sensor_configs}

    def compute(
        self,
        sensor_name: str,
        base_value: float,
        noise_factor: float,
        additional_elapsed_h: float = 0.0,
    ) -> tuple[float, str, str]:
        sensor = self.sensor_map.get(sensor_name, {})
        affected = self.fault_mode and sensor.get("failure_mode") == self.fault_mode
        elapsed = self.elapsed_h + additional_elapsed_h

        if affected and self.fault_mode:
            physics_fn = PHYSICS_MAP.get(self.fault_mode, bearing_wear)
            value = physics_fn(base_value, self.severity, elapsed, noise_factor)
        else:
            value = base_value + random.gauss(0, noise_factor * max(1.0, base_value))

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
