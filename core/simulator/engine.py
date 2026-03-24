import time
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from core.simulator.fault_injection import FaultInjector


class OTSimulator:
    """
    Physics-based OT simulator.
    """

    def __init__(self, config: dict, spark, catalog: str):
        self.config = config
        self.spark = spark
        self.catalog = catalog
        self.tick_interval_s = config["simulator"]["tick_interval_ms"] / 1000
        self.noise = config["simulator"]["noise_factor"]
        self._injectors: dict[str, FaultInjector] = {}
        self._build_injectors()

    def _build_injectors(self) -> None:
        for asset in self.config["simulator"]["assets"]:
            sensors = self.config["sensors"].get(asset["type"], [])
            self._injectors[asset["id"]] = FaultInjector(asset, sensors)

    def _get_schema(self) -> StructType:
        return StructType(
            [
                StructField("site_id", StringType(), False),
                StructField("area_id", StringType(), False),
                StructField("unit_id", StringType(), False),
                StructField("equipment_id", StringType(), False),
                StructField("component_id", StringType(), True),
                StructField("tag_name", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("unit", StringType(), True),
                StructField("quality", StringType(), False),
                StructField("quality_code", StringType(), False),
                StructField("source_protocol", StringType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )

    def _get_base_value(self, sensor: dict) -> float:
        lo, hi = sensor["normal_range"]
        return (lo + hi) / 2

    def emit_tick(self) -> list[dict]:
        rows = []
        now = datetime.utcnow()

        for asset in self.config["simulator"]["assets"]:
            injector = self._injectors[asset["id"]]
            sensors = self.config["sensors"].get(asset["type"], [])

            for sensor in sensors:
                if __import__("random").random() < 0.7:
                    base = self._get_base_value(sensor)
                    value, quality_label, quality_code = injector.compute(
                        sensor["name"], base, self.noise
                    )
                    rows.append(
                        {
                            "site_id": asset.get("site", ""),
                            "area_id": asset.get("area", ""),
                            "unit_id": asset.get("unit", ""),
                            "equipment_id": asset["id"],
                            "component_id": None,
                            "tag_name": sensor["name"],
                            "value": float(value),
                            "unit": sensor["unit"],
                            "quality": quality_label,
                            "quality_code": quality_code,
                            "source_protocol": self.config["simulator"].get(
                                "protocol", "OPC-UA"
                            ),
                            "timestamp": now,
                        }
                    )
        return rows

    def run(self, max_ticks: int | None = None) -> None:
        schema = self._get_schema()
        tick = 0
        while max_ticks is None or tick < max_ticks:
            rows = self.emit_tick()
            if rows:
                df = self.spark.createDataFrame([Row(**r) for r in rows], schema=schema)
                df.write.format("delta").mode("append").saveAsTable(
                    f"{self.catalog}.bronze._simulator_staging"
                )
            time.sleep(self.tick_interval_s)
            tick += 1
