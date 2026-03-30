"""
Real Zerobus connector launcher.

This module intentionally runs the official connector CLI (`python -m opcua2uc`)
instead of hardcoding ingestion logic in this repo.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml

from core.config.loader import load_config

_DEFAULTS_PATH = Path(__file__).with_name("defaults.yaml")


def _defaults() -> dict[str, Any]:
    if _DEFAULTS_PATH.exists():
        content = yaml.safe_load(_DEFAULTS_PATH.read_text(encoding="utf-8")) or {}
        return content.get("zerobus", {})
    return {}


def _protocol_enabled(name: str) -> bool:
    defaults = _defaults()
    protocol_default = (
        defaults.get("protocols", {}).get(name, {}).get("enabled", True)
    )
    env = os.getenv(f"ZEROBUS_ENABLE_{name.upper()}", "true").strip().lower()
    if f"ZEROBUS_ENABLE_{name.upper()}" not in os.environ:
        return bool(protocol_default)
    return env in {"1", "true", "yes", "y", "on"}


def _build_sources(host: str) -> list[dict[str, Any]]:
    defaults = _defaults()
    protocols = defaults.get("protocols", {})
    sources: list[dict[str, Any]] = []
    if _protocol_enabled("opcua"):
        opcua_defaults = protocols.get("opcua", {})
        sources.append(
            {
                "name": "simulator-opcua",
                "endpoint": f"opc.tcp://{host}:{int(opcua_defaults.get('port', 4840))}",
                "protocol_type": "opcua",
                "variable_limit": int(
                    os.getenv(
                        "ZEROBUS_OPCUA_VARIABLE_LIMIT",
                        str(opcua_defaults.get("variable_limit", 500)),
                    )
                ),
            }
        )
    if _protocol_enabled("mqtt"):
        mqtt_defaults = protocols.get("mqtt", {})
        mqtt_topics = mqtt_defaults.get("topics", ["sensors/#"])
        if isinstance(mqtt_topics, str):
            mqtt_topics = [mqtt_topics]
        mqtt_topics_env = os.getenv("ZEROBUS_MQTT_TOPICS")
        if mqtt_topics_env:
            mqtt_topics = [t.strip() for t in mqtt_topics_env.split(",") if t.strip()]
        sources.append(
            {
                "name": "simulator-mqtt-json",
                "endpoint": f"mqtt://{host}:{int(mqtt_defaults.get('port', 1883))}",
                "protocol_type": "mqtt",
                "mqtt": {
                    "topics": mqtt_topics,
                    "qos": int(os.getenv("ZEROBUS_MQTT_QOS", str(mqtt_defaults.get("qos", 1)))),
                },
            }
        )
    # Sparkplug B comes through MQTT topics with dedicated parser mode.
    if _protocol_enabled("sparkplug"):
        sparkplug_defaults = protocols.get("sparkplug", {})
        sparkplug_topics = sparkplug_defaults.get("topics", ["spBv1.0/#"])
        if isinstance(sparkplug_topics, str):
            sparkplug_topics = [sparkplug_topics]
        sparkplug_topics_env = os.getenv("ZEROBUS_SPARKPLUG_TOPICS")
        if sparkplug_topics_env:
            sparkplug_topics = [t.strip() for t in sparkplug_topics_env.split(",") if t.strip()]
        sources.append(
            {
                "name": "simulator-mqtt-sparkplug",
                "endpoint": f"mqtt://{host}:{int(sparkplug_defaults.get('port', 1883))}",
                "protocol_type": "mqtt",
                "mqtt": {
                    "topics": sparkplug_topics,
                    "qos": int(
                        os.getenv(
                            "ZEROBUS_MQTT_QOS",
                            str(sparkplug_defaults.get("qos", 1)),
                        )
                    ),
                    "payload_format": "sparkplug_b",
                },
            }
        )
    if _protocol_enabled("modbus"):
        modbus_defaults = protocols.get("modbus", {})
        sources.append(
            {
                "name": "simulator-modbus",
                "endpoint": f"modbus://{host}:{int(modbus_defaults.get('port', 5020))}",
                "protocol_type": "modbus",
                "modbus": {
                    "slave_id": int(
                        os.getenv(
                            "ZEROBUS_MODBUS_SLAVE_ID",
                            str(modbus_defaults.get("slave_id", 1)),
                        )
                    ),
                    "registers": [
                        {
                            "address": int(modbus_defaults.get("register_address", 0)),
                            "count": int(modbus_defaults.get("register_count", 4096)),
                            "type": "holding",
                        }
                    ],
                    "poll_interval_ms": int(
                        os.getenv(
                            "ZEROBUS_MODBUS_POLL_INTERVAL_MS",
                            str(modbus_defaults.get("poll_interval_ms", 500)),
                        )
                    ),
                },
            }
        )
    if _protocol_enabled("canbus"):
        can_defaults = protocols.get("canbus", {})
        frame_ids = can_defaults.get("frame_ids", ["0x100", "0x101", "0x200"])
        if isinstance(frame_ids, str):
            frame_ids = [f.strip() for f in frame_ids.split(",") if f.strip()]
        frame_ids_env = os.getenv("ZEROBUS_CANBUS_FRAME_IDS")
        if frame_ids_env:
            frame_ids = [f.strip() for f in frame_ids_env.split(",") if f.strip()]
        sources.append(
            {
                "name": "simulator-canbus",
                "endpoint": f"canbus://{os.getenv('ZEROBUS_CANBUS_INTERFACE', str(can_defaults.get('interface', 'vcan0')))}",
                "protocol_type": "canbus",
                "canbus": {
                    "interface": os.getenv(
                        "ZEROBUS_CANBUS_INTERFACE",
                        str(can_defaults.get("interface", "vcan0")),
                    ),
                    "bitrate": int(
                        os.getenv(
                            "ZEROBUS_CANBUS_BITRATE",
                            str(can_defaults.get("bitrate", 500000)),
                        )
                    ),
                    "poll_interval_ms": int(
                        os.getenv(
                            "ZEROBUS_CANBUS_POLL_INTERVAL_MS",
                            str(can_defaults.get("poll_interval_ms", 100)),
                        )
                    ),
                    "frame_ids": frame_ids,
                },
            }
        )
    return sources


def build_connector_config(
    industry: str, catalog: str, config_root: str = "industries"
) -> dict[str, Any]:
    cfg = load_config(industry, config_root=config_root)
    defaults = _defaults()
    host = os.getenv("ZEROBUS_HOST", str(defaults.get("host", "localhost")))
    databricks_profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    token = os.getenv("DATABRICKS_TOKEN", "")
    workspace_host = os.getenv("DATABRICKS_HOST", "")

    # The connector writes to the canonical per-industry Bronze landing table so
    # simulator and connector ingest share the same DLT source.
    return {
        "web_port": int(os.getenv("ZEROBUS_WEB_PORT", str(defaults.get("web_port", 8080)))),
        "metrics_port": int(
            os.getenv("ZEROBUS_METRICS_PORT", str(defaults.get("metrics_port", 9090)))
        ),
        "sources": _build_sources(host),
        "databricks": {
            "profile": databricks_profile,
            "host": workspace_host,
            "token": token,
            "catalog": catalog,
            "schema": "bronze",
            "target_table": "pravin_zerobus",
            "quality_codes": True,
            "isa95_levels": [l["key"] for l in cfg.get("isa95_hierarchy", {}).get("levels", [])],
            "industry": industry,
        },
    }


def _write_temp_config(config: dict[str, Any]) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="zerobus_cfg_"))
    cfg_path = temp_dir / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return cfg_path


def ensure_staging_table(spark, catalog: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.pravin_zerobus (
          site_id STRING,
          area_id STRING,
          unit_id STRING,
          equipment_id STRING,
          component_id STRING,
          tag_name STRING,
          value DOUBLE,
          unit STRING,
          quality STRING,
          quality_code STRING,
          source_protocol STRING,
          timestamp TIMESTAMP
        ) USING DELTA
        """
    )


def start_connector(
    industry: str, catalog: str, spark, config_root: str = "industries"
) -> int:
    ensure_staging_table(spark, catalog)
    config = build_connector_config(industry, catalog, config_root=config_root)
    if not config["sources"]:
        raise ValueError("No enabled Zerobus sources. Enable at least one protocol.")

    cfg_path = _write_temp_config(config)
    defaults = _defaults()
    cmd = os.getenv(
        "ZEROBUS_CONNECTOR_CMD",
        str(defaults.get("connector_cmd", "python -m opcua2uc")),
    ).strip()
    argv = [*cmd.split(), "--config", str(cfg_path)]
    proc = subprocess.run(argv, check=False)
    return int(proc.returncode)
