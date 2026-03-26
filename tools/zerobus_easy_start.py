"""
Easy local starter for real simulator + real Zerobus connector.

Defaults are pulled from core/zerobus_ingest/defaults.yaml and can be overridden
with environment variables (ZEROBUS_*).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULTS_PATH = ROOT / "core" / "zerobus_ingest" / "defaults.yaml"


def _defaults() -> dict:
    if not DEFAULTS_PATH.exists():
        return {}
    return (yaml.safe_load(DEFAULTS_PATH.read_text(encoding="utf-8")) or {}).get("zerobus", {})


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-simulator", action="store_true")
    parser.add_argument("--no-connector", action="store_true")
    parser.add_argument("--simulator-cmd", default="python -m ot_simulator --protocol all")
    parser.add_argument("--connector-cmd", default="python -m opcua2uc")
    parser.add_argument("--connector-config", default="config.yaml")
    args = parser.parse_args()

    defaults = _defaults()
    print("Using Zerobus defaults from:", DEFAULTS_PATH)
    print("Host:", os.getenv("ZEROBUS_HOST", str(defaults.get("host", "localhost"))))
    print(
        "Protocol defaults:",
        {
            "opcua_port": defaults.get("protocols", {}).get("opcua", {}).get("port", 4840),
            "mqtt_port": defaults.get("protocols", {}).get("mqtt", {}).get("port", 1883),
            "modbus_port": defaults.get("protocols", {}).get("modbus", {}).get("port", 5020),
            "canbus_interface": defaults.get("protocols", {}).get("canbus", {}).get("interface", "vcan0"),
        },
    )

    processes: list[subprocess.Popen] = []

    if (not args.no_simulator) and _bool_env("ZEROBUS_ENABLE_SIMULATOR", True):
        sim_cmd = args.simulator_cmd.split()
        print("Starting simulator:", " ".join(sim_cmd))
        processes.append(subprocess.Popen(sim_cmd))

    if (not args.no_connector) and _bool_env("ZEROBUS_ENABLE_CONNECTOR", True):
        connector_cmd = [*args.connector_cmd.split(), "--config", args.connector_config]
        print("Starting connector:", " ".join(connector_cmd))
        processes.append(subprocess.Popen(connector_cmd))

    if not processes:
        print("No processes started. Check flags/env toggles.")
        return 1

    try:
        while True:
            rcodes = [p.poll() for p in processes]
            if any(r is not None and r != 0 for r in rcodes):
                return 2
            if all(r is not None for r in rcodes):
                return 0
    except KeyboardInterrupt:
        for p in processes:
            if p.poll() is None:
                p.terminate()
        return 130


if __name__ == "__main__":
    sys.exit(main())
