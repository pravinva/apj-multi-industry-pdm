"""
Loads and validates industry config.yaml.
Used by DLT pipelines, ML training, agent, and app.
"""

from pathlib import Path
from typing import Any

import yaml


def load_config(industry: str, config_root: str = "industries") -> dict[str, Any]:
    """Load config for the given industry and validate required keys."""
    path = Path(config_root) / industry / "config.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    _validate(config)
    return config


def _validate(config: dict[str, Any]) -> None:
    required = [
        "industry",
        "catalog",
        "isa95_hierarchy",
        "simulator",
        "sensors",
        "failure_modes",
        "features",
        "agent",
        "dashboard",
    ]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Config missing required keys: {missing}")


def get_asset_types(config: dict[str, Any]) -> list[str]:
    return list(config["sensors"].keys())


def get_sensors_for_asset(config: dict[str, Any], asset_id: str) -> list[dict[str, Any]]:
    asset = next((a for a in config["simulator"]["assets"] if a["id"] == asset_id), None)
    if not asset:
        return []
    return config["sensors"].get(asset["type"], [])


def get_isa95_fields(config: dict[str, Any]) -> list[str]:
    return [level["key"] for level in config["isa95_hierarchy"]["levels"]]


def get_failure_modes(config: dict[str, Any]) -> dict[str, Any]:
    return config["failure_modes"]


def get_agent_config(config: dict[str, Any]) -> dict[str, Any]:
    return config["agent"]
