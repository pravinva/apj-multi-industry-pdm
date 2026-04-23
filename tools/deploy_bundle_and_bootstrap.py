#!/usr/bin/env python3
"""
One-command deployment for all industries + app + bootstrap.

This script performs:
1) `databricks bundle deploy` for each requested industry (creates per-industry resources)
2) one `databricks bundle run ot_pdm_workspace_bootstrap_job` (seeds/backfills all industries)

The Databricks App is deployed through the bundle resource definition in `databricks.yml`.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

import yaml

DEFAULT_INDUSTRIES = ["mining", "energy", "water", "automotive", "semiconductor"]
QUICKSTART_INDUSTRY = "mining"


def _run(cmd: list[str]) -> None:
    print(f"[run] {' '.join(shlex.quote(c) for c in cmd)}")
    subprocess.run(cmd, check=True)


def _run_capture(cmd: list[str]) -> str:
    print(f"[run] {' '.join(shlex.quote(c) for c in cmd)}")
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return res.stdout


def _load_target_host(target: str) -> str | None:
    cfg_path = Path(__file__).resolve().parents[1] / "databricks.yml"
    if not cfg_path.exists():
        return None
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    return (
        cfg.get("targets", {})
        .get(target, {})
        .get("workspace", {})
        .get("host")
    )


def _resolve_profile(explicit_profile: str | None, target: str) -> str:
    if explicit_profile:
        return explicit_profile
    host = _load_target_host(target)
    if not host:
        return "DEFAULT"
    try:
        out = _run_capture(["databricks", "auth", "profiles", "--output", "json"])
        data = json.loads(out)
        for p in data.get("profiles", []):
            if str(p.get("host", "")).rstrip("/") == str(host).rstrip("/") and bool(p.get("valid")):
                name = str(p.get("name", "")).strip()
                if name:
                    print(f"[info] auto-selected profile '{name}' for host {host}")
                    return name
    except Exception:
        pass
    return "DEFAULT"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy DAB resources and bootstrap demo data.")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile name (auto-detected if omitted).")
    parser.add_argument("--target", default="dev", help="Bundle target (for example: dev/prod).")
    parser.add_argument(
        "--mode",
        choices=["quickstart", "full"],
        default="quickstart",
        help="quickstart=15-min path (single industry deploy + full bootstrap), full=deploy all industries.",
    )
    parser.add_argument(
        "--industries",
        default=",".join(DEFAULT_INDUSTRIES),
        help="Comma-separated industry list used when --mode full.",
    )
    parser.add_argument(
        "--skip-bootstrap",
        action="store_true",
        help="Only deploy resources; do not run workspace bootstrap job.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    profile = _resolve_profile(args.profile, args.target)
    if args.mode == "quickstart":
        industries = [QUICKSTART_INDUSTRY]
    else:
        industries = [i.strip().lower() for i in args.industries.split(",") if i.strip()]
    if not industries:
        print("[error] no industries provided")
        return 2

    _run(["databricks", "bundle", "validate", "--target", args.target, "-p", profile])

    for industry in industries:
        _run(
            [
                "databricks",
                "bundle",
                "deploy",
                "--target",
                args.target,
                "-p",
                profile,
                "--var",
                f"industry={industry}",
            ]
        )

    if not args.skip_bootstrap:
        # Bootstrap notebook already supports multi-industry seeding/backfill.
        _run(
            [
                "databricks",
                "bundle",
                "run",
                "ot_pdm_workspace_bootstrap_job",
                "--target",
                args.target,
                "-p",
                profile,
                "--var",
                f"industry={industries[0]}",
            ]
        )

    print("[ok] deployment workflow completed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(f"[error] command failed with exit code {exc.returncode}")
        raise SystemExit(exc.returncode) from exc
