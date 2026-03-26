"""
Databricks job entrypoint for real Zerobus ingestion.

Example:
  python core/zerobus_ingest/run_connector.py --industry mining
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from glob import glob

def _ensure_repo_root_on_path() -> Path:
    candidates: list[Path] = []
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2])
    cwd = Path.cwd()
    candidates.extend([cwd, *cwd.parents])
    for p in glob("/Workspace/Users/*/.bundle/ot-pdm-intelligence/dev/files"):
        candidates.append(Path(p))
    for root in candidates:
        if (root / "core" / "config" / "loader.py").exists():
            root_s = str(root)
            if root_s not in sys.path:
                sys.path.insert(0, root_s)
            return root
    raise RuntimeError("Unable to locate bundle repo root for imports.")


REPO_ROOT = _ensure_repo_root_on_path()

from core.config.loader import load_config
from core.zerobus_ingest.connector import start_connector


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--industry", default=os.getenv("INDUSTRY", "mining"))
    args = parser.parse_args()

    cfg = load_config(args.industry, config_root=str(REPO_ROOT / "industries"))
    catalog = cfg["catalog"]
    return start_connector(
        args.industry,
        catalog,
        spark,  # noqa: F821
        config_root=str(REPO_ROOT / "industries"),
    )


if __name__ == "__main__":
    raise SystemExit(main())
