"""
Start a DLT update for each deployed ot-pdm-dlt-<industry> pipeline.

Typical use: optional recovery or forced sync (job schedule is PAUSED when pipelines
run in continuous mode). Unpause the job in databricks.yml if you want a daily API kick.

Pipelines must already exist (bundle deploy per industry). Missing pipelines are skipped.

Run (cluster / job):
  python core/dlt/trigger_pipeline_updates.py
  python core/dlt/trigger_pipeline_updates.py --industries mining,energy
  python core/dlt/trigger_pipeline_updates.py --full-refresh
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

DEFAULT_INDUSTRIES = ("mining", "energy", "water", "automotive", "semiconductor")


def _repo_root() -> Path:
    candidates: list[Path] = []
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2])
    cwd = Path.cwd().resolve()
    candidates.extend([cwd, *cwd.parents])
    for root in candidates:
        if (root / "core" / "dlt").exists():
            return root
    return cwd


def _name_matches(pipeline_name: str | None, industry: str) -> bool:
    target = f"ot-pdm-dlt-{industry}".lower()
    name = str(pipeline_name or "").strip().lower()
    if not name:
        return False
    stripped = name
    if stripped.startswith("[") and "]" in stripped:
        stripped = stripped.split("]", 1)[-1].strip()
    return stripped == target or stripped.endswith(f" {target}") or target in stripped


def _resolve_pipeline_id(client: Any, industry: str) -> str | None:
    try:
        page_token = None
        for _ in range(0, 50):
            query: dict[str, str] = {"max_results": "100"}
            if page_token:
                query["page_token"] = page_token
            listing = client.api_client.do("GET", "/api/2.0/pipelines", query=query)
            items = listing.get("statuses", []) if isinstance(listing, dict) else []
            for p in items:
                pid = str(p.get("pipeline_id") or "").strip()
                if pid and _name_matches(p.get("name"), industry):
                    return pid
            page_token = listing.get("next_page_token") if isinstance(listing, dict) else None
            if not page_token:
                break
    except Exception as e:
        print(f"[dlt-trigger] list pipelines failed: {e}", file=sys.stderr)
    return None


def main() -> None:
    root = _repo_root()
    root_s = str(root)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)

    parser = argparse.ArgumentParser(description="Trigger DLT pipeline updates by industry")
    parser.add_argument(
        "--industries",
        default=",".join(DEFAULT_INDUSTRIES),
        help="Comma-separated industry keys (default: all five skins)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Pass full_refresh=true to the pipeline update (rebuild tables; heavier)",
    )
    args = parser.parse_args()

    industries = [i.strip().lower() for i in str(args.industries or "").split(",") if i.strip()]
    if not industries:
        industries = list(DEFAULT_INDUSTRIES)

    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    body: dict[str, Any] = {}
    if args.full_refresh:
        body["full_refresh"] = True
    body["cause"] = "scheduled ot-pdm-dlt-daily-refresh"

    ok_n = 0
    miss_n = 0
    err_n = 0
    for ind in industries:
        pid = _resolve_pipeline_id(client, ind)
        if not pid:
            print(f"[dlt-trigger] pipeline not found for industry={ind} (expected name ot-pdm-dlt-{ind})")
            miss_n += 1
            continue
        try:
            resp = client.api_client.do("POST", f"/api/2.0/pipelines/{pid}/updates", body=body)
            uid = (resp or {}).get("update_id") if isinstance(resp, dict) else None
            print(f"[dlt-trigger] started industry={ind} pipeline_id={pid} update_id={uid}")
            ok_n += 1
        except Exception as e:
            print(f"[dlt-trigger] FAILED industry={ind} pipeline_id={pid}: {e}", file=sys.stderr)
            err_n += 1

    print(f"[dlt-trigger] done ok={ok_n} missing={miss_n} errors={err_n}")
    if err_n:
        sys.exit(1)


if __name__ == "__main__":
    main()
