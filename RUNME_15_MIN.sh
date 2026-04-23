#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[info] OT PdM 15-min quickstart"
echo "[info] deploying bundle + app + bootstrap (quickstart mode)"

python3 tools/deploy_bundle_and_bootstrap.py --mode quickstart "$@"
