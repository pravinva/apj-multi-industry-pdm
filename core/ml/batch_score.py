import os
import sys
from argparse import ArgumentParser
from pathlib import Path
from glob import glob

import mlflow
import pandas as pd

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
from core.ml.anomaly_model import OTPdMAnomalyModel
from core.ml.features import get_feature_matrix

def _resolve_industry() -> str:
    parser = ArgumentParser(add_help=False)
    parser.add_argument("--industry", default=None)
    args, _ = parser.parse_known_args()
    return args.industry or os.environ.get("INDUSTRY", "mining")


def _load_latest_model_uri(model_name: str) -> str | None:
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name = '{model_name}'")
    if not versions:
        return None
    latest = max(versions, key=lambda v: int(v.version))
    return f"models:/{model_name}/{latest.version}"


INDUSTRY = _resolve_industry()
config = load_config(INDUSTRY, config_root=str(REPO_ROOT / "industries"))
catalog = config["catalog"]


def score_all_assets(spark):
    results = []
    for asset in config["simulator"]["assets"]:
        eid = asset["id"]
        try:
            anomaly_name = f"{catalog}.models.ot_pdm_anomaly_{eid.lower()}"
            rul_name = f"{catalog}.models.ot_pdm_rul_{eid.lower()}"
            anomaly_uri = _load_latest_model_uri(anomaly_name)
            rul_uri = _load_latest_model_uri(rul_name)
            if not anomaly_uri or not rul_uri:
                continue
            anomaly_pipeline = mlflow.sklearn.load_model(anomaly_uri)
            rul_model = mlflow.sklearn.load_model(rul_uri)
        except Exception:
            continue

        x, _ = get_feature_matrix(spark, catalog, eid, n_hours=2)
        if x.empty:
            continue

        model = OTPdMAnomalyModel(eid)
        model.pipeline = anomaly_pipeline
        scores = model.score(x)
        ruls = rul_model.predict(x)
        i = -1
        results.append(
            {
                "equipment_id": eid,
                "prediction_timestamp": pd.Timestamp.utcnow(),
                "anomaly_score": float(scores[i]),
                "anomaly_label": "anomaly" if scores[i] > 0.5 else "normal",
                "rul_hours": float(ruls[i]),
                "predicted_failure_date": (
                    pd.Timestamp.utcnow() + pd.Timedelta(hours=float(ruls[i]))
                )
                if ruls[i] < 9000
                else None,
            }
        )

    if results:
        spark.createDataFrame(pd.DataFrame(results)).write.format("delta").mode("append").saveAsTable(
            f"{catalog}.gold.pdm_predictions"
        )


if __name__ == "__main__":
    score_all_assets(spark)  # noqa: F821
