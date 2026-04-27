import os
import sys
from argparse import ArgumentParser
from pathlib import Path

import mlflow
import pandas as pd

def _ensure_repo_root_on_path() -> Path:
    candidates: list[Path] = []
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parents[2])
    cwd = Path.cwd()
    candidates.extend([cwd, *cwd.parents])
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

mlflow.set_registry_uri("databricks-uc")

def _resolve_industry() -> str:
    parser = ArgumentParser(add_help=False)
    parser.add_argument("--industry", default=None)
    args, _ = parser.parse_known_args()
    return args.industry or os.environ.get("INDUSTRY", "mining")


def _load_model_uri_and_version(model_name: str) -> tuple[str | None, str | None]:
    client = mlflow.MlflowClient()
    # Prefer the explicit champion alias when available.
    try:
        champion = client.get_model_version_by_alias(model_name, "champion")
        if champion and getattr(champion, "version", None):
            return f"models:/{model_name}@champion", str(champion.version)
    except Exception:
        pass
    try:
        versions = client.search_model_versions(f"name = '{model_name}'")
    except Exception:
        versions = []
    if not versions:
        return None, None
    latest = max(versions, key=lambda v: int(v.version))
    return f"models:/{model_name}/{latest.version}", str(latest.version)


INDUSTRY = _resolve_industry()
config = load_config(INDUSTRY, config_root=str(REPO_ROOT / "industries"))
catalog = config["catalog"]


def score_all_assets(spark):
    results = []
    assets_seen = 0
    assets_with_features = 0
    assets_with_models = 0
    for asset in config["simulator"]["assets"]:
        assets_seen += 1
        eid = asset["id"]
        anomaly_pipeline = None
        rul_model = None
        try:
            anomaly_name = f"{catalog}.models.ot_pdm_anomaly_{eid.lower()}"
            rul_name = f"{catalog}.models.ot_pdm_rul_{eid.lower()}"
            anomaly_uri, anomaly_version = _load_model_uri_and_version(anomaly_name)
            rul_uri, rul_version = _load_model_uri_and_version(rul_name)
            if anomaly_uri:
                anomaly_pipeline = mlflow.sklearn.load_model(anomaly_uri)
            if rul_uri:
                rul_model = mlflow.sklearn.load_model(rul_uri)
        except Exception:
            anomaly_pipeline = None
            rul_model = None
            anomaly_version = None
            rul_version = None

        x, _ = get_feature_matrix(spark, catalog, eid, n_hours=2)
        if x.empty:
            continue
        assets_with_features += 1

        i = -1
        if anomaly_pipeline is None:
            # Do not synthesize scores; scoring is model-driven only.
            continue
        assets_with_models += 1
        model = OTPdMAnomalyModel(eid)
        model.pipeline = anomaly_pipeline
        scores = model.score(x)
        anomaly_score = float(scores[i])

        rul_hours = None
        if rul_model is not None:
            ruls = rul_model.predict(x)
            rul_hours = float(ruls[i])

        results.append(
            {
                "equipment_id": eid,
                "prediction_timestamp": pd.Timestamp.utcnow(),
                "anomaly_score": anomaly_score,
                "anomaly_label": "anomaly" if anomaly_score > 0.5 else "normal",
                "rul_hours": rul_hours,
                "predicted_failure_date": (
                    pd.Timestamp.utcnow() + pd.Timedelta(hours=float(rul_hours))
                )
                if rul_hours is not None and rul_hours < 9000
                else None,
                "model_version_anomaly": anomaly_version,
                "model_version_rul": rul_version,
            }
        )

    if results:
        spark.createDataFrame(pd.DataFrame(results)).write.format("delta").mode("append").saveAsTable(
            f"{catalog}.gold.pdm_predictions"
        )
        return

    raise RuntimeError(
        "No predictions written to gold.pdm_predictions. "
        f"assets_seen={assets_seen}, assets_with_features={assets_with_features}, "
        f"assets_with_models={assets_with_models}"
    )


if __name__ == "__main__":
    score_all_assets(spark)  # noqa: F821
