"""Compare a candidate model version against the champion alias.

Promotion rule (defaults):
  MAE improves by >= 2.5%
  RMSE does not worsen by more than 2%
  F1(late>5) does not drop by more than 0.01

If the candidate passes, it becomes `champion`; otherwise it becomes
`challenger` and the existing champion stays.
"""

from __future__ import annotations

import argparse
import os

from mlflow import MlflowClient

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MLFLOW_MODEL_NAME", "trip_delay_lgbm")
MAE_IMPROVEMENT = float(os.environ.get("PROMOTE_MAE_IMPROVEMENT", "0.025"))
RMSE_TOLERANCE = float(os.environ.get("PROMOTE_RMSE_TOLERANCE", "0.02"))
F1_TOLERANCE = float(os.environ.get("PROMOTE_F1_TOLERANCE", "0.01"))


def metrics(client: MlflowClient, version: str) -> dict:
    mv = client.get_model_version(MODEL_NAME, version)
    run = client.get_run(mv.run_id)
    return run.data.metrics


def promotion_decision(cand: dict, champ: dict) -> dict:
    cand_mae = float(cand.get("mae", float("inf")))
    champ_mae = float(champ.get("mae", float("inf")))
    cand_rmse = float(cand.get("rmse", float("inf")))
    champ_rmse = float(champ.get("rmse", float("inf")))
    cand_f1 = float(cand.get("f1_late_5", 0.0))
    champ_f1 = float(champ.get("f1_late_5", 0.0))

    mae_ok = cand_mae <= champ_mae * (1.0 - MAE_IMPROVEMENT)
    rmse_ok = cand_rmse <= champ_rmse * (1.0 + RMSE_TOLERANCE)
    f1_ok = cand_f1 >= champ_f1 - F1_TOLERANCE
    return {
        "promote": mae_ok and rmse_ok and f1_ok,
        "mae_ok": mae_ok,
        "rmse_ok": rmse_ok,
        "f1_ok": f1_ok,
        "cand_mae": cand_mae,
        "champ_mae": champ_mae,
        "cand_rmse": cand_rmse,
        "champ_rmse": champ_rmse,
        "cand_f1": cand_f1,
        "champ_f1": champ_f1,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidate-version", required=True)
    ap.add_argument("--champion-alias", default="champion")
    ap.add_argument("--init-champion", action="store_true")
    args = ap.parse_args()

    client = MlflowClient(TRACKING_URI)
    cand = metrics(client, args.candidate_version)

    try:
        champ_mv = client.get_model_version_by_alias(MODEL_NAME, args.champion_alias)
        champ = metrics(client, champ_mv.version)
        champ_version = champ_mv.version
    except Exception:
        champ = None
        champ_version = None

    if champ is None:
        client.set_registered_model_alias(
            MODEL_NAME, args.champion_alias, args.candidate_version
        )
        print(
            f"NO_CHAMPION set {args.champion_alias}=v{args.candidate_version}",
            flush=True,
        )
        return

    decision = promotion_decision(cand, champ)
    print(
        f"candidate=v{args.candidate_version} champion=v{champ_version} "
        f"mae={decision['cand_mae']:.4f}/{decision['champ_mae']:.4f} "
        f"rmse={decision['cand_rmse']:.4f}/{decision['champ_rmse']:.4f} "
        f"f1={decision['cand_f1']:.4f}/{decision['champ_f1']:.4f} "
        f"checks(mae={decision['mae_ok']},rmse={decision['rmse_ok']},f1={decision['f1_ok']}) "
        f"promote={decision['promote']}",
        flush=True,
    )

    if decision["promote"]:
        client.set_registered_model_alias(
            MODEL_NAME, args.champion_alias, args.candidate_version
        )
        print(f"PROMOTED {args.champion_alias}=v{args.candidate_version}", flush=True)
    else:
        client.set_registered_model_alias(
            MODEL_NAME, "challenger", args.candidate_version
        )
        print(
            f"KEPT {args.champion_alias}=v{champ_version}; "
            f"challenger=v{args.candidate_version}",
            flush=True,
        )


if __name__ == "__main__":
    main()
