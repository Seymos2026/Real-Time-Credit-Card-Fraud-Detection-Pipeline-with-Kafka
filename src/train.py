"""
Offline training for the Credit Card Fraud model.

Run once before starting the streaming pipeline:

    python -m src.train

Outputs:
    models/fraud_model.joblib   -- pipeline with StandardScaler + classifier
    models/metrics.json         -- accuracy, precision, recall, F1, ROC-AUC

The dataset is the Kaggle "Credit Card Fraud Detection" set
(https://kaggle.com/datasets/mlg-ulb/creditcardfraud) -- 284,807 transactions
with 30 features (Time, V1..V28, Amount) and a binary Class label
(0 = legit, 1 = fraud). Fraud is ~0.17% of rows, so we use class_weight
to handle the imbalance.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from . import config

FEATURE_COLUMNS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]
TARGET_COLUMN = "Class"


def load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        sys.exit(
            f"\n[ERROR] Dataset not found at {path}.\n"
            "Download creditcard.csv from "
            "https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud "
            "and place it in the data/ folder.\n"
        )
    df = pd.read_csv(path)
    missing = [c for c in FEATURE_COLUMNS + [TARGET_COLUMN] if c not in df.columns]
    if missing:
        sys.exit(f"[ERROR] Missing expected columns in dataset: {missing}")
    return df


def evaluate(name: str, y_true, y_pred, y_proba) -> dict:
    metrics = {
        "model": name,
        "accuracy": accuracy_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0),
        "roc_auc": roc_auc_score(y_true, y_proba),
        "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
    }
    print(f"\n=== {name} ===")
    print(f"  accuracy : {metrics['accuracy']:.4f}")
    print(f"  precision: {metrics['precision']:.4f}")
    print(f"  recall   : {metrics['recall']:.4f}")
    print(f"  F1       : {metrics['f1']:.4f}")
    print(f"  ROC-AUC  : {metrics['roc_auc']:.4f}")
    print("  Classification report:")
    print(classification_report(y_true, y_pred, zero_division=0))
    return metrics


def main() -> None:
    print(f"Loading dataset from {config.DATA_PATH} ...")
    df = load_dataset(config.DATA_PATH)
    print(f"  rows={len(df):,}  fraud_rate={df[TARGET_COLUMN].mean():.4%}")

    X = df[FEATURE_COLUMNS].values
    y = df[TARGET_COLUMN].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    candidates = {
        "logistic_regression": Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegression(
                        max_iter=1000,
                        class_weight="balanced",
                        solver="liblinear",
                    ),
                ),
            ]
        ),
        "random_forest": Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    RandomForestClassifier(
                        n_estimators=120,
                        max_depth=12,
                        class_weight="balanced",
                        n_jobs=-1,
                        random_state=42,
                    ),
                ),
            ]
        ),
    }

    all_metrics = []
    best_name, best_pipe, best_f1 = None, None, -1.0
    for name, pipe in candidates.items():
        print(f"\nTraining {name} ...")
        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)
        y_proba = pipe.predict_proba(X_test)[:, 1]
        m = evaluate(name, y_test, y_pred, y_proba)
        all_metrics.append(m)
        if m["f1"] > best_f1:
            best_f1, best_name, best_pipe = m["f1"], name, pipe

    print(f"\nBest model by F1: {best_name} (F1={best_f1:.4f})")

    config.MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "pipeline": best_pipe,
        "feature_columns": FEATURE_COLUMNS,
        "model_name": best_name,
    }
    joblib.dump(artifact, config.MODEL_PATH)
    print(f"Saved model -> {config.MODEL_PATH}")

    metrics_path = config.MODEL_PATH.parent / "metrics.json"
    metrics_path.write_text(
        json.dumps(
            {"best_model": best_name, "all_models": all_metrics},
            indent=2,
            default=lambda o: float(o) if isinstance(o, np.floating) else o,
        )
    )
    print(f"Saved metrics -> {metrics_path}")


if __name__ == "__main__":
    main()
