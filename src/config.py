"""Centralised configuration loaded from environment variables / .env."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Resolve project root (one level above /src) and load .env from there.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _get(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Copy .env.example to .env and fill it in."
        )
    return value  # type: ignore[return-value]


# ----- Confluent Cloud / Kafka -----
KAFKA_BOOTSTRAP_SERVERS = _get("KAFKA_BOOTSTRAP_SERVERS", required=True)
KAFKA_SECURITY_PROTOCOL = _get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISMS = _get("KAFKA_SASL_MECHANISMS", "PLAIN")
KAFKA_SASL_USERNAME = _get("KAFKA_SASL_USERNAME", required=True)
KAFKA_SASL_PASSWORD = _get("KAFKA_SASL_PASSWORD", required=True)

# ----- Topics -----
RAW_TOPIC = _get("RAW_TOPIC", "raw-transactions")
PREDICTIONS_TOPIC = _get("PREDICTIONS_TOPIC", "fraud-predictions")

# ----- Pipeline knobs -----
PRODUCE_DELAY_SECONDS = float(_get("PRODUCE_DELAY_SECONDS", "1.0"))
MAX_ROWS = int(_get("MAX_ROWS", "0"))  # 0 = no cap
MODEL_PATH = PROJECT_ROOT / _get("MODEL_PATH", "models/fraud_model.joblib")
DATA_PATH = PROJECT_ROOT / _get("DATA_PATH", "data/creditcard.csv")


def confluent_kafka_config() -> dict:
    """Config dict for the ``confluent-kafka`` Producer / Consumer."""
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanisms": KAFKA_SASL_MECHANISMS,
        "sasl.username": KAFKA_SASL_USERNAME,
        "sasl.password": KAFKA_SASL_PASSWORD,
    }


def faust_broker_url() -> str:
    """Faust uses a URL form (no SASL keys here — those go via broker_credentials)."""
    return f"kafka://{KAFKA_BOOTSTRAP_SERVERS}"
