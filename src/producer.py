"""
Kafka producer: streams creditcard.csv to the raw-transactions topic at ~1 row/sec.

Run:
    python -m src.producer
"""
from __future__ import annotations

import json
import signal
import sys
import time
import uuid
from pathlib import Path
from typing import Iterator

import pandas as pd
from confluent_kafka import Producer

from . import config

FEATURE_COLUMNS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]


def iter_rows(path: Path, max_rows: int) -> Iterator[dict]:
    """Yield one row dict at a time from the CSV without loading it all in memory."""
    if not path.exists():
        sys.exit(
            f"\n[ERROR] Dataset not found at {path}.\n"
            "Download creditcard.csv from Kaggle and place it in data/.\n"
        )
    reader = pd.read_csv(path, chunksize=1000)
    sent = 0
    for chunk in reader:
        for _, row in chunk.iterrows():
            payload = {col: float(row[col]) for col in FEATURE_COLUMNS}
            payload["label"] = int(row["Class"]) if "Class" in row else None
            payload["event_id"] = str(uuid.uuid4())
            payload["event_ts"] = time.time()
            yield payload
            sent += 1
            if max_rows and sent >= max_rows:
                return


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[producer] DELIVERY FAILED: {err}")
    # Successful deliveries stay quiet to keep the terminal readable.


def main() -> None:
    producer = Producer(
        {
            **config.confluent_kafka_config(),
            "client.id": "fraud-producer",
            "linger.ms": 50,
        }
    )

    stop = {"flag": False}

    def _shutdown(_signum, _frame):
        print("\n[producer] shutdown requested, flushing...")
        stop["flag"] = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    delay = config.PRODUCE_DELAY_SECONDS
    print(
        f"[producer] streaming {config.DATA_PATH} -> topic '{config.RAW_TOPIC}' "
        f"every {delay:.2f}s (max_rows={config.MAX_ROWS or 'all'})"
    )

    count = 0
    for payload in iter_rows(config.DATA_PATH, config.MAX_ROWS):
        if stop["flag"]:
            break
        key = payload["event_id"]
        producer.produce(
            topic=config.RAW_TOPIC,
            key=key,
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        count += 1
        if count % 10 == 0:
            print(
                f"[producer] sent {count} rows "
                f"(last amount=${payload['Amount']:.2f}, label={payload['label']})"
            )
        time.sleep(delay)

    producer.flush(10)
    print(f"[producer] done. total rows sent: {count}")


if __name__ == "__main__":
    main()
