"""
Output consumer: read the fraud-predictions topic and print rows to the console.

Run:
    python -m src.consumer
"""
from __future__ import annotations

import json
import signal
from datetime import datetime

from colorama import Fore, Style, init as colorama_init
from confluent_kafka import Consumer, KafkaError

from . import config

colorama_init(autoreset=True)


def _fmt_header() -> str:
    return (
        f"{'TIME':<20}  {'FLAG':<5}  {'AMOUNT':>10}  "
        f"{'P(FRAUD)':>9}  {'ACTUAL':>6}  {'MODEL'}"
    )


def _fmt_row(row: dict) -> str:
    pred = int(row.get("prediction", 0))
    proba = float(row.get("fraud_probability", 0.0))
    amount = float(row.get("amount", 0.0))
    actual = row.get("actual_label")
    processed = row.get("processed_at", "")[:19].replace("T", " ")
    flag = f"{Fore.RED}FRAUD{Style.RESET_ALL}" if pred == 1 else f"{Fore.GREEN}OK   {Style.RESET_ALL}"

    if actual is not None:
        match = "OK" if int(actual) == pred else "MISS"
        actual_str = f"{int(actual)}({match})"
    else:
        actual_str = "-"

    return (
        f"{processed:<20}  {flag}  ${amount:>9.2f}  "
        f"{proba:>9.4f}  {actual_str:>6}  {row.get('model', '')}"
    )


def main() -> None:
    consumer = Consumer(
        {
            **config.confluent_kafka_config(),
            "group.id": "fraud-output-consumer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([config.PREDICTIONS_TOPIC])
    print(f"[consumer] subscribed to '{config.PREDICTIONS_TOPIC}'. Waiting for predictions...\n")
    print(_fmt_header())
    print("-" * 80)

    stop = {"flag": False}

    def _shutdown(_signum, _frame):
        print("\n[consumer] shutting down...")
        stop["flag"] = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    total = 0
    fraud = 0
    try:
        while not stop["flag"]:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[consumer] error: {msg.error()}")
                continue
            try:
                row = json.loads(msg.value())
            except Exception as exc:  # noqa: BLE001
                print(f"[consumer] bad message: {exc}")
                continue

            total += 1
            if int(row.get("prediction", 0)) == 1:
                fraud += 1
            print(_fmt_row(row))

            if total % 25 == 0:
                rate = fraud / total
                print(
                    f"--- processed={total}  flagged_fraud={fraud}  "
                    f"flag_rate={rate:.2%} ---"
                )
    finally:
        consumer.close()
        print(f"\n[consumer] done. processed={total}, flagged={fraud}")


if __name__ == "__main__":
    main()
