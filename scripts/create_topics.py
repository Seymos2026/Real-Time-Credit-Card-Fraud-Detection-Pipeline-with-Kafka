"""
One-shot helper that creates the two topics this project uses.

You can also create them by hand in the Confluent Cloud UI:
    - raw-transactions       (6 partitions, 1 replica is fine on Basic)
    - fraud-predictions      (6 partitions)

Run:
    python -m scripts.create_topics
"""
from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka.admin import AdminClient, NewTopic

from src import config


def main() -> None:
    admin = AdminClient(config.confluent_kafka_config())
    topics = [
        NewTopic(config.RAW_TOPIC, num_partitions=6, replication_factor=3),
        NewTopic(config.PREDICTIONS_TOPIC, num_partitions=6, replication_factor=3),
    ]
    fs = admin.create_topics(topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"[admin] created topic '{topic}'")
        except Exception as exc:  # noqa: BLE001
            # Topic already exists is fine
            print(f"[admin] '{topic}' -> {exc}")


if __name__ == "__main__":
    main()
