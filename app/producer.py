"""CLI producer for the celery-rmq-quorum stack (in-container variant).

Thin shell over ``rabbitmq_client.RabbitMQClient``: handles argument
parsing, batch iteration, and human-readable output. All AMQP + Celery
protocol details live in ``rabbitmq_client``.

Usage examples (run via ``docker compose run --rm producer ...``)::

    --recipient alice@example.com --message "hello"
    --queue jobs --message "run my job"
    --queue jobs --message "scheduled" --delay-ms 5000
    --count 50 --message "burst"
    --from-file /app/batches/sample.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Iterable

from rabbitmq_client import QUEUE_CONFIGS, RabbitMQClient


def _iter_messages_from_file(path: str) -> Iterable[dict[str, Any]]:
    with open(path, "r") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SystemExit(f"{path}: expected a JSON array of message objects")
    for i, item in enumerate(data):
        if "recipient" not in item or "message" not in item:
            raise SystemExit(f"{path}: item #{i} missing recipient/message")
        yield item


def _iter_generated(
    count: int,
    recipient: str,
    message: str,
    delay_ms: int | None,
) -> Iterable[dict[str, Any]]:
    for i in range(count):
        msg = f"{message} #{i + 1}" if count > 1 else message
        yield {"recipient": recipient, "message": msg, "delay_ms": delay_ms}


def _broker_url_from_env() -> str:
    url = os.environ.get("CELERY_BROKER_URL")
    if not url:
        raise SystemExit("CELERY_BROKER_URL env var must be set")
    return url


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="producer.py",
        description="Publish Celery tasks via pika (single or batch).",
    )
    ap.add_argument(
        "--queue", choices=sorted(QUEUE_CONFIGS.keys()), default="notifications",
        help="target queue (default: notifications). 'jobs' uses the jobs/jobs_schedule pair.",
    )
    ap.add_argument("--recipient", default="user@example.com",
                    help="task arg: recipient")
    ap.add_argument("--message", default="hello from pika",
                    help="task arg: message body")
    ap.add_argument("--delay-ms", type=int, default=None,
                    help="if >0, route through the quorum wait queue with this TTL (ms)")
    ap.add_argument("--count", type=int, default=1,
                    help="number of identical messages to publish (ignored with --from-file)")
    ap.add_argument(
        "--from-file", type=str, default=None,
        help="JSON file with a list of {recipient, message, delay_ms?, queue?} objects",
    )
    args = ap.parse_args(argv)

    if args.count < 1:
        raise SystemExit("--count must be >= 1")

    if args.from_file:
        source = _iter_messages_from_file(args.from_file)
    else:
        source = _iter_generated(args.count, args.recipient, args.message, args.delay_ms)

    published = 0
    t0 = time.monotonic()
    with RabbitMQClient(_broker_url_from_env(), origin="pika-producer") as client:
        for item in source:
            queue = item.get("queue", args.queue)
            tid = client.publish(
                queue=queue,
                recipient=item["recipient"],
                message=item["message"],
                delay_ms=item.get("delay_ms"),
            )
            print(f"published task_id={tid} queue={queue} "
                  f"recipient={item['recipient']!r} delay_ms={item.get('delay_ms') or 0}")
            published += 1

    elapsed = time.monotonic() - t0
    rate = published / elapsed if elapsed > 0 else 0.0
    print(f"done. total published: {published} in {elapsed:.3f}s ({rate:.1f} msg/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
