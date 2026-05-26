"""pika-based producer for Celery tasks on the celery-rmq-quorum stack.

Publishes Celery-protocol-v2 messages directly via pika so the worker can
consume them as normal Celery tasks. Supports single-shot publish, repeated
publish (--count), and batch publish from a JSON file (--from-file).

Two logical queues are wired:
  - notifications  (direct: notifications.direct,  wait: notifications.wait)
  - jobs           (direct: jobs.direct,           wait: jobs_schedule)

Direct path:  publish to <queue>.direct exchange (no expiration)
Delayed path: publish to <queue>.wait exchange with per-message AMQP
              expiration property; on TTL expiry the broker dead-letters
              into <queue>.direct → the work queue.

All queues are quorum so messages stay replicated for the entire wait.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import uuid
from typing import Any, Iterable

import pika
from pika import BasicProperties
from pika.exceptions import AMQPConnectionError

TASK_NAME = "send_notification"

# queue name -> exchange/routing-key triple
QUEUE_CONFIGS: dict[str, dict[str, str]] = {
    "notifications": {
        "direct_exchange": "notifications.direct",
        "wait_exchange": "notifications.wait",
        "routing_key": "notify",
    },
    "jobs": {
        "direct_exchange": "jobs.direct",
        "wait_exchange": "jobs.wait",
        "routing_key": "job",
    },
}


def _parse_broker_urls(env_value: str) -> list[pika.URLParameters]:
    """Parse a semicolon-separated CELERY_BROKER_URL into pika params.

    Celery accepts `pyamqp://`; pika only knows `amqp://` — translate.
    """
    out: list[pika.URLParameters] = []
    for u in (x.strip() for x in env_value.split(";")):
        if not u:
            continue
        if u.startswith("pyamqp://"):
            u = "amqp://" + u[len("pyamqp://"):]
        out.append(pika.URLParameters(u))
    if not out:
        raise SystemExit("CELERY_BROKER_URL parsed to no usable URLs")
    return out


def _open_connection() -> pika.BlockingConnection:
    env = os.environ.get("CELERY_BROKER_URL")
    if not env:
        raise SystemExit("CELERY_BROKER_URL env var must be set")
    last_err: Exception | None = None
    for params in _parse_broker_urls(env):
        try:
            return pika.BlockingConnection(params)
        except AMQPConnectionError as e:
            last_err = e
            print(f"[producer] connect failed to {params.host}: {e}", file=sys.stderr)
    raise SystemExit(f"[producer] could not connect to any broker host: {last_err}")


def _celery_v2_message(args: list[Any], kwargs: dict[str, Any]) -> tuple[bytes, dict, str]:
    """Build a Celery-protocol-v2 message body + headers. Returns (body, headers, task_id)."""
    task_id = str(uuid.uuid4())
    body = json.dumps(
        [args, kwargs, {"callbacks": None, "errbacks": None, "chain": None, "chord": None}]
    ).encode("utf-8")
    headers = {
        "lang": "py",
        "task": TASK_NAME,
        "id": task_id,
        "shadow": None,
        "eta": None,
        "expires": None,
        "group": None,
        "group_index": None,
        "retries": 0,
        "timelimit": [None, None],
        "root_id": task_id,
        "parent_id": None,
        "argsrepr": repr(tuple(args)),
        "kwargsrepr": repr(kwargs),
        "origin": f"pika-producer@{socket.gethostname()}",
        "ignore_result": False,
    }
    return body, headers, task_id


def publish_one(
    channel: "pika.adapters.blocking_connection.BlockingChannel",
    recipient: str,
    message: str,
    queue: str,
    delay_ms: int | None = None,
) -> str:
    cfg = QUEUE_CONFIGS[queue]
    body, headers, task_id = _celery_v2_message([recipient, message], {})
    exchange = cfg["wait_exchange"] if delay_ms and delay_ms > 0 else cfg["direct_exchange"]
    props_kwargs: dict[str, Any] = dict(
        content_type="application/json",
        content_encoding="utf-8",
        headers=headers,
        correlation_id=task_id,
        reply_to="",
        delivery_mode=2,  # persistent — quorum queues require this
    )
    if delay_ms and delay_ms > 0:
        # AMQP per-message TTL; string of milliseconds.
        props_kwargs["expiration"] = str(int(delay_ms))
    channel.basic_publish(
        exchange=exchange,
        routing_key=cfg["routing_key"],
        body=body,
        properties=BasicProperties(**props_kwargs),
        mandatory=True,
    )
    return task_id


def _iter_messages_from_file(path: str) -> Iterable[dict[str, Any]]:
    with open(path, "r") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SystemExit(f"{path}: expected a JSON array of message objects")
    for i, item in enumerate(data):
        if "recipient" not in item or "message" not in item:
            raise SystemExit(f"{path}: item #{i} missing recipient/message")
        yield item


def _iter_generated(count: int, recipient: str, message: str, delay_ms: int | None) -> Iterable[dict[str, Any]]:
    for i in range(count):
        msg = f"{message} #{i + 1}" if count > 1 else message
        yield {"recipient": recipient, "message": msg, "delay_ms": delay_ms}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="producer.py",
        description="Publish Celery tasks via pika (single or batch).",
    )
    ap.add_argument("--queue", choices=sorted(QUEUE_CONFIGS.keys()), default="notifications",
                    help="target queue (default: notifications). 'jobs' uses the jobs/jobs_schedule pair.")
    ap.add_argument("--recipient", default="user@example.com", help="task arg: recipient")
    ap.add_argument("--message", default="hello from pika", help="task arg: message body")
    ap.add_argument("--delay-ms", type=int, default=None,
                    help="if >0, route through the quorum wait queue with this TTL (ms)")
    ap.add_argument("--count", type=int, default=1,
                    help="number of identical messages to publish (ignored with --from-file)")
    ap.add_argument("--from-file", type=str, default=None,
                    help="JSON file with a list of {recipient, message, delay_ms?, queue?} objects")
    args = ap.parse_args(argv)

    if args.count < 1:
        raise SystemExit("--count must be >= 1")

    conn = _open_connection()
    try:
        ch = conn.channel()
        ch.confirm_delivery()

        if args.from_file:
            source = _iter_messages_from_file(args.from_file)
        else:
            source = _iter_generated(args.count, args.recipient, args.message, args.delay_ms)

        published = 0
        t0 = time.monotonic()
        for item in source:
            queue = item.get("queue", args.queue)
            if queue not in QUEUE_CONFIGS:
                raise SystemExit(f"unknown queue {queue!r}; choose from {list(QUEUE_CONFIGS)}")
            tid = publish_one(
                ch,
                recipient=item["recipient"],
                message=item["message"],
                queue=queue,
                delay_ms=item.get("delay_ms"),
            )
            print(f"published task_id={tid} queue={queue} recipient={item['recipient']!r} "
                  f"delay_ms={item.get('delay_ms') or 0}")
            published += 1

        elapsed = time.monotonic() - t0
        rate = published / elapsed if elapsed > 0 else 0.0
        print(f"done. total published: {published} in {elapsed:.3f}s ({rate:.1f} msg/s)")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
