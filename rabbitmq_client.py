"""RabbitMQ client for publishing Celery-protocol-v2 tasks.

Reusable wrapper around a pika BlockingConnection that knows the
celery-rmq-quorum topology and can publish to the work queue (direct path)
or to the corresponding quorum wait queue (delayed path, via per-message
AMQP TTL + DLX).

This module has no CLI / argparse / file-I/O surface — it's a pure client.
Callers compose it (see ``producer.py``).

Kept identical to ``app/rabbitmq_client.py`` so the host and container
producers share an implementation.
"""

from __future__ import annotations

import json
import socket
import sys
import uuid
from typing import Any

try:
    import pika
    from pika import BasicProperties
    from pika.exceptions import AMQPConnectionError
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "missing dependency: pika\n"
        "  install with:  pip install pika\n"
    )
    raise


# ─── topology — must stay in sync with rabbitmq/definitions.json
#                and the Kombu declarations in app/celery_app.py ─────────────
TASK_NAME = "send_notification"

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


def parse_broker_urls(broker_url: str) -> list["pika.URLParameters"]:
    """Parse a semicolon-separated AMQP URL string into pika params.

    Celery uses ``pyamqp://``; pika only understands ``amqp://`` — translate.
    A list is returned so the caller can fail over between hosts.
    """
    out: list[pika.URLParameters] = []
    for u in (x.strip() for x in broker_url.split(";")):
        if not u:
            continue
        if u.startswith("pyamqp://"):
            u = "amqp://" + u[len("pyamqp://"):]
        out.append(pika.URLParameters(u))
    if not out:
        raise ValueError(f"broker URL parsed to no usable URLs: {broker_url!r}")
    return out


def build_celery_v2_message(
    args: list[Any],
    kwargs: dict[str, Any],
    origin: str,
) -> tuple[bytes, dict, str]:
    """Build a Celery-protocol-v2 message: ``(body, headers, task_id)``.

    The body is the JSON-serialized ``[args, kwargs, embed]`` triple, where
    ``embed`` carries the callbacks/errbacks/chain/chord scaffolding.
    """
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
        "origin": origin,
        "ignore_result": False,
    }
    return body, headers, task_id


class RabbitMQClient:
    """Publishes Celery v2 task messages to the celery-rmq-quorum stack.

    Lifecycle: explicit ``connect()`` / ``close()`` or use as a context manager.

    Example::

        with RabbitMQClient("amqp://app:app@localhost:5672/") as client:
            client.publish("notifications", "alice@example.com", "hi")
            client.publish("jobs", "bob@example.com", "later", delay_ms=5000)
    """

    def __init__(self, broker_url: str, origin: str | None = None) -> None:
        self.broker_url = broker_url
        self.origin = origin or f"rabbitmq-client@{socket.gethostname()}"
        self._connection: "pika.BlockingConnection | None" = None
        self._channel: "pika.adapters.blocking_connection.BlockingChannel | None" = None

    # ─── context manager glue ────────────────────────────────────────────
    def __enter__(self) -> "RabbitMQClient":
        self.connect()
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    # ─── lifecycle ───────────────────────────────────────────────────────
    def connect(self) -> None:
        """Open a connection + channel, trying each host in turn.

        Enables publisher confirms on the channel so publishes raise on
        broker-side rejection (e.g. mandatory unroutable).
        """
        if self._connection and self._connection.is_open:
            return
        last_err: Exception | None = None
        for params in parse_broker_urls(self.broker_url):
            try:
                self._connection = pika.BlockingConnection(params)
                self._channel = self._connection.channel()
                self._channel.confirm_delivery()
                return
            except AMQPConnectionError as e:
                last_err = e
                print(f"[rabbitmq-client] connect failed to "
                      f"{params.host}:{params.port}: {e}", file=sys.stderr)
        raise ConnectionError(
            f"could not connect to any broker host: {last_err}"
        )

    def close(self) -> None:
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass
        finally:
            self._connection = None
            self._channel = None

    # ─── publish ─────────────────────────────────────────────────────────
    def publish(
        self,
        queue: str,
        recipient: str,
        message: str,
        delay_ms: int | None = None,
    ) -> str:
        """Publish one ``send_notification`` task. Returns the task id.

        ``queue`` selects the logical queue family (``notifications`` or
        ``jobs``). When ``delay_ms > 0``, the message is routed through the
        queue's quorum wait queue with that TTL; the broker dead-letters
        it into the work queue once the timer expires.
        """
        if queue not in QUEUE_CONFIGS:
            raise ValueError(
                f"unknown queue {queue!r}; choose from {list(QUEUE_CONFIGS)}"
            )
        if self._channel is None:
            self.connect()
        assert self._channel is not None  # for type checkers

        cfg = QUEUE_CONFIGS[queue]
        body, headers, task_id = build_celery_v2_message(
            [recipient, message], {}, self.origin
        )
        exchange = (cfg["wait_exchange"]
                    if delay_ms and delay_ms > 0
                    else cfg["direct_exchange"])
        props_kwargs: dict[str, Any] = dict(
            content_type="application/json",
            content_encoding="utf-8",
            headers=headers,
            correlation_id=task_id,
            reply_to="",
            delivery_mode=2,  # persistent — quorum queues require this
        )
        if delay_ms and delay_ms > 0:
            # AMQP per-message TTL: milliseconds as a string.
            props_kwargs["expiration"] = str(int(delay_ms))
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=cfg["routing_key"],
            body=body,
            properties=BasicProperties(**props_kwargs),
            mandatory=True,
        )
        return task_id
