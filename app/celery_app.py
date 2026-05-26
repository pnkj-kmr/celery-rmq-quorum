import os

from celery import Celery
from kombu import Exchange, Queue

# Celery 5.5 auto-creates 28 `celery_delayed_<bucket>` quorum queues on worker
# boot as a fallback for ETA/countdown tasks on direct exchanges. We use
# AMQP TTL + DLX for delays instead, so the buckets are dead weight (28
# queues × 3 replicas of Raft state for nothing). Disable the bootstep
# before the Celery app is constructed.
try:
    from celery.worker.consumer.delayed_delivery import DelayedDelivery
    DelayedDelivery.start = lambda self, c: None
except Exception:
    pass

BROKER_URL = os.environ["CELERY_BROKER_URL"]

app = Celery("notify", broker=BROKER_URL, include=["tasks"])

# ─── notifications topology ─────────────────────────────────────────────
notifications_direct = Exchange("notifications.direct", type="direct", durable=True)
notifications_wait_ex = Exchange("notifications.wait", type="direct", durable=True)

notifications_queue = Queue(
    "notifications",
    exchange=notifications_direct,
    routing_key="notify",
    durable=True,
    queue_arguments={"x-queue-type": "quorum"},
)

notifications_wait_queue = Queue(
    "notifications.wait",
    exchange=notifications_wait_ex,
    routing_key="notify",
    durable=True,
    queue_arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "notifications.direct",
        "x-dead-letter-routing-key": "notify",
    },
)

# ─── jobs topology (same delay pattern, different name) ─────────────────
jobs_direct = Exchange("jobs.direct", type="direct", durable=True)
jobs_wait_ex = Exchange("jobs.wait", type="direct", durable=True)

jobs_queue = Queue(
    "jobs",
    exchange=jobs_direct,
    routing_key="job",
    durable=True,
    queue_arguments={"x-queue-type": "quorum"},
)

jobs_schedule_queue = Queue(
    "jobs_schedule",
    exchange=jobs_wait_ex,
    routing_key="job",
    durable=True,
    queue_arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "jobs.direct",
        "x-dead-letter-routing-key": "job",
    },
)

app.conf.update(
    task_queues=(
        notifications_queue,
        notifications_wait_queue,
        jobs_queue,
        jobs_schedule_queue,
    ),
    task_default_queue="notifications",
    task_default_exchange="notifications.direct",
    task_default_routing_key="notify",
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    worker_prefetch_multiplier=1,
    worker_enable_remote_control=False,
)
