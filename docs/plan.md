# Plan: Celery + RabbitMQ 4.x Cluster with Quorum Queues, HA Delayed Messages, and a pika Producer

_Date: 2026-05-26_

## Context

`celery-rmq-quorum/` is a greenfield project. The goal is a runnable, self-contained demo of a production-shaped Celery worker backed by a clustered RabbitMQ broker with:

- **3-node RabbitMQ 4.x cluster** (single Docker Compose stack) for broker HA and node failover.
- **Quorum queues** (Raft-based, replicated to all 3 nodes) — the canonical RabbitMQ 4.x replacement for classic mirrored queues, durable across node loss.
- **Two publish paths**, both terminating in the same quorum work queue + worker:
  - **Direct / immediate** — task lands in the work queue and is consumed right away.
  - **Delayed** — task waits in a **quorum waiting queue** with per-message TTL, then dead-letters into the work queue when its TTL expires. The delayed message itself is Raft-replicated across all 3 nodes during the wait.
- A **pika-based CLI producer** that constructs Celery-protocol-v2 messages directly and publishes them via AMQP. Supports single, repeated (`--count N`), and batch-from-file (`--from-file`).
- A **Celery worker** that processes the sample task `send_notification(recipient, message)` and writes the result as JSON to `./result/<queue_name>/<task_id>.json` on the host.

Outcome: `docker compose up -d --build` brings up the cluster + worker + an init container that grows quorum-queue membership. `docker compose run --rm producer ...` publishes one or many tasks. Each completed task leaves a JSON file on disk. Stopping any single RabbitMQ node — even mid-delay — leaves the system functional thanks to quorum.

## Design decisions

### Delay mechanism: DLX + per-message TTL (not the delayed-message-exchange plugin)

`rabbitmq_delayed_message_exchange` stores in-flight delayed messages in a **node-local** store on whichever node accepted the publish. If that node crashes during the delay window, the message is lost. Incompatible with the HA spirit of this project.

This plan uses **DLX + per-message TTL with a quorum waiting queue** instead. The delayed message lives in a real (Raft-replicated) queue for its entire wait, and is dead-lettered into the work queue when its TTL expires. Pure AMQP — no plugin needed.

### Producer: pika, not FastAPI

The producer is a CLI tool (`producer.py`) that uses `pika` directly. It constructs Celery-protocol-v2 message bodies + headers + properties and publishes them. This avoids the HTTP layer for environments where the producer is a script, a cron job, or another service, and it demonstrates that any AMQP client speaking Celery's protocol can drive the worker.

For the delayed path the producer sets the AMQP `expiration` property (milliseconds as a string) on the message itself, then publishes to the `notifications.wait` exchange. The broker handles the rest.

### Worker writes results to disk

Each successful `send_notification` writes a JSON record to `${RESULT_DIR}/${RESULT_QUEUE_NAME}/<task_id>.json`. The host directory `./result` is bind-mounted to `/app/result` in the worker container, so results are visible on the host immediately. The record includes whether the task arrived through the delayed path (via `x-death` header detection).

### Bootstrap: rabbit-init container

`definitions.json` loads on `rabbit1` startup, before `rabbit2`/`rabbit3` have joined the cluster, so both quorum queues are created with 1 member. The `rabbit-init` one-shot service (depends on all 3 nodes being healthy) writes the Erlang cookie file and runs `rabbitmq-queues grow rabbit@rabbit2 all` / `... rabbit@rabbit3 all` to bring both queues up to 3 members. The worker and producer `depends_on: rabbit-init: service_completed_successfully`, so by the time they connect, the topology is fully replicated.

## Architecture

```
                                     ┌──────────────────────────────────────────────────┐
                                     │  RabbitMQ cluster (rabbit1, rabbit2, rabbit3)    │
       ┌─ direct ───────────────────►│  ex: notifications.direct ─► notifications (Q)   │
       │  (no expiration)            │                                                  │
producer (pika CLI)                  │                          DLX on TTL expiry       │
       │                             │  ex: notifications.wait  ─► notifications.wait(Q)│
       └─ delayed ──────────────────►│   (AMQP expiration=ms    │                       │
          (basic_publish with        │    on the message)       ▼                       │
           expiration property)      │                       notifications (Q) ──► worker
                                     └──────────────────────────────────────────────────┘
                                                                       │
                                                                       ▼
                                                    ./result/notifications/<task_id>.json
```

Both queues are **quorum** (`x-queue-type: quorum`). After `rabbit-init` runs, both have members on all 3 nodes.

## Project layout

```
celery-rmq-quorum/
├── docker-compose.yml          # 3× rabbit + rabbit-init + worker + producer (profile=tools)
├── .env(.example)              # erlang cookie, default user/pass
├── README.md                   # quickstart + demo recipes
├── docs/plan.md                # this file
├── batches/sample.json         # example batch input
├── result/                     # worker writes task result JSON here (host bind mount)
├── rabbitmq/
│   ├── Dockerfile              # FROM rabbitmq:4-management
│   ├── rabbitmq.conf           # classic_config 3-node discovery, pause_minority, load_definitions
│   ├── enabled_plugins         # [rabbitmq_management]
│   └── definitions.json        # user, vhost, exchanges, quorum queues, bindings
└── app/
    ├── Dockerfile              # python:3.12-slim
    ├── requirements.txt        # celery, kombu, pika
    ├── celery_app.py           # broker URL + queue/exchange topology (Kombu)
    ├── tasks.py                # send_notification — writes ./result/<queue>/<id>.json
    ├── producer.py             # pika CLI
    └── entrypoint.sh           # ROLE=worker | ROLE=producer
```

## Detailed design

### RabbitMQ cluster (`rabbitmq/`)

`rabbitmq.conf`:

```
loopback_users.guest = false
listeners.tcp.default = 5672
management.tcp.port = 15672

cluster_formation.peer_discovery_backend = classic_config
cluster_formation.classic_config.nodes.1 = rabbit@rabbit1
cluster_formation.classic_config.nodes.2 = rabbit@rabbit2
cluster_formation.classic_config.nodes.3 = rabbit@rabbit3

cluster_partition_handling = pause_minority

management.load_definitions = /etc/rabbitmq/definitions.json
```

`definitions.json` declares:
- user `app` / password `app` (administrator)
- vhost `/`
- exchange `notifications.direct`, type `direct`, durable
- exchange `notifications.wait`, type `direct`, durable
- queue `notifications`, `x-queue-type: quorum`
- queue `notifications.wait`, `x-queue-type: quorum`, `x-dead-letter-exchange: notifications.direct`, `x-dead-letter-routing-key: notify`
- bindings: `notifications.direct` → `notifications` on `notify`; `notifications.wait` → `notifications.wait` on `notify`

Per-message TTL is set by the producer per send (AMQP `expiration` property), so each delayed task can carry its own delay value.

### rabbit-init (compose service)

Runs after all three `rabbit*` services are healthy. Steps:

1. Write `RABBITMQ_ERLANG_COOKIE` to `/var/lib/rabbitmq/.erlang.cookie` with correct ownership/mode.
2. `rabbitmqctl --node rabbit@rabbit1 await_startup` — defensive wait.
3. `rabbitmq-queues --node rabbit@rabbit1 grow rabbit@rabbit2 all`
4. `rabbitmq-queues --node rabbit@rabbit1 grow rabbit@rabbit3 all`
5. Print final queue state for log visibility.

Marked `restart: "no"` — one-shot. Worker and producer wait for `service_completed_successfully`.

### Worker (`app/celery_app.py`, `app/tasks.py`, `app/entrypoint.sh`)

`celery_app.py` declares the same exchanges and queues as `definitions.json` via Kombu, so the worker's view of the topology matches what's actually on the broker. Key settings:

```python
task_acks_late=True,
task_reject_on_worker_lost=True,
broker_connection_retry_on_startup=True,
worker_prefetch_multiplier=1,
worker_enable_remote_control=False,   # prevents pidbox declaring transient queues — see below
```

RabbitMQ 4.x denies declaration of `transient_nonexcl_queues` by default. The worker is launched with `--without-mingle --without-gossip --without-heartbeat` and `worker_enable_remote_control=False` to suppress every place Celery would otherwise declare a non-durable queue at startup.

`tasks.py` writes the result file:

```
${RESULT_DIR:-/app/result}/${RESULT_QUEUE_NAME:-notifications}/<task_id>.json
```

with fields: `task_id`, `task_name`, `queue`, `exchange`, `routing_key`, `via_delay` (detected from the `x-death` header), `args`, `processed_at`, `delivered_to`.

### Producer (`app/producer.py`)

CLI flags:

| Flag | Meaning |
|---|---|
| `--recipient` | task arg `recipient` |
| `--message` | task arg `message` |
| `--delay-ms` | if `>0`, publish to `notifications.wait` with this AMQP TTL |
| `--count N` | publish N copies (single message generator) |
| `--from-file PATH` | publish from a JSON array of `{recipient, message, delay_ms?}` objects |

Internals:

- Parses `CELERY_BROKER_URL` (semicolon-separated `pyamqp://...` list), translates `pyamqp://` → `amqp://` for pika, tries each host in order.
- Opens a `BlockingConnection`, gets one channel, enables publisher confirms (`channel.confirm_delivery()`).
- For each message: builds Celery-protocol-v2 (body = JSON `[args, kwargs, embed]`, headers per spec), sets `delivery_mode=2`, and `expiration=<delay_ms>` if delayed; calls `basic_publish` with `mandatory=True`.

### Compose

- `worker` and `producer` share the same image (`./app`) and dispatch on `ROLE`.
- `producer` is in profile `tools` so `docker compose up` doesn't start it; invoke via `docker compose run --rm producer ...`.
- `worker` mounts `./result:/app/result`. `producer` mounts `./batches:/app/batches:ro`.
- Multi-host broker URL: `pyamqp://app:app@rabbit1:5672;pyamqp://app:app@rabbit2:5672;pyamqp://app:app@rabbit3:5672//`.

## Verification

1. **Cluster forms + queues replicated**
   - `docker compose up -d --build`
   - `docker compose logs rabbit-init` prints both queues with `members: [rabbit@rabbit1, rabbit@rabbit2, rabbit@rabbit3]`.

2. **Direct publish**
   - `docker compose run --rm producer --recipient alice@example.com --message "hi"`
   - `ls result/notifications/` shows one new JSON file within ~1s.

3. **Delayed publish**
   - `docker compose run --rm producer --recipient alice@example.com --message "later" --delay-ms 5000`
   - The JSON file appears ~5s later. Its `via_delay` field is `true`.

4. **Batch publish (--count)**
   - `docker compose run --rm producer --count 25 --message "load"`
   - 25 new files in `result/notifications/`.

5. **Batch publish (--from-file)**
   - `docker compose run --rm producer --from-file /app/batches/sample.json`
   - 4 new files; the two delayed ones (3000 ms, 8000 ms) arrive on time, both `via_delay: true`.

6. **HA over node loss with in-flight delayed message**
   - Publish with `--delay-ms 30000`. Immediately `docker compose stop rabbit2`.
   - 30 s later, the result file still appears. Restart `rabbit2`, confirm 3 running.

## Out of scope

- HAProxy / external load balancer (client-side multi-host URL is enough for the demo).
- Result backend (the on-disk result file replaces it for this demo).
- Flower / monitoring UI (RabbitMQ management UI is sufficient).
- TLS, auth on the producer, periodic tasks (Celery beat), retries with backoff strategy.
