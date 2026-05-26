# celery-rmq-quorum

A self-contained Docker Compose demo of a **Celery 5.x worker** consuming from a **3-node RabbitMQ 4.x cluster** with **quorum queues** for high availability, and **delayed-message** support implemented as a quorum **waiting queue with per-message TTL + DLX** (so in-flight delayed messages are also Raft-replicated across all 3 nodes).

The producer is a **pika** CLI — it constructs Celery-protocol-v2 messages directly and publishes them via AMQP. Supports single send, repeated send (`--count`), and batch send from a JSON file. The worker writes each task's result as JSON into `./result/<queue_name>/<task_id>.json`.

See [docs/plan.md](docs/plan.md) for the full design and [docs/scenarios.md](docs/scenarios.md) for a complete end-to-end test playbook (sanity checks, batch tests, HA failover).

## Topology

```
                                        ┌──────────────────────────────────────────────────┐
                                        │  RabbitMQ cluster (rabbit1, rabbit2, rabbit3)    │
            ┌─ direct ──────────────────►│  ex: notifications.direct ─► notifications (Q)  │
            │   (no expiration)          │                                                  │
producer ───┤  pika                      │                          DLX on TTL expiry       │
  (CLI)     │                            │  ex: notifications.wait  ─► notifications.wait(Q)│
            └─ delayed ─────────────────►│    (per-msg AMQP         │                       │
               (AMQP expiration=ms)      │     expiration prop)     ▼                       │
                                        │                       notifications (Q) ──► worker ──► ./result/notifications/<id>.json
                                        └──────────────────────────────────────────────────┘
```

Both `notifications` and `notifications.wait` are declared as **quorum queues** (`x-queue-type: quorum`), Raft-replicated across all 3 nodes. The `rabbit-init` one-shot container grows the quorum-queue membership to all 3 nodes after the cluster forms.

## Quick start

Requires Docker + Docker Compose.

```bash
cp .env.example .env
docker compose up -d --build
```

Wait ~30 seconds for the cluster to form and `rabbit-init` to finish:

```bash
docker compose ps
docker compose logs rabbit-init        # should show 3-member quorum queues
docker exec rabbit1 rabbitmqctl cluster_status
```

Management UI: http://localhost:15672 (user `app` / password `app`).

## Publishing messages

There are two equivalent ways to publish:

| Where it runs | Script | When to use |
|---|---|---|
| Inside Docker | `app/producer.py` (via `docker compose run --rm producer …`) | No local Python setup; everything in the compose network |
| On the host | [producer.py](producer.py) at the repo root | Quick one-liners from your terminal; pipes/scripts; no compose overhead |

### Host-side producer (no Docker)

Requires Python 3 + pika installed on the host:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install pika
```

Then publish against `rabbit1`'s published port (`localhost:5672`):

```bash
python3 producer.py --recipient alice@example.com --message "hello"
python3 producer.py --count 100 --message "burst"
python3 producer.py --from-file batches/sample.json
python3 producer.py --recipient bob@example.com --message "later" --delay-ms 5000

# Override the broker URL (e.g. point at a remote cluster)
python3 producer.py --broker-url 'amqp://app:app@10.0.0.5:5672/' --message "..."
BROKER_URL='amqp://app:app@localhost:5672/' python3 producer.py --message "..."
```

### Docker-side producer

The `producer` service is in the `tools` profile so it doesn't auto-start. Invoke it via `docker compose run --rm`.

### Single message — direct

```bash
docker compose run --rm producer \
  --recipient alice@example.com \
  --message "hello direct"
```

### Single message — delayed

```bash
docker compose run --rm producer \
  --recipient alice@example.com \
  --message "ping me later" \
  --delay-ms 8000
```

### Batch — N copies of the same message

```bash
docker compose run --rm producer \
  --recipient load@example.com \
  --message "stress test" \
  --count 50
```

### Batch — from a JSON file

The `./batches` directory on the host is mounted read-only at `/app/batches` in the container. A sample is provided at [batches/sample.json](batches/sample.json):

```json
[
  { "recipient": "alice@example.com", "message": "welcome aboard" },
  { "recipient": "bob@example.com",   "message": "shipment dispatched" },
  { "recipient": "carol@example.com", "message": "reminder: payment due", "delay_ms": 3000 },
  { "recipient": "dave@example.com",  "message": "scheduled report",      "delay_ms": 8000 }
]
```

Send it:

```bash
docker compose run --rm producer --from-file /app/batches/sample.json
```

## Inspecting results

The worker writes one JSON file per completed task to `./result/<queue_name>/<task_id>.json` on the host. The queue name is currently always `notifications` (the worker's only consumed queue).

```bash
ls -la result/notifications/
cat result/notifications/<task_id>.json
```

A result file looks like:

```json
{
  "task_id": "5f0b...",
  "task_name": "send_notification",
  "queue": "notifications",
  "exchange": "notifications.direct",
  "routing_key": "notify",
  "via_delay": false,
  "args": { "recipient": "alice@example.com", "message": "welcome aboard" },
  "processed_at": "2026-05-26T12:30:14.123456+00:00",
  "delivered_to": "alice@example.com"
}
```

`via_delay: true` indicates the task arrived through the wait queue (TTL+DLX); `false` means it was published directly.

## HA demo: kill a node mid-delay

```bash
# Publish a 30-second delayed message
docker compose run --rm producer \
  --recipient ha@example.com --message "survive me" --delay-ms 30000

# Immediately stop one cluster node
docker compose stop rabbit2

# Wait the rest of the TTL; the worker still processes the task,
# because notifications.wait is a quorum queue replicated to all 3 nodes.
ls -la result/notifications/

# Bring it back
docker compose start rabbit2
docker exec rabbit1 rabbitmqctl cluster_status   # 3 running again
```

## Why this delay design (DLX + TTL) instead of `rabbitmq_delayed_message_exchange`?

The `rabbitmq_delayed_message_exchange` plugin keeps in-flight delayed messages in a **node-local** store on whichever node received the publish. If that node crashes before the timer fires, the message is lost. Using a second quorum queue with per-message TTL + DLX keeps the delayed message Raft-replicated for the entire wait, so any single-node loss is survivable.

## Files

```
celery-rmq-quorum/
├── docker-compose.yml          # 3× rabbit + rabbit-init + worker + producer
├── producer.py                 # host-runnable pika producer (no Docker)
├── .env.example
├── docs/plan.md                # full design doc
├── docs/scenarios.md           # end-to-end test playbook
├── batches/sample.json         # example batch payload
├── result/                     # task results land here (created on first run)
├── rabbitmq/
│   ├── Dockerfile              # FROM rabbitmq:4-management
│   ├── rabbitmq.conf           # cluster discovery, pause_minority, load_definitions
│   ├── enabled_plugins         # [rabbitmq_management]
│   └── definitions.json        # user, vhost, exchanges, quorum queues, bindings
└── app/
    ├── Dockerfile              # python:3.12-slim
    ├── requirements.txt        # celery, kombu, pika
    ├── celery_app.py           # broker URL + queue/exchange topology (Kombu)
    ├── tasks.py                # send_notification — writes ./result/<queue>/<id>.json
    ├── producer.py             # pika CLI: single, --count, --from-file, --delay-ms
    └── entrypoint.sh           # ROLE=worker | ROLE=producer
```

## Teardown

```bash
docker compose down            # stop containers, keep volumes
docker compose down -v         # also remove queue data (rabbit1/2/3_data volumes)
rm -rf result/notifications    # clean up result files
```
