# Test Scenarios

End-to-end test playbook for the `celery-rmq-quorum` stack. Scenarios are ordered from quickest sanity checks to full HA failover tests. Every command is copy-paste-ready and assumes you're in the project root.

> **Note on the result folder.** The worker writes one JSON file per completed task to `./result/<queue_name>/<task_id>.json` on the host (bind-mounted from `/app/result` inside the container). The queue name is `notifications` for all scenarios here.

---

## Pre-flight

Bring the stack up and confirm both quorum queues have 3 members.

```bash
cd /Users/pankaj/Github/celery-rmq-quorum

# Optional: destroy queue data for a fresh slate
docker compose down -v

docker compose up -d --build

# rabbit-init runs once after the 3 nodes are healthy and grows the
# quorum-queue membership to all 3 nodes. Its log proves the topology.
docker compose logs rabbit-init | tail -10

# Worker should report "celery@... ready."
docker compose logs worker --tail 20

# Per-node health summary
docker compose ps
docker exec rabbit1 rabbitmqctl cluster_status
```

**Pass criteria**

- `docker compose ps` shows all `rabbit*` healthy, `worker` running, `rabbit_init` exited (0).
- `rabbit-init` log lists both `notifications` and `notifications.wait` with `members: [rabbit@rabbit1, rabbit@rabbit2, rabbit@rabbit3]`.
- Worker log ends with `celery@<hostname> ready.`

Management UI for visual debugging: <http://localhost:15672> (user `app`, password `app`).

---

## Scenario 1 — Direct publish (immediate)

```bash
# Clean result dir for a clear count
rm -rf result/notifications && mkdir -p result/notifications

docker compose run --rm producer \
  --recipient alice@example.com --message "direct test"

# Should land within ~1s
sleep 2 && ls result/notifications/ | wc -l            # → 1
cat result/notifications/*.json | jq '{via_delay, args}'
```

**Pass criteria.** Exactly 1 file appears; `via_delay: false`; `exchange: "notifications.direct"`.

---

## Scenario 2 — Delayed publish (single, 5s)

```bash
rm -rf result/notifications && mkdir -p result/notifications

date +%T && \
docker compose run --rm producer \
  --recipient bob@example.com --message "delayed test" --delay-ms 5000

# Peek mid-delay: the message should be sitting on notifications.wait
sleep 2
docker exec rabbit1 rabbitmqctl list_queues name messages messages_ready 2>&1 | grep notif

# Confirm the result file arrives ~5s after publish
sleep 5
ls result/notifications/ | wc -l                       # → 1
jq '{via_delay, processed_at, args}' result/notifications/*.json
```

**Pass criteria.** During the mid-delay peek, `notifications.wait` shows `messages: 1`. The resulting JSON has `via_delay: true` and `processed_at` ~5 s after the `date` line.

---

## Scenario 3 — Batch by `--count`

```bash
rm -rf result/notifications && mkdir -p result/notifications

# 100 identical direct messages
time docker compose run --rm producer \
  --count 100 --recipient load@example.com --message "burst"

sleep 3
ls result/notifications/ | wc -l                       # → 100

# Every file should be via_delay=false
grep -l '"via_delay": false' result/notifications/*.json | wc -l
```

**Pass criteria.** 100 files; all `via_delay: false`.

---

## Scenario 4 — Batch from file (mixed direct + delayed)

The repo ships [`batches/sample.json`](../batches/sample.json) with 4 messages: 2 direct, 2 delayed (3 s and 8 s).

```bash
rm -rf result/notifications && mkdir -p result/notifications

date +%T && \
docker compose run --rm producer --from-file /app/batches/sample.json

# Immediately: only the 2 non-delayed ones should be present
sleep 1
ls result/notifications/ | wc -l                       # → 2

# After ~4s total: carol (3s delay) arrives
sleep 3 && ls result/notifications/ | wc -l            # → 3

# After ~10s total: dave (8s delay) arrives
sleep 6 && ls result/notifications/ | wc -l            # → 4

# Should split 2-and-2
grep -l '"via_delay": true'  result/notifications/*.json | wc -l   # → 2
grep -l '"via_delay": false' result/notifications/*.json | wc -l   # → 2
```

**Pass criteria.** File count goes 2 → 3 → 4 as delays elapse; final result is exactly 2 direct + 2 delayed.

### Writing your own batch file

The host directory `./batches/` is mounted read-only at `/app/batches`. Drop a JSON array of `{recipient, message, delay_ms?}` objects:

```bash
cat > batches/mybatch.json <<'EOF'
[
  {"recipient": "x@y.com", "message": "now"},
  {"recipient": "x@y.com", "message": "in 10s", "delay_ms": 10000},
  {"recipient": "x@y.com", "message": "in 30s", "delay_ms": 30000}
]
EOF

docker compose run --rm producer --from-file /app/batches/mybatch.json
```

---

## Scenario 5 — HA: delayed message survives node loss (the headline test)

This is the test that proves the DLX + TTL design beats the `rabbitmq_delayed_message_exchange` plugin. With the plugin, the message would have been node-local and lost; with our quorum-replicated waiting queue, it survives.

```bash
rm -rf result/notifications && mkdir -p result/notifications

# Publish a 30-second delayed message
date +%T && \
docker compose run --rm producer \
  --recipient ha@example.com --message "survive node loss" --delay-ms 30000

# Immediately kill rabbit2 (mid-wait)
docker compose stop rabbit2

# Confirm rabbit2 is down but the cluster still has quorum (2/3 nodes)
docker exec rabbit1 rabbitmqctl cluster_status 2>&1 | grep -A4 "Running Nodes"

# Wait the rest of the TTL
sleep 32

# The result file should still appear — the delayed message lived on rabbit1
# and rabbit3 too, so rabbit2's loss didn't drop it.
ls result/notifications/                          # → exactly 1 file
jq '{via_delay, args}' result/notifications/*.json

# Bring rabbit2 back, confirm 3 running again
docker compose start rabbit2
sleep 10
docker exec rabbit1 rabbitmqctl cluster_status 2>&1 | grep -A4 "Running Nodes"
```

**Pass criteria.** The result file appears even though `rabbit2` was down for the entire delay window; `via_delay: true`. After restart, all 3 nodes show running.

---

## Scenario 6 — HA: publish while a node is down

```bash
rm -rf result/notifications && mkdir -p result/notifications

# Take down rabbit3
docker compose stop rabbit3

# Producer's broker URL lists all 3 nodes; pika tries each in order.
# Direct and delayed publishes should both still succeed.
docker compose run --rm producer --recipient z@x.com --message "node3 is down"
docker compose run --rm producer --recipient z@x.com --message "node3 is down + delay" --delay-ms 5000

sleep 7
ls result/notifications/ | wc -l                       # → 2

# Restore
docker compose start rabbit3
```

**Pass criteria.** Both result files appear; producer output shows no errors.

---

## Scenario 7 — Messages survive a rolling broker restart

This exercises queue durability — both `notifications` and `notifications.wait` are durable quorum queues, so messages persist across full broker restart.

```bash
rm -rf result/notifications && mkdir -p result/notifications

# Stop the worker so messages accumulate on the broker
docker compose stop worker

# Publish 10 direct + 10 delayed (15s TTL)
docker compose run --rm producer --count 10 --message "buffered direct"
docker compose run --rm producer --count 10 --message "buffered delayed" --delay-ms 15000

# Confirm they're sitting on the broker
docker exec rabbit1 rabbitmqctl list_queues name messages 2>&1 | grep notif
# Expect: notifications=10, notifications.wait=10

# Rolling restart of the cluster
docker compose restart rabbit1 rabbit2 rabbit3
sleep 15                # let cluster come back fully

# Re-check queues — messages should still be there
docker exec rabbit1 rabbitmqctl list_queues name messages 2>&1 | grep notif

# Start worker, watch it drain everything
docker compose start worker
sleep 25                # enough for the delayed batch to TTL + drain
ls result/notifications/ | wc -l                       # → 20
```

**Pass criteria.** All 20 messages eventually written to `result/notifications/`; nothing lost across the rolling broker restart.

---

## Scenario 8 — Worker crash mid-task (redelivery)

Exercises `task_acks_late=True` + `task_reject_on_worker_lost=True`: messages that were unacknowledged when the worker dies must be re-delivered by RabbitMQ, not lost.

```bash
rm -rf result/notifications && mkdir -p result/notifications

# Publish a burst, then kill the worker mid-flight
docker compose run --rm producer --count 50 --message "kill me"
docker compose kill worker            # ungraceful — unacked tasks must redeliver
sleep 2
docker compose start worker
sleep 5

# All 50 should eventually appear (re-delivered)
ls result/notifications/ | wc -l                       # → 50
```

**Pass criteria.** All 50 published tasks end up as result files. Some may run twice if they happened to be in-flight at the kill — that's expected at-least-once semantics. (To stress the kill-window, edit `app/tasks.py` to add `time.sleep(2)` before the file write, rebuild the worker, and re-run.)

---

## Useful inspection commands

```bash
# Cluster + queue snapshot
docker exec rabbit1 rabbitmqctl cluster_status
docker exec rabbit1 rabbitmqctl list_queues name type messages members
docker exec rabbit1 rabbitmqctl list_exchanges name type

# Watch the worker live
docker compose logs -f worker

# Result counts in real time
watch -n1 'ls result/notifications/ | wc -l'

# Inspect a single result
jq . result/notifications/<task_id>.json

# Filter just delayed-path tasks and show their processed_at
grep -l '"via_delay": true' result/notifications/*.json \
  | xargs -I{} jq -r '"\(.processed_at)  \(.args.recipient)"' {}
```

---

## Teardown

```bash
docker compose down            # stop containers, keep queue volumes
docker compose down -v         # also wipe the rabbit data volumes (clean slate)
rm -rf result/notifications    # clean up local result files
```
