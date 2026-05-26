# celery-rmq-quorum — convenience targets
#
# Override variables on the command line, e.g.:
#   make notify-batch COUNT=500
#   make notify-delay DELAY_SHORT_MS=2000
#   make job RECIPIENT=alice@example.com

PRODUCER       ?= python3 producer.py
RECIPIENT      ?= user@example.com
COUNT          ?= 100
DELAY_SHORT_MS ?= 10000
DELAY_LONG_MS  ?= 30000

.DEFAULT_GOAL := help

.PHONY: help up down down-clean ps status logs-worker logs-init \
        notify notify-batch notify-delay notify-batch-delay \
        job job-batch job-delay job-batch-delay \
        all-notifications all-jobs all clean-results watch-results

help: ## list available targets
	@awk -F ':.*##' '/^[a-zA-Z][a-zA-Z0-9_-]*:.*##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─── stack lifecycle ──────────────────────────────────────────────────────
up: ## build images and start the cluster, worker, and rabbit-init
	docker compose up -d --build

down: ## stop containers (keeps broker volumes)
	docker compose down

down-clean: ## stop and wipe broker volumes (fresh slate)
	docker compose down -v

ps: ## list running services
	docker compose ps

status: ## show cluster status + per-queue membership
	docker exec rabbit1 rabbitmqctl cluster_status
	@echo
	docker exec rabbit1 rabbitmqctl list_queues name type messages members

logs-worker: ## tail worker logs
	docker compose logs -f worker

logs-init: ## show one-shot rabbit-init output
	docker compose logs rabbit-init

# ─── notifications ────────────────────────────────────────────────────────
notify: ## publish 1 direct message → notifications
	$(PRODUCER) --queue notifications --recipient $(RECIPIENT) \
		--message "notifications: single direct"

notify-batch: ## publish $(COUNT) direct messages → notifications
	$(PRODUCER) --queue notifications --recipient $(RECIPIENT) \
		--message "notifications: batch direct" --count $(COUNT)

notify-delay: ## publish 1 delayed message ($(DELAY_SHORT_MS)ms) → notifications.wait
	$(PRODUCER) --queue notifications --recipient $(RECIPIENT) \
		--message "notifications: single delayed $(DELAY_SHORT_MS)ms" \
		--delay-ms $(DELAY_SHORT_MS)

notify-batch-delay: ## publish $(COUNT) delayed messages ($(DELAY_LONG_MS)ms) → notifications.wait
	$(PRODUCER) --queue notifications --recipient $(RECIPIENT) \
		--message "notifications: batch delayed $(DELAY_LONG_MS)ms" \
		--count $(COUNT) --delay-ms $(DELAY_LONG_MS)

# ─── jobs ─────────────────────────────────────────────────────────────────
job: ## publish 1 direct message → jobs
	$(PRODUCER) --queue jobs --recipient $(RECIPIENT) \
		--message "jobs: single direct"

job-batch: ## publish $(COUNT) direct messages → jobs
	$(PRODUCER) --queue jobs --recipient $(RECIPIENT) \
		--message "jobs: batch direct" --count $(COUNT)

job-delay: ## publish 1 delayed message ($(DELAY_SHORT_MS)ms) → jobs_schedule
	$(PRODUCER) --queue jobs --recipient $(RECIPIENT) \
		--message "jobs: single delayed $(DELAY_SHORT_MS)ms" \
		--delay-ms $(DELAY_SHORT_MS)

job-batch-delay: ## publish $(COUNT) delayed messages ($(DELAY_LONG_MS)ms) → jobs_schedule
	$(PRODUCER) --queue jobs --recipient $(RECIPIENT) \
		--message "jobs: batch delayed $(DELAY_LONG_MS)ms" \
		--count $(COUNT) --delay-ms $(DELAY_LONG_MS)

# ─── bundles ──────────────────────────────────────────────────────────────
all-notifications: notify notify-batch notify-delay notify-batch-delay ## run all 4 notifications targets

all-jobs: job job-batch job-delay job-batch-delay ## run all 4 jobs targets

all: all-notifications all-jobs ## run every publish target

# ─── results ──────────────────────────────────────────────────────────────
clean-results: ## delete every result file (keeps the subdirs so the bind mount stays alive)
	find result -type f -name '*.json' -delete 2>/dev/null || true
	@echo "cleared result/*"

watch-results: ## live count of result files per queue
	@while true; do clear; \
	  echo "result/notifications: $$(ls result/notifications 2>/dev/null | wc -l)"; \
	  echo "result/jobs:          $$(ls result/jobs 2>/dev/null | wc -l)"; \
	  sleep 1; \
	done
