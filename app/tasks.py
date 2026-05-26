import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from celery_app import app

log = logging.getLogger("notify")

RESULT_BASE = Path(os.environ.get("RESULT_DIR", "/app/result"))

# Routing keys map to the worker's consumed queue, which is also the
# result-folder bucket. Keep this in sync with celery_app.py + definitions.json.
ROUTING_KEY_TO_QUEUE = {
    "notify": "notifications",
    "job": "jobs",
}
DEFAULT_QUEUE = "notifications"


@app.task(name="send_notification", bind=True, max_retries=3, default_retry_delay=5)
def send_notification(self, recipient: str, message: str) -> dict:
    delivery_info = self.request.delivery_info or {}
    headers = (self.request.headers or {}) if hasattr(self.request, "headers") else {}
    via_delay = "x-death" in headers
    routing_key = delivery_info.get("routing_key") or ""
    queue_name = ROUTING_KEY_TO_QUEUE.get(routing_key, DEFAULT_QUEUE)

    result = {
        "task_id": self.request.id,
        "task_name": self.name,
        "queue": queue_name,
        "exchange": delivery_info.get("exchange"),
        "routing_key": routing_key,
        "via_delay": via_delay,
        "args": {"recipient": recipient, "message": message},
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "delivered_to": recipient,
    }

    out_dir = RESULT_BASE / queue_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{self.request.id}.json"
    out_file.write_text(json.dumps(result, indent=2, default=str))

    log.info(
        "send_notification queue=%s recipient=%s task_id=%s via_delay=%s wrote=%s",
        queue_name,
        recipient,
        self.request.id,
        via_delay,
        out_file,
    )
    return result
