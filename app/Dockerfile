# Build context for this image is the repo root (set in docker-compose.yml),
# so paths below are relative to the project root, not to app/.
FROM python:3.12-slim

WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Shared client lives at the repo root; copy it alongside the app modules
# so a single source of truth feeds both the host and container producers.
COPY rabbitmq_client.py .
COPY app/celery_app.py app/tasks.py app/producer.py app/entrypoint.sh ./
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
