#!/bin/sh
set -e

case "$ROLE" in
  worker)
    exec celery -A celery_app worker \
      --loglevel=INFO \
      --concurrency=4 \
      -Q notifications,jobs \
      --without-mingle \
      --without-gossip \
      --without-heartbeat
    ;;
  producer)
    exec python producer.py "$@"
    ;;
  *)
    echo "ROLE must be 'worker' or 'producer' (got: '$ROLE')" >&2
    exit 1
    ;;
esac
