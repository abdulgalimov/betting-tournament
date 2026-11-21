#!/bin/sh
set -eu

# Escape the queue name so the policy only matches that queue.
pattern=$(printf '%s' "$RABBITMQ_DEAD_LETTER_QUEUE" | sed 's/[][\\.^$*+?(){}|]/\\&/g')
attempt=0
until rabbitmq-diagnostics -q check_running >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    rabbitmq-diagnostics -q check_running
    exit 1
  fi
  sleep 1
done
rabbitmqctl -q set_policy --apply-to quorum_queues --priority 100 \
  "tournament-dlq-retention:$RABBITMQ_DEAD_LETTER_QUEUE" "\\A$pattern\\z" \
  '{"delivery-limit":-1}'
touch /tmp/tournament-dlq-policy-ready
