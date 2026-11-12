#!/bin/bash
# Publish a message to the zuul-triggers topic.
#   trigger.sh [event] [project] [data-json]
#   trigger.sh build-requested
#   trigger.sh deploy-requested test1 '{"environment": "production"}'
set -eu
cd "$(dirname "$0")"
event=${1:-build-requested}
project=${2:-test1}
data=${3:-'{}'}
msg=$(printf '{"project": "%s", "event": "%s", "data": %s}' \
      "$project" "$event" "$data")
echo "Sending: $msg"
echo "$msg" | podman compose exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic zuul-triggers
