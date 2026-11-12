#!/bin/bash
# Bring up the Zuul + Kafka demo environment in podman.
#
#   ./deploy.sh          build what's missing, start everything, wait until ready
#   ./deploy.sh --open   also open the demo UI and Zuul in the browser
#
# The first run fetches Zuul's source and builds its images (about 25
# minutes); later runs start in under a minute.  ./clean.sh removes it all.
set -eu
source "$(dirname "$0")/common.sh"

ensure_podman
ensure_zuul_images
# Read by every job (zuul.conf [executor] variables); post-logs uses it to
# build log links that work from other computers.
printf 'demo_host: %s\n' "$DEMO_HOST" > etc_zuul/site-variables.yaml
say "Starting the containers..."
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
compose up -d --build >/dev/null
wait_ready "$started"

printf '\n'
say "${B}Demo UI${R}    $UI      send Kafka messages, watch jobs"
say "${B}Zuul${R}       $ZUUL/status"
say "${B}Job logs${R}   $LOGS"
say "${B}Kafka${R}      localhost:29092, topic zuul-triggers"
if [ "$DEMO_HOST" != localhost ]; then
    printf '\n'
    say "From other computers on the network:"
    say "  Demo UI   http://$DEMO_HOST:8080"
    say "  Zuul      http://$DEMO_HOST:9000/t/example-tenant/status"
    say "  Job logs  http://$DEMO_HOST:8000"
fi
printf '\n'
say "Guided walkthrough: ./demo.sh    Pause: ./stop.sh    Remove everything: ./clean.sh"

if [ "${1:-}" = "--open" ]; then
    open "$UI" 2>/dev/null || xdg-open "$UI" 2>/dev/null || true
    open "$ZUUL/status" 2>/dev/null || xdg-open "$ZUUL/status" 2>/dev/null || true
fi
