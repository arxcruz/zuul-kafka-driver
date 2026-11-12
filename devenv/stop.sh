#!/bin/bash
# Stop the demo without deleting anything, to free CPU and memory until
# the next ./deploy.sh (which starts it again in about a minute; images,
# build history and job logs are kept).
#
#   ./stop.sh                 stop the containers, and the podman machine
#                             if nothing else is running in it
#   ./stop.sh --keep-machine  only stop the containers
set -eu
source "$(dirname "$0")/common.sh"

keep_machine=0
for arg in "$@"; do
    case $arg in
        --keep-machine) keep_machine=1 ;;
        *) die "usage: $0 [--keep-machine]" ;;
    esac
done

if ! podman info >/dev/null 2>&1; then
    ok "podman isn't running; nothing to stop"
    exit 0
fi

say "Stopping the containers..."
# The scheduler goes first, while Kafka is still up, so its consumer can
# leave the group cleanly; otherwise Kafka holds the group for the old
# member's session timeout after the next start.
podman stop -t 30 "$(container scheduler)" >/dev/null 2>&1 || true
compose stop >/dev/null || true
ok "Containers stopped (images, volumes and build history kept)"

# On macOS/Windows podman runs in a VM; stop it too unless something
# else still uses it.
if [ $keep_machine = 0 ] && podman machine list --format '{{.Name}}' 2>/dev/null | grep -q .; then
    if [ -n "$(podman ps -q)" ]; then
        say "Other containers are still running, so the podman machine stays up."
    else
        say "Stopping the podman machine..."
        podman machine stop >/dev/null 2>&1 || true
        ok "Podman machine stopped"
    fi
fi
say "Start again with: ./deploy.sh"
