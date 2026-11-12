#!/bin/bash
# Usage: wait-for.sh host:port... -- command
# Waits for each TCP endpoint and for the git repos, then runs command.
while [ "$1" != "--" ]; do
    host=${1%:*}; port=${1#*:}; shift
    until (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
        echo "Waiting for $host:$port"; sleep 2
    done
done
shift
until [ -e /srv/git/.ready ]; do echo "Waiting for git repos"; sleep 2; done
exec "$@"
