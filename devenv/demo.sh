#!/bin/bash
# Guided presentation of the Zuul Kafka driver.
#
#   ./demo.sh          start everything (via deploy.sh) and walk through the demo
#
# During the walkthrough, each step waits for Enter.  At a "send" prompt,
# press Enter to let the script publish the message, or click the same
# button in the demo UI and type "s" + Enter to skip sending.
set -eu
source "$(dirname "$0")/common.sh"

title() { printf '\n%s━━ %s ━━%s\n\n' "$B$BLUE" "$1" "$R"; }
pause() { printf '\n  %s[Enter]%s ' "$D" "$R"; read -r _ </dev/tty; }
open_url() { command -v open >/dev/null && open "$1" || xdg-open "$1" 2>/dev/null || true; }

scheduler_log_since() {
    podman logs --since "$1" "$(container scheduler)" 2>&1 | grep -E "$2" |
        sed -E 's/^[0-9-]+ ([0-9:]+),[0-9]+ /\1 /' | sed 's/^/    /' || true
}

send() {
    local scenario=$1 label=$2
    printf '\n  %sSend "%s"?%s Enter = send it, s = I clicked it in the UI ' \
        "$B" "$label" "$R"
    read -r ans </dev/tty
    if [ "$ans" != "s" ]; then
        curl -sf -X POST "$UI/api/send" -d "{\"scenario\": \"$scenario\"}" |
            python3 -c 'import json,sys; m=json.load(sys.stdin); print("  \033[36m→ published to %s, partition %s, offset %s\033[0m\n    %s" % (m["topic"], m["partition"], m["offset"], m["value"]))'
    fi
}

# Publish a bundle message (prompting like send); prints the bundle id.
send_bundle() {
    printf '\n  %sSend it?%s Enter = send it, s = I clicked it in the UI ' \
        "$B" "$R" >&2
    read -r ans </dev/tty
    if [ "$ans" = "s" ]; then
        curl -sf "$UI/api/bundles" |
            python3 -c 'import json,sys; print(json.load(sys.stdin)["bundles"][0]["id"])'
        return
    fi
    curl -sf -X POST "$UI/api/bundle" -d "$1" | python3 -c '
import json, sys
m = json.load(sys.stdin)
if m.get("error"):
    sys.exit("  " + m["error"])
print("  \033[36m→ published to %s, offset %s\033[0m\n    %s" % (m["topic"], m["offset"], m["value"]), file=sys.stderr)
print(m["key"])'
}

# Print the latest result of each job for a bundle.
show_bundle() {
    curl -sf "$UI/api/bundles" | python3 -c '
import json, sys
d = json.load(sys.stdin)
b = next((b for b in d["bundles"] if b["id"] == sys.argv[1]), None)
icons = {"SUCCESS": "\033[32m✔", "FAILURE": "\033[31m✘"}
cells = []
for job in d["jobs"]:
    state = (b["jobs"].get(job) or {}).get("state") if b else None
    cells.append("%s %s\033[0m" % (icons.get(state, "\033[2m·"), job[len("bundle-"):]))
for i in range(0, len(cells), 5):
    print("    " + "   ".join(cells[i:i + 5]))
' "$1"
}

# Show live job progress until no items are left in the given pipeline.
watch_pipeline() {
    local pipeline=$1 seen=0 empty=0
    printf '\n'
    for _ in $(seq 1 240); do
        local line
        line=$(curl -sf "$UI/api/status" | python3 -c '
import json, sys
pipeline = sys.argv[1]
items = [i for i in json.load(sys.stdin) if i["pipeline"] == pipeline]
icons = {"SUCCESS": "\033[32m✔", "FAILURE": "\033[31m✘", "SKIPPED": "\033[2m⤼"}
out = []
jobs = [j for item in items for j in item["jobs"]]
if len(jobs) > 4:
    # Too many for one line: count them instead.
    count = {}
    for j in jobs:
        state = j["result"] or ("running" if j["started"] else "queued")
        count[state] = count.get(state, 0) + 1
    names = [("running", "\033[34m▶"), ("queued", "\033[2m…"),
             ("SUCCESS", "\033[32m✔"), ("FAILURE", "\033[31m✘"),
             ("SKIPPED", "\033[2m⤼")]
    print("   ".join("%s %d %s\033[0m" % (icon, count[k], k.lower())
                     for k, icon in names if count.get(k)))
    sys.exit()
for item in items:
    for j in item["jobs"]:
        if j["result"]:
            icon = icons.get(j["result"], "\033[33m!") + " " + j["result"].lower()
        elif j["started"]:
            icon = "\033[34m▶ running %ds" % ((j["elapsed"] or 0) // 1000)
        else:
            icon = "\033[2m… queued"
        out.append("%s %s\033[0m" % (j["name"], icon))
print("   ".join(out))
' "$pipeline" 2>/dev/null || true)
        if [ -n "$line" ]; then
            seen=1; empty=0
            printf '\r\e[2K    %s' "$line"
        elif [ $seen = 1 ]; then
            empty=$((empty + 1))
            [ $empty -ge 2 ] && break
        fi
        sleep 2
    done
    printf '\n'
    if [ $seen = 0 ]; then
        say "${YEL}Nothing was enqueued in $pipeline.${R}"
    fi
}

show_last_builds() {
    curl -sf "$UI/api/builds" | python3 -c '
import json, sys
n = int(sys.argv[1])
for b in json.load(sys.stdin)[:n]:
    color = {"SUCCESS": "\033[32m", "FAILURE": "\033[31m"}.get(b["result"], "\033[2m")
    print("    %s%-8s\033[0m %-18s %-20s %s" % (color, b["result"], b["job_name"],
          b["pipeline"], b.get("log_url") or ""))
' "$1"
}

walkthrough() {
    clear
    title "Zuul × Kafka"
    say "Zuul normally reacts to Gerrit, GitHub or GitLab events."
    say "This driver lets ${B}any system that can publish to Kafka${R} trigger Zuul jobs:"
    say "a build system announcing a new build, a release tool, a chat bot..."
    say ""
    say "  ${CYAN}producer → Kafka topic zuul-triggers → kafka driver (zuul-scheduler)${R}"
    say "  ${CYAN}         → pipeline trigger filters → zuul-executor runs the jobs${R}"
    say ""
    say "It's an out-of-tree plugin: Zuul itself is unmodified."
    say "Opening the demo UI and the Zuul status page."
    open_url "$UI"
    open_url "$ZUUL/status"
    pause

    title "1. How Zuul is configured"
    say "zuul.conf only needs a connection:"
    printf '%s' "$D"; sed -n '/^\[connection kafka\]/,/^$/p' etc_zuul/zuul.conf | sed 's/^/      /'; printf '%s' "$R"
    say "and a pipeline uses it as a trigger:"
    printf '%s' "$D"; sed -n '/name: kafka-build$/,/build-requested/p' repos/zuul-config/zuul.d/pipelines.yaml | sed 's/^/      /'; printf '%s' "$R"
    say "A message is plain JSON:  ${B}{\"project\": \"test1\", \"event\": \"build-requested\"}${R}"
    pause

    title "2. Happy path: a new build is announced"
    say "kafka-build runs hello-kafka and lint in parallel."
    say "deploy-preview depends on hello-kafka, so it waits for it."
    say "Click a running job in the UI to see its live console."
    local since; since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    send build "New build announced"
    sleep 3
    say ""; say "Scheduler log:"
    scheduler_log_since "$since" "KafkaConnection|Reporting enqueue"
    watch_pipeline kafka-build
    show_last_builds 3
    local log_url
    log_url=$(curl -sf "$UI/api/builds" | python3 -c 'import json,sys; print(next((b["log_url"] for b in json.load(sys.stdin) if b["job_name"]=="hello-kafka" and b["log_url"]), ""))')
    if [ -n "$log_url" ]; then
        say ""; say "From the hello-kafka log:"
        curl -sf "${log_url}job-output.txt" | grep -m1 "hey mom" | sed "s/^/    $GREEN/;s/\$/$R/" || true
    fi
    pause

    title "3. A broken build"
    say "Same jobs, but the integration test fails at the end of hello-kafka."
    say "Watch: lint still passes, hello-kafka fails, deploy-preview is skipped."
    send broken "Broken build announced"
    watch_pipeline kafka-build-broken
    show_last_builds 3
    say ""
    say "To build it again, publish the message again: that's the Kafka"
    say "equivalent of a \"recheck\"."
    pause

    title "4. Filtering on message data"
    say "kafka-deploy only accepts deploy-requested messages where"
    say "${B}data.environment == production${R}. First, staging:"
    send staging "Deploy to staging"
    sleep 8
    if curl -sf "$UI/api/status" | grep -q kafka-deploy; then
        say "${YEL}Something was enqueued?!${R}"
    else
        ok "Nothing enqueued: the trigger filter dropped it"
    fi
    say ""; say "Now production:"
    send production "Deploy to production"
    watch_pipeline kafka-deploy
    show_last_builds 2
    pause

    title "5. Bad messages don't break anything"
    since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    send garbage "Garbage message"
    send unknown-project "Unknown project"
    sleep 4
    say ""; say "Scheduler log:"
    scheduler_log_since "$since" "KafkaConnection"
    ok "Logged, skipped, and the consumer keeps going"
    pause

    title "6. Container bundles: re-run only what failed"
    say "Each bundle has an ID.  The message carries it, and each bundle is"
    say "its own ref (refs/bundles/<id>), so bundles run side by side:"
    say ""
    say "  ${CYAN}{\"project\": \"bundles\", \"event\": \"bundle-test\","
    say "   \"ref\": \"refs/bundles/b-1234\", \"data\": {\"bundle_id\": \"b-1234\"}}${R}"
    say ""
    say "bundle-dispatch runs first.  The driver sets the Zuul event id to the"
    say "message's place in Kafka, so the job reads the message back and tells"
    say "Zuul which of the 10 tests to run (zuul_return child_jobs):"
    say "  data.rerun = failed  → only jobs whose last result isn't SUCCESS"
    say "  data.jobs = [...]    → exactly these"
    say "  neither              → all of them"
    say ""
    say "The tests fail at random, like a flaky network.  Watch the Bundles grid."
    bundle=$(send_bundle '{"action": "new", "flaky": 0.3}')
    [ -n "$bundle" ] && watch_pipeline bundle-test && show_bundle "$bundle"
    pause
    say "Now re-run only the failures (with less flakiness):"
    send_bundle "{\"action\": \"failed\", \"bundle_id\": \"$bundle\", \"flaky\": 0.1}" >/dev/null
    watch_pipeline bundle-test && show_bundle "$bundle"
    say ""
    say "Passed jobs were skipped (\"Skipped due to child_jobs return value\")."
    say "Re-run all or pick single jobs from the grid in the UI."
    pause

    title "7. Under the hood"
    say "Kafka consumer group 'zuul' (all schedulers share it, so each"
    say "message is handled once; offsets are committed after processing):"
    compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:9092 --describe --group zuul |
        awk 'NF {printf "    %-8s %-15s %-10s %-15s %-15s %s\n", $1, $2, $3, $4, $5, $6}'
    say ""
    say "The driver is loaded by a .pth hook, without changing Zuul:"
    compose exec -T scheduler python3 -c '
from zuul.lib.connections import ConnectionRegistry
import zuul_kafka
d = ConnectionRegistry().drivers["kafka"]
print("    drivers[\"kafka\"] = %s.%s" % (type(d).__module__, type(d).__name__))'
    pause

    title "Thanks!"
    if [ "$DEMO_HOST" != localhost ]; then
        say "Try it yourself: http://$DEMO_HOST:8080"
    else
        say "Try it yourself: $UI"
    fi
    say ""
    say "Next steps:"
    say "  • pass the message data (e.g. build_id) to jobs as variables"
    say "  • treat messages for different builds as different items"
    say "  • a Kafka reporter, so results go back to Kafka"
    say ""
}

./deploy.sh
walkthrough
