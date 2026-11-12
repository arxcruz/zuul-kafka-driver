# Shared settings and helpers for deploy.sh, clean.sh and demo.sh.
# Source it; don't run it.

cd "$(dirname "${BASH_SOURCE[0]}")"

# Must match "name:" in compose.yaml.
PROJECT=zuul-kafka-demo

# Zuul is built from source, pinned to the version the driver was
# developed against.  Set ZUUL_SRC to use an existing checkout instead.
ZUUL_REPO=https://opendev.org/zuul/zuul
ZUUL_REF=708e1bb256ed88cb0af47f0169c9a146f1c0103b
ZUUL_SRC=${ZUUL_SRC:-.zuul-src}

UI=http://localhost:8080
ZUUL=http://localhost:9000/t/example-tenant
LOGS=http://localhost:8000

# The address other computers on the network use to reach the demo.
# Set DEMO_HOST to override (e.g. a hostname); defaults to the IP of the
# interface with the default route.
lan_ip() {
    if [ "$(uname)" = Darwin ]; then
        local iface
        iface=$(route -n get default 2>/dev/null | awk '/interface:/ {print $2}')
        [ -n "$iface" ] && ipconfig getifaddr "$iface" 2>/dev/null
    else
        ip route get 1.1.1.1 2>/dev/null |
            awk '{for (i = 1; i < NF; i++) if ($i == "src") print $(i + 1)}'
    fi
}
DEMO_HOST=${DEMO_HOST:-$(lan_ip || true)}
DEMO_HOST=${DEMO_HOST:-localhost}

# Images built for the demo, and images pulled to build or run it.
BUILT_IMAGES="localhost/zuul-kafka-dev:latest
localhost/zuul-kafka-executor-dev:latest
localhost/zuul:dev
localhost/zuul-executor:dev"
PULLED_IMAGES="quay.io/opendevmirror/zookeeper:latest
quay.io/opendevmirror/mariadb:latest
docker.io/apache/kafka:3.9.1
quay.io/opendevorg/python-base:3.14-trixie
quay.io/opendevorg/python-builder:3.14-trixie
quay.io/opendevmirror/node:22-trixie
quay.io/opendevmirror/golang:1.25-trixie
quay.io/zuul-ci/zuul-scheduler:latest
quay.io/zuul-ci/zuul-executor:latest"

if [ -t 1 ]; then
    B=$'\e[1m'; D=$'\e[2m'; R=$'\e[0m'; BLUE=$'\e[34m'; GREEN=$'\e[32m'
    RED=$'\e[31m'; YEL=$'\e[33m'; CYAN=$'\e[36m'
else
    B= D= R= BLUE= GREEN= RED= YEL= CYAN=
fi

compose() { podman compose "$@" 2>/dev/null; }
say() { printf '  %s\n' "$@"; }
ok() { printf '  %s✔ %s%s\n' "$GREEN" "$1" "$R"; }
die() { printf '%s%s%s\n' "$RED" "$1" "$R" >&2; exit 1; }
container() { echo "${PROJECT}_$1_1"; }

ensure_podman() {
    command -v podman >/dev/null ||
        die "podman is not installed (macOS: brew install podman podman-compose)"
    if ! podman info >/dev/null 2>&1; then
        if podman machine list --format '{{.Name}}' 2>/dev/null | grep -q .; then
            say "Starting the podman machine..."
            podman machine start >/dev/null
        else
            say "Creating a podman machine (4 CPUs, 8 GB RAM, 60 GB disk)..."
            podman machine init --cpus 4 --memory 8192 --disk-size 60 --now
        fi
    fi
    # Checked after the machine is up: "podman compose" needs it running.
    podman compose version >/dev/null 2>&1 ||
        die "podman compose is not available (macOS: brew install podman-compose)"
}

# Build localhost/zuul:dev and localhost/zuul-executor:dev with Zuul's own
# Dockerfile.  Upstream images are amd64 only, and under emulation the
# executor's bubblewrap can't create namespaces, so build natively.
ensure_zuul_images() {
    if podman image exists localhost/zuul:dev &&
       podman image exists localhost/zuul-executor:dev; then
        return
    fi
    if [ ! -f "$ZUUL_SRC/Dockerfile" ]; then
        say "Fetching Zuul source into $ZUUL_SRC..."
        git clone -q "$ZUUL_REPO" "$ZUUL_SRC"
        git -C "$ZUUL_SRC" checkout -q "$ZUUL_REF"
    fi
    say "Building the Zuul images (first run only, about 25 minutes)..."
    podman build --target zuul-executor -t localhost/zuul-executor:dev "$ZUUL_SRC"
    podman build --target zuul -t localhost/zuul:dev "$ZUUL_SRC"
}

# After a restart Kafka may still be rebalancing the "zuul" consumer group,
# and messages wait until it settles; don't report ready before that.
consumer_group_stable() {
    podman exec "$(container kafka)" /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:9092 --describe --group zuul --state 2>/dev/null |
        grep -q Stable
}

wait_ready() {
    # A restarted container keeps its old logs, so only look at new lines.
    local since=$1
    printf '  Waiting for Zuul to start'
    for _ in $(seq 1 150); do
        if podman logs --since "$since" "$(container scheduler)" 2>&1 |
               grep -q "Subscribed to Kafka topics" &&
           curl -sf http://localhost:9000/api/components 2>/dev/null |
               grep -q '"executor": *\[{[^]]*"state": *"running"' &&
           curl -sf "$UI/api/status" >/dev/null 2>&1 &&
           consumer_group_stable; then
            printf '\n'; ok "Zuul is up and subscribed to Kafka"; return
        fi
        printf '.'; sleep 4
    done
    printf '\n'
    die "Zuul did not become ready; check: podman logs $(container scheduler)"
}
