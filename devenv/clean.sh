#!/bin/bash
# Remove everything deploy.sh created: containers, volumes, network,
# images and the Zuul source checkout.
#
#   ./clean.sh                 asks before deleting
#   ./clean.sh -y              doesn't ask
#   ./clean.sh --keep-images   keep the images, so the next deploy is fast
#
# podman itself (and its machine) is left alone; to remove it too:
#   podman machine rm -f && brew uninstall podman-compose podman
set -eu
source "$(dirname "$0")/common.sh"

yes=0 keep_images=0
for arg in "$@"; do
    case $arg in
        -y|--yes) yes=1 ;;
        --keep-images) keep_images=1 ;;
        *) die "usage: $0 [-y] [--keep-images]" ;;
    esac
done

if ! podman info >/dev/null 2>&1; then
    say "podman isn't running, so there are no containers or images to remove."
    rm -rf "$ZUUL_SRC"
    exit 0
fi

printf '%sThis deletes:%s\n' "$B" "$R"
say "• the $PROJECT containers, their volumes (job logs, Kafka, database) and network"
if [ $keep_images = 0 ]; then
    say "• the demo images (about 8 GB):"
    for image in $BUILT_IMAGES $PULLED_IMAGES; do
        podman image exists "$image" && say "    $image"
    done
    say "• podman's unused volumes and untagged images (build leftovers)"
    [ -d "$ZUUL_SRC" ] && say "• the Zuul source in $PWD/$ZUUL_SRC"
fi
if [ $yes = 0 ]; then
    printf '\nContinue? [y/N] '
    read -r answer
    [ "$answer" = y ] || [ "$answer" = Y ] || { echo "Nothing deleted."; exit 0; }
fi

# Removing the containers with -v also removes their anonymous volumes
# (Kafka, ZooKeeper and MariaDB declare some), which compose down leaves.
containers=$(podman ps -aq --filter "label=io.podman.compose.project=$PROJECT")
[ -z "$containers" ] || podman rm -fv --depend $containers >/dev/null
compose down -v >/dev/null || true
ok "Containers, volumes and network removed"

if [ $keep_images = 0 ]; then
    for image in $BUILT_IMAGES $PULLED_IMAGES; do
        podman image exists "$image" && podman rmi -f "$image" >/dev/null
    done
    podman image prune -af --filter dangling=true >/dev/null || true
    podman volume prune -f >/dev/null || true
    rm -rf "$ZUUL_SRC"
    ok "Images and Zuul source removed"
fi
