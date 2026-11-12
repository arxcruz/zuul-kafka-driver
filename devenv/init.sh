#!/bin/bash
# One-shot setup shared by all containers:
#  - ZooKeeper TLS certs in /var/certs (Zuul requires TLS to ZooKeeper)
#  - bare git repos in /srv/git built from the directories in /repos
set -eu

if [ ! -e /var/certs/keystores/zk.pem ]; then
    /certs-tools/zk-ca.sh /var/certs zk
    # ZooKeeper and Zuul run as different users.
    chmod -R a+rX /var/certs
fi

for src in /repos/*/; do
    name=$(basename "$src")
    dest=/srv/git/$name
    [ -d "$dest" ] && continue
    work=$(mktemp -d)
    cp -r "$src". "$work"
    git -C "$work" init -q -b master
    git -C "$work" add -A
    git -C "$work" -c user.name=devenv -c user.email=devenv@example.com \
        commit -q -m "Initial commit"
    git clone -q --bare "$work" "$dest"
    rm -rf "$work"
    echo "Created $dest"
done
touch /srv/git/.ready
