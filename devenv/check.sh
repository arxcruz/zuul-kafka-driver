#!/bin/bash
# Show the most recent builds recorded by Zuul.
set -eu
curl -sf 'http://localhost:9000/api/tenant/example-tenant/builds?limit=10' |
    python3 -c '
import json, sys
builds = json.load(sys.stdin)
if not builds:
    print("No builds yet")
for b in builds:
    print("{:<20} {:<18} {:<8} {:<8} {}".format(
        b["start_time"] or "", b["pipeline"], b["job_name"],
        b["result"] or "",
        b.get("project") or b.get("ref", {}).get("project")))
'
