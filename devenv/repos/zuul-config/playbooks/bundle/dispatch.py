"""Decide which bundle tests to run for a Kafka message.

  dispatch.py EVENT_ID KAFKA_BOOTSTRAP ZUUL_API PROJECT REF CHILD_JOBS_JSON

The kafka driver sets the Zuul event id to kafka:<topic>:<partition>:<offset>,
so the triggering message is read back from Kafka.  Its data may contain:

  bundle_id   the bundle under test (default: the last part of the ref)
  rerun       "all" (default) or "failed": only the jobs whose latest
              result for this bundle is not SUCCESS
  jobs        an explicit list of job names to run
  flaky       chance (0-1) that a test hits a "network problem" (demo only)

Prints a JSON object with child_jobs, bundle_id, flaky and a report.
"""

import json
import sys
import urllib.parse
import urllib.request

import confluent_kafka

# Results that don't say whether a job works for this bundle.
NOT_A_VERDICT = {'SKIPPED', 'RETRY', None}


def read_message(event_id, bootstrap):
    kind, topic, partition, offset = event_id.split(':')
    if kind != 'kafka':
        raise SystemExit('event %s did not come from Kafka' % event_id)
    consumer = confluent_kafka.Consumer({
        'bootstrap.servers': bootstrap,
        'group.id': 'zuul-bundle-dispatch',
        'enable.auto.commit': False,
    })
    try:
        consumer.assign([confluent_kafka.TopicPartition(
            topic, int(partition), int(offset))])
        for _ in range(20):
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                return json.loads(msg.value())
    finally:
        consumer.close()
    raise SystemExit('message %s not found in Kafka' % event_id)


def latest_results(zuul_api, project, ref):
    """Latest result per job for this bundle (newest builds come first)."""
    query = urllib.parse.urlencode(
        {'project': project, 'ref': ref, 'complete': 'true', 'limit': 500})
    with urllib.request.urlopen('%s/builds?%s' % (zuul_api, query),
                                timeout=10) as r:
        builds = json.load(r)
    results = {}
    for build in builds:
        if build['result'] not in NOT_A_VERDICT:
            results.setdefault(build['job_name'], build['result'])
    return results


def main():
    event_id, bootstrap, zuul_api, project, ref, child_jobs = sys.argv[1:]
    child_jobs = json.loads(child_jobs)
    message = read_message(event_id, bootstrap)
    data = message.get('data') or {}
    bundle_id = data.get('bundle_id') or ref.rsplit('/', 1)[-1]
    report = ['bundle %s, message %s' % (bundle_id, event_id)]

    if data.get('jobs'):
        selected = [j for j in child_jobs if j in data['jobs']]
        unknown = sorted(set(data['jobs']) - set(child_jobs))
        report.append('explicit list: %s' % ', '.join(selected))
        if unknown:
            report.append('ignoring unknown jobs: %s' % ', '.join(unknown))
    elif data.get('rerun') == 'failed':
        results = latest_results(zuul_api, project, ref)
        selected = [j for j in child_jobs if results.get(j) != 'SUCCESS']
        for job in child_jobs:
            report.append('%-18s last result %-8s -> %s' % (
                job, results.get(job, 'never ran'),
                'run again' if job in selected else 'skip'))
        if not selected:
            report.append('nothing failed, nothing to re-run')
    else:
        selected = child_jobs
        report.append('running all %d tests' % len(selected))

    print(json.dumps({
        'child_jobs': selected,
        'bundle_id': bundle_id,
        'flaky': float(data.get('flaky', 0.3)),
        'report': report,
    }))


if __name__ == '__main__':
    main()
