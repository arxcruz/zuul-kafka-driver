"""Demo UI: publish Kafka messages and watch the Zuul jobs they trigger.

Serves index.html, publishes messages to Kafka and proxies the Zuul API
(so the browser doesn't need CORS).  Standard library + confluent_kafka.
"""

import collections
import http.server
import json
import os
import threading
import time
import urllib.request

import confluent_kafka

BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
TOPIC = os.environ.get('KAFKA_TOPIC', 'zuul-triggers')
ZUUL_API = os.environ.get(
    'ZUUL_API', 'http://web:9000/api/tenant/example-tenant')
HERE = os.path.dirname(os.path.abspath(__file__))

SCENARIOS = {
    'build': {
        'key': 'build',
        'value': {'project': 'test1', 'event': 'build-requested',
                  'data': {'build_id': None}},
    },
    'broken': {
        'key': 'build',
        'value': {'project': 'test1', 'event': 'build-broken',
                  'data': {'build_id': None}},
    },
    'production': {
        'key': 'deploy',
        'value': {'project': 'test1', 'event': 'deploy-requested',
                  'data': {'environment': 'production'}},
    },
    'staging': {
        'key': 'deploy',
        'value': {'project': 'test1', 'event': 'deploy-requested',
                  'data': {'environment': 'staging'}},
    },
    'unknown-project': {
        'key': 'build',
        'value': {'project': 'no-such-project', 'event': 'build-requested'},
    },
    'garbage': {
        'key': 'oops',
        'value': 'this is not json, mom',
    },
}

producer = confluent_kafka.Producer({'bootstrap.servers': BOOTSTRAP,
                                     'client.id': 'zuul-demo-ui'})
sent = collections.deque(maxlen=30)
sent_lock = threading.Lock()
build_counter = int(time.time()) % 10000

BUNDLE_PROJECT = 'bundles'
BUNDLE_PIPELINE = 'bundle-test'
BUNDLE_JOBS = ['bundle-networking', 'bundle-storage', 'bundle-auth',
               'bundle-api', 'bundle-upgrade', 'bundle-rollback',
               'bundle-scale', 'bundle-security', 'bundle-logging',
               'bundle-metrics']
# Results that don't say whether a job works for a bundle; the same rule
# as the bundle-dispatch job.
NOT_A_VERDICT = {'SKIPPED', 'RETRY', None}


def publish(key, value):
    payload = value if isinstance(value, str) else json.dumps(value)
    result = {}
    done = threading.Event()

    def delivered(err, msg):
        if err:
            result['error'] = str(err)
        else:
            result.update(partition=msg.partition(), offset=msg.offset())
        done.set()

    producer.produce(TOPIC, key=key, value=payload.encode('utf8'),
                     on_delivery=delivered)
    producer.flush(10)
    done.wait(10)
    entry = dict(time=time.time(), topic=TOPIC, key=key, value=payload,
                 **result)
    with sent_lock:
        sent.appendleft(entry)
    return entry


def zuul_get(path):
    with urllib.request.urlopen(ZUUL_API + path, timeout=5) as r:
        return json.load(r)


def elapsed_ms(job):
    # elapsed_time in the status API is a snapshot that only changes
    # when the build is updated; compute it from the start time instead.
    if not job.get('start_time'):
        return None
    if job.get('result'):
        return job.get('elapsed_time')
    return int((time.time() - job['start_time']) * 1000)


def summarize_status(status):
    """Flatten the Zuul status API into pipeline -> items -> jobs."""
    items = []
    for pipeline in status.get('pipelines', []):
        for queue in pipeline.get('change_queues', []):
            for head in queue.get('heads', []):
                for item in head:
                    refs = item.get('refs') or [item]
                    ref = refs[0]
                    jobs = []
                    for job in item.get('jobs', []):
                        jobs.append({
                            'name': job.get('name'),
                            'result': job.get('result'),
                            'started': bool(job.get('start_time')),
                            'elapsed': elapsed_ms(job),
                            'estimated': job.get('estimated_time') or None,
                            'waiting': job.get('waiting_status'),
                            'uuid': job.get('uuid'),
                        })
                    items.append({
                        'pipeline': pipeline['name'],
                        'project': (ref.get('project') or {}).get('name')
                        if isinstance(ref.get('project'), dict)
                        else ref.get('project'),
                        'ref': ref.get('ref'),
                        'enqueue_time': item.get('enqueue_time'),
                        'jobs': jobs,
                    })
    return items


def bundles():
    """Per bundle: the latest verdict of each job, plus what's running."""
    builds = zuul_get('/builds?project=%s&pipeline=%s&limit=1000' % (
        BUNDLE_PROJECT, BUNDLE_PIPELINE))
    out = {}

    def bundle(ref):
        return out.setdefault(ref, {
            'id': ref.rsplit('/', 1)[-1], 'ref': ref, 'busy': False,
            'runs': 0, 'last': '', 'jobs': {}})

    for b in builds:  # newest first
        ref = (b.get('ref') or {}).get('ref') if isinstance(
            b.get('ref'), dict) else b.get('ref')
        if not ref:
            continue
        info = bundle(ref)
        info['last'] = max(info['last'], b.get('start_time') or '')
        if b['job_name'] == 'bundle-dispatch':
            info['runs'] += 1
        elif b['result'] not in NOT_A_VERDICT:
            info['jobs'].setdefault(b['job_name'], {
                'state': b['result'], 'log': b.get('log_url'),
                'uuid': b['uuid']})

    for item in summarize_status(zuul_get('/status')):
        if item['pipeline'] != BUNDLE_PIPELINE:
            continue
        info = bundle(item['ref'])
        info['busy'] = True
        info['last'] = '9999'
        for j in item['jobs']:
            if j['name'] not in BUNDLE_JOBS or j['result'] == 'SKIPPED':
                continue
            if j['result'] is None:
                info['jobs'][j['name']] = {
                    'state': 'RUNNING' if j['started'] else 'QUEUED',
                    'uuid': j['uuid']}
            else:
                info['jobs'][j['name']] = {'state': j['result'],
                                           'uuid': j['uuid']}
    result = sorted(out.values(), key=lambda b: b['last'], reverse=True)
    return {'jobs': BUNDLE_JOBS, 'bundles': result[:8]}


def send_bundle(req):
    global build_counter
    action = req.get('action')
    if action == 'new':
        build_counter += 1
        bundle_id = 'b-%04d' % build_counter
    else:
        bundle_id = req['bundle_id']
        busy = [b for b in bundles()['bundles']
                if b['id'] == bundle_id and b['busy']]
        if busy:
            # Zuul would drop it as a duplicate of the running item.
            raise ValueError('%s is still running; wait for it to finish'
                             % bundle_id)
    data = {'bundle_id': bundle_id, 'flaky': float(req.get('flaky', 0.3))}
    if action == 'failed':
        data['rerun'] = 'failed'
    elif action == 'selected':
        data['jobs'] = list(req['jobs'])
    return publish(bundle_id, {
        'project': BUNDLE_PROJECT, 'event': 'bundle-test',
        'ref': 'refs/bundles/%s' % bundle_id, 'data': data})


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _json(self, data, code=200):
        body = json.dumps(data).encode('utf8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            if self.path in ('/', '/index.html'):
                with open(os.path.join(HERE, 'index.html'), 'rb') as f:
                    body = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == '/api/scenarios':
                self._json(SCENARIOS)
            elif self.path == '/api/sent':
                with sent_lock:
                    self._json(list(sent))
            elif self.path == '/api/status':
                self._json(summarize_status(zuul_get('/status')))
            elif self.path == '/api/bundles':
                self._json(bundles())
            elif self.path == '/api/builds':
                self._json(zuul_get('/builds?limit=12'))
            else:
                self._json({'error': 'not found'}, 404)
        except Exception as e:
            self._json({'error': str(e)}, 502)

    def do_POST(self):
        global build_counter
        try:
            length = int(self.headers.get('Content-Length', 0))
            req = json.loads(self.rfile.read(length) or b'{}')
            if self.path == '/api/bundle':
                return self._json(send_bundle(req))
            if self.path != '/api/send':
                return self._json({'error': 'not found'}, 404)
            if 'scenario' in req:
                scenario = json.loads(json.dumps(SCENARIOS[req['scenario']]))
                value = scenario['value']
                if isinstance(value, dict) and 'build_id' in value.get(
                        'data', {}):
                    build_counter += 1
                    value['data']['build_id'] = 'build-%04d' % build_counter
                entry = publish(scenario['key'], value)
            else:
                entry = publish(req.get('key') or 'custom', req['value'])
            self._json(entry)
        except Exception as e:
            self._json({'error': str(e)}, 400)


if __name__ == '__main__':
    print('Demo UI on http://0.0.0.0:8080 (Kafka %s, topic %s)' %
          (BOOTSTRAP, TOPIC), flush=True)
    http.server.ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
