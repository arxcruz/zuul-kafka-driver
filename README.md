# zuul-kafka-driver

An out-of-tree [Zuul](https://zuul-ci.org) driver that enqueues items into
pipelines when a message arrives on a Kafka topic. Zuul itself is not
modified.

Any system that can publish to Kafka (a build system, a release tool, a
bot) can start Zuul jobs. The repository also contains a complete demo
environment in podman, including a container-bundle scenario that re-runs
only the tests that failed.

## Quick start: run it locally

Everything runs in containers. The host only needs **podman** (with
**podman-compose**), **git** and **curl**.

```sh
# macOS
brew install podman podman-compose
# Fedora / RHEL
sudo dnf install podman podman-compose git curl
```

```sh
git clone https://github.com/arxcruz/zuul-kafka-driver.git
cd zuul-kafka-driver/devenv
./deploy.sh --open
```

`deploy.sh` does the following:

1. Starts the podman machine on macOS. If you have no machine, it creates one with 4 CPUs, 8 GB of RAM and a 60 GB disk.
2. On the first run, clones Zuul into `devenv/.zuul-src` and builds the Zuul images natively. This takes about 25 minutes.
3. Builds the images with this driver installed and starts everything.
4. Waits until Zuul is subscribed to Kafka.

Later runs take about 20 seconds. Then open:

| URL | What |
|-----|------|
| http://localhost:8080 | Demo UI: send Kafka messages with one click and watch the jobs |
| http://localhost:9000 | Zuul web (status, builds, live consoles) |
| http://localhost:8000 | Job logs |

Click **Send** next to "New build announced". Zuul runs `hello-kafka`,
which prints *hey mom! i ran with kafka!*. To try the bundle scenario, click
**Send** under "Announce a new bundle", wait for it to finish, then click
**Re-run failed** in the Bundles grid.

The demo is also reachable from **other computers on the same network**, which is handy when presenting from another machine. `deploy.sh` prints the URLs (for example `http://192.168.1.20:8080`). It uses the IP of the interface with the default route; set `DEMO_HOST` to override it, for example `DEMO_HOST=mybox.local ./deploy.sh`. The demo UI and the job-log links use that address. Kafka's host port (`29092`) is advertised as `localhost`, so external producers only work from this machine.

```sh
./demo.sh                  # guided walkthrough in the terminal (for presentations)
./stop.sh                  # pause: stop everything (and the podman machine), keep all data
./deploy.sh                # start again (~20 s), with build history intact
./clean.sh                 # remove containers, volumes, images and the Zuul source
./clean.sh --keep-images   # same, but keep the images for a fast redeploy
```

See [Demo environment](#demo-environment) for details.

## Why out of tree?

This driver was developed with the help of an AI coding assistant. The
Zuul project asks contributors not to submit substantial AI-generated code
(see the "AI-Generated Content" section of Zuul's
[README](https://opendev.org/zuul/zuul/src/branch/master/README.rst)), so
it's published as a separate package that plugs into an unmodified Zuul.
The hope is to see Kafka triggers in Zuul itself one day. This project is a
working prototype and a set of requirements for that.

## How it plugs into Zuul

Zuul hardcodes its driver list in
`zuul.lib.connections.ConnectionRegistry.__init__` and has no plugin
mechanism. This package works around that:

1. The wheel installs `zuul_kafka_driver.pth` into `site-packages`. Python
   runs it at startup, and it installs a tiny import hook. Zuul is not
   imported at this point.
2. When Zuul imports `zuul.lib.connections`, the hook wraps
   `ConnectionRegistry.__init__` so it also registers every driver
   advertised in the `zuul.drivers` entry point group. This package
   advertises `kafka = zuul_kafka.driver:KafkaDriver`.

Other packages can use the same `zuul.drivers` entry point group to add
more drivers.

Install the package into **every** Zuul component's environment (scheduler,
web, executor, merger, launcher). Each one parses `zuul.conf` and rejects
connections that use an unknown driver. Only the scheduler consumes from
Kafka.

```sh
pip install /path/to/zuul-kafka-driver        # must be a regular (non-editable) install
python -c "from zuul.lib.connections import ConnectionRegistry as R; print('kafka' in R().drivers)"
```

For a container image:

```dockerfile
FROM quay.io/zuul-ci/zuul-scheduler:latest
COPY . /tmp/zuul-kafka-driver
RUN pip install --no-cache-dir /tmp/zuul-kafka-driver && rm -rf /tmp/zuul-kafka-driver
```

## zuul.conf

```ini
[connection kafka]
driver=kafka
bootstrap_servers=kafka1:9092,kafka2:9092
topics=zuul-triggers
# optional
group_id=zuul                 ; shared by all schedulers, so each message is handled once
auto_offset_reset=latest      ; where a brand new consumer group starts
default_event=message         ; event type when the message has no "event" field
# Any rdkafka.* option is passed verbatim to librdkafka:
rdkafka.security.protocol=SASL_SSL
rdkafka.sasl.mechanism=PLAIN
rdkafka.sasl.username=zuul
rdkafka.sasl.password=secret
rdkafka.ssl.ca.location=/etc/ssl/certs/ca.pem
```

## Message format

The message value must be a JSON object:

```json
{
  "project": "org/my-project",
  "branch": "main",
  "event": "deploy-requested",
  "tenant": "example-tenant",
  "newrev": "3f2a...",
  "data": {"environment": "staging", "requested_by": "alice"}
}
```

| field     | required | meaning |
|-----------|----------|---------|
| `project` | yes      | Project name, short (`org/repo`) or canonical (`host/org/repo`). It must be in a tenant. |
| `branch`  | no       | Branch to run on. Defaults to the project's default branch. Cannot be combined with `ref`. |
| `ref`     | no       | A full ref, e.g. `refs/tags/v1.0`, instead of `branch`. Tags usually also need `newrev`. |
| `oldrev`/`newrev` | no | Specific revisions. Without `newrev`, the item uses the branch head, like the timer trigger. |
| `event`   | no       | Event type that trigger filters match on. Defaults to `default_event`. |
| `tenant`  | no       | Only send the event to this tenant. Otherwise it goes to every tenant that contains the project. |
| `data`    | no       | Arbitrary object. Trigger filters can match on it. |

Messages that are not valid JSON, fail the schema, or name an unknown
project, tenant or branch are logged and skipped. If dispatching a valid
message fails, for example because ZooKeeper is unavailable, it is retried 3
times and then skipped. Offsets are committed only after a message has been
processed.

### Separate items and the event id

Zuul treats an event as a duplicate when an item for the same project, ref
and revision is already in the pipeline, so a second message for the same
branch is dropped while the first one is still running. To test several
things side by side (for example one container bundle per message), give
each its own `ref`, such as `refs/bundles/<bundle id>`. Without `newrev`,
the job checks out the branch heads as usual, and Zuul's build history
can be filtered by that ref.

Jobs only receive the Zuul event id, not the message. The driver sets the
event id to `kafka:<topic>:<partition>:<offset>`, so a job can read the
whole message back from Kafka. `devenv/repos/zuul-config/playbooks/bundle/`
shows this: a dispatcher job reads the message and uses `zuul_return`
`child_jobs` to run all tests, only the ones that failed last time, or a
given list.

## Pipeline configuration

The trigger key is the connection name. Every attribute is optional and
accepts a regex or a list of regexes. Values in a list are ORed and different
attributes are ANDed.

```yaml
- pipeline:
    name: kafka-deploy
    manager: independent
    trigger:
      kafka:
        - event: deploy-requested
          topic: zuul-triggers
          key: ^prod-.*                  # Kafka message key
          data:                          # dotted paths into "data"; all must match
            environment: [staging, production]
            meta.priority: high
    success:
      mqtt: ...                          # any reporter; the kafka driver has none
```

Projects then add jobs to the pipeline as usual:

```yaml
- project:
    kafka-deploy:
      jobs:
        - deploy
```

Example producer:

```sh
echo '{"project":"org/my-project","event":"deploy-requested","data":{"environment":"staging"}}' \
  | kcat -P -b kafka1:9092 -t zuul-triggers -k prod-1
```

## Limitations

- The message `data` is used to match triggers but is not passed to jobs
  as variables. Zuul has no generic way to pass trigger-event data to jobs;
  read the message back from Kafka using the event id (see above).
- Because this package wraps a Zuul internal (`ConnectionRegistry.__init__`)
  and uses its driver APIs, a Zuul upgrade could break it. Run the tests
  against the new Zuul version before upgrading.
- An editable install (`pip install -e`) may not install the `.pth` file.
  Use a regular install for deployment.

## Demo environment

`devenv/` runs a complete Zuul with podman: ZooKeeper, MariaDB, Kafka, and
Zuul's scheduler, merger, executor and web. Local git repos stand in for a
code review system. It also includes a log server and a demo web UI for
publishing Kafka messages.

```sh
cd devenv
./deploy.sh          # build what's missing, start everything, wait until ready
./demo.sh            # guided presentation (runs deploy.sh first)
./clean.sh           # delete containers, volumes, images and the Zuul source
```

| URL | What |
|-----|------|
| http://localhost:8080 | Demo UI: send messages with one click and watch the jobs |
| http://localhost:9000 | Zuul web (status, builds, live consoles) |
| http://localhost:8000 | Job logs |

The tenant has three pipelines on project `test1`:

| Message | Pipeline | Jobs |
|---------|----------|------|
| `event: build-requested` | `kafka-build` | `hello-kafka` (~1 min, says *hey mom! i ran with kafka!*), `lint`, then `deploy-preview` |
| `event: build-broken` | `kafka-build-broken` | the same, but `hello-kafka` fails and `deploy-preview` is skipped |
| `event: deploy-requested`, `data.environment: production` | `kafka-deploy` | `hello-kafka`, then `deploy-production` (staging is filtered out) |

On project `bundles`, the `bundle-test` pipeline tests container bundles.
Each message names a bundle (`ref: refs/bundles/<id>`, `data.bundle_id`);
`bundle-dispatch` reads it and selects which of the 10 `bundle-*` tests run:

| `data` | Runs |
|--------|------|
| neither field | all 10 tests |
| `rerun: failed` | only tests whose latest result for this bundle isn't SUCCESS |
| `jobs: [...]` | exactly these tests |

The tests fail at random (`data.flaky`, default 0.3) to simulate network
problems. The UI's Bundles grid shows the latest result of each test per
bundle, with re-run buttons.

Only podman (with podman-compose), git and curl are needed on the host.
The first `deploy.sh` clones Zuul into `devenv/.zuul-src`, pinned to the
version the driver was developed against, and builds its images natively
(about 25 minutes; set `ZUUL_SRC` to use an existing checkout). Upstream
images are amd64 only, and under emulation the executor's bubblewrap can't
create namespaces. `clean.sh --keep-images` keeps the images for a fast
redeploy. `trigger.sh` and `check.sh` are command-line alternatives to
the UI. Kafka is also exposed on `localhost:29092`.

## Development

Runtime dependencies are declared in `pyproject.toml`: only
`confluent-kafka`, which Zuul images already include. The unit tests import
Zuul, so `requirements-test.txt` installs Zuul pinned to the version the
driver was developed against, plus pytest:

```sh
python3 -m venv .venv            # or: uv venv -p 3.12 .venv
.venv/bin/pip install -r requirements-test.txt .
.venv/bin/python -m pytest tests
```

The tests run on Linux and macOS. On macOS, Zuul's bubblewrap driver is
replaced by a fake, because it reads `/proc`.

## License

Apache License 2.0, see [LICENSE](LICENSE).
