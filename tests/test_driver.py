import configparser
import json
import threading
from unittest import mock

import pytest
import voluptuous as v

from zuul.exceptions import ChangeNotFound

from zuul_kafka import _hook
from zuul_kafka.connection import InvalidMessage, KafkaConnection
from zuul_kafka.driver import KafkaDriver
from zuul_kafka.model import KafkaTriggerEvent


CONN_CONFIG = {
    'driver': 'kafka',
    'bootstrap_servers': 'localhost:9092',
    'topics': 'zuul-triggers, other',
}


class FakeMessage:
    def __init__(self, value, topic='zuul-triggers', key=b'k1',
                 partition=0, offset=42, error=None):
        if isinstance(value, dict):
            value = json.dumps(value).encode('utf8')
        self._value = value
        self._topic = topic
        self._key = key
        self._partition = partition
        self._offset = offset
        self._error = error

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error


def make_project(name='org/project', hostname='review.example.com'):
    project = mock.Mock()
    project.name = name
    project.canonical_hostname = hostname
    project.canonical_name = '%s/%s' % (hostname, name)
    project.source.getChangeKey.return_value = 'change-key'
    return project


def make_tenant(name, projects, default_branch='main'):
    tenant = mock.Mock()
    tenant.name = name

    def getProject(pname):
        for p in projects:
            if pname in (p.name, p.canonical_name):
                return True, p
        return None, None

    tenant.getProject.side_effect = getProject
    metadata = mock.Mock(default_branch=default_branch)
    tenant.layout.getProjectMetadata.return_value = metadata
    return tenant


@pytest.fixture
def project():
    return make_project()


@pytest.fixture
def sched(project):
    sched = mock.Mock()
    sched.statsd = None
    sched.abide.tenants = {
        'empty': make_tenant('empty', []),
        'tenant-one': make_tenant('tenant-one', [project]),
    }
    return sched


@pytest.fixture
def conn(sched):
    c = KafkaConnection(KafkaDriver(), 'kafka', dict(CONN_CONFIG))
    c.registerScheduler(sched)
    c.retry_delay = 0
    return c


# Hook / registration

def test_hook_registers_driver(registry):
    assert isinstance(registry.drivers['kafka'], KafkaDriver)
    assert registry.getTriggerEventClass('kafka') is KafkaTriggerEvent


def test_hook_patch_is_idempotent(registry):
    import zuul.lib.connections
    _hook.patch(zuul.lib.connections)
    _hook.install()
    registry.__class__()  # would raise "already registered" if doubled


def test_registry_configures_connection(registry):
    config = configparser.ConfigParser()
    config.read_dict({'connection kafka': CONN_CONFIG})
    registry.configure(config, triggers=True)
    conn = registry.connections['kafka']
    assert isinstance(conn, KafkaConnection)
    assert conn.topics == ['zuul-triggers', 'other']
    trigger = registry.getTrigger('kafka', {})
    assert trigger.driver is registry.drivers['kafka']


def test_registry_skips_connection_without_triggers(registry):
    # Executors/mergers don't load trigger connections, but the driver
    # must still be known so zuul.conf validates.
    config = configparser.ConfigParser()
    config.read_dict({'connection kafka': CONN_CONFIG})
    registry.configure(config, sources=True)
    assert 'kafka' not in registry.connections


# Connection config

def test_missing_topics():
    from zuul.exceptions import ConfigurationError
    with pytest.raises(ConfigurationError):
        KafkaConnection(KafkaDriver(), 'kafka',
                        {'bootstrap_servers': 'localhost:9092'})


def test_consumer_config():
    config = dict(CONN_CONFIG)
    config['group_id'] = 'my-group'
    config['rdkafka.security.protocol'] = 'SASL_SSL'
    c = KafkaConnection(KafkaDriver(), 'kafka', config)
    cc = c.getConsumerConfig()
    assert cc['bootstrap.servers'] == 'localhost:9092'
    assert cc['group.id'] == 'my-group'
    assert cc['enable.auto.commit'] is False
    assert cc['security.protocol'] == 'SASL_SSL'
    assert not any(k.startswith('rdkafka.') for k in cc)


def test_onload_without_scheduler():
    c = KafkaConnection(KafkaDriver(), 'kafka', dict(CONN_CONFIG))
    c.onLoad(None, None)
    assert c.consumer_thread is None


# Message handling

def test_handle_message(conn, sched, project):
    msg = FakeMessage({'project': 'org/project', 'branch': 'stable',
                       'event': 'deploy', 'data': {'env': 'prod'}})
    conn._handleMessage(msg)

    sched.addTriggerEvent.assert_called_once()
    driver_name, event = sched.addTriggerEvent.call_args.args
    assert driver_name == 'kafka'
    assert event.type == 'deploy'
    assert event.connection_name == 'kafka'
    assert event.canonical_project_name == project.canonical_name
    assert event.branch == 'stable'
    assert event.ref == 'refs/heads/stable'
    assert event.newrev is None
    assert event.data == {'env': 'prod'}
    assert (event.topic, event.partition, event.offset, event.key) == (
        'zuul-triggers', 0, 42, 'k1')
    assert event.zuul_event_id == 'kafka:zuul-triggers:0:42'
    project.source.getChange.assert_called_once_with(
        'change-key', refresh=True, event=event)


def test_handle_message_defaults(conn, sched):
    conn._handleMessage(FakeMessage({'project': 'org/project'}))
    event = sched.addTriggerEvent.call_args.args[1]
    assert event.type == 'message'
    assert event.branch == 'main'
    assert event.data == {}


def test_handle_message_tag_ref(conn, sched):
    conn._handleMessage(FakeMessage({
        'project': 'review.example.com/org/project',
        'ref': 'refs/tags/1.0', 'newrev': 'abc'}))
    event = sched.addTriggerEvent.call_args.args[1]
    assert event.ref == 'refs/tags/1.0'
    assert event.branch is None
    assert event.newrev == 'abc'


def test_handle_message_explicit_tenant(conn, sched):
    sched.trigger_events = mock.MagicMock()
    conn._handleMessage(FakeMessage({'project': 'org/project',
                                     'tenant': 'tenant-one'}))
    sched.addTriggerEvent.assert_not_called()
    queue = sched.trigger_events.__getitem__
    queue.assert_called_once_with('tenant-one')
    queue.return_value.put.assert_called_once()


@pytest.mark.parametrize('value', [
    None,
    b'not json',
    b'[1, 2]',
    {'branch': 'main'},
    {'project': 'org/project', 'branch': 'a', 'ref': 'refs/heads/a'},
    {'project': 'org/project', 'data': 'string'},
    {'project': 'org/unknown'},
    {'project': 'org/project', 'tenant': 'nope'},
    {'project': 'org/project', 'tenant': 'empty'},
])
def test_invalid_messages(conn, value):
    with pytest.raises(InvalidMessage):
        conn._handleMessage(FakeMessage(value))


def test_unknown_branch(conn, project):
    project.source.getChange.side_effect = ChangeNotFound(1, 1)
    with pytest.raises(InvalidMessage):
        conn._handleMessage(FakeMessage({'project': 'org/project',
                                         'branch': 'nope'}))


def test_process_message_retries(conn, sched):
    sched.addTriggerEvent.side_effect = [Exception('zk down'), None]
    conn._processMessage(FakeMessage({'project': 'org/project'}))
    assert sched.addTriggerEvent.call_count == 2


def test_process_message_gives_up(conn, sched):
    sched.addTriggerEvent.side_effect = Exception('zk down')
    conn._processMessage(FakeMessage({'project': 'org/project'}))
    assert sched.addTriggerEvent.call_count == conn.max_attempts


def test_consume_commits_and_stops(conn, sched):
    messages = [None,
                FakeMessage({'project': 'org/project'}),
                FakeMessage(b'garbage')]
    consumer = mock.Mock()

    def poll(timeout):
        if messages:
            return messages.pop(0)
        conn._stopped.set()
        return None

    consumer.poll.side_effect = poll
    conn.consumer_factory = mock.Mock(return_value=consumer)
    conn._consume()

    consumer.subscribe.assert_called_once_with(['zuul-triggers', 'other'])
    assert consumer.commit.call_count == 2
    consumer.close.assert_called_once()
    sched.addTriggerEvent.assert_called_once()


def test_onload_and_stop(conn, sched):
    sched.primed_event = threading.Event()
    sched.primed_event.set()
    consumer = mock.Mock()
    consumer.poll.return_value = None
    conn.consumer_factory = mock.Mock(return_value=consumer)
    conn.onLoad(None, None)
    conn.onStop()
    assert conn.consumer_thread is None
    consumer.close.assert_called_once()


# Event serialization

def test_event_roundtrip():
    event = KafkaTriggerEvent()
    event.type = 'deploy'
    event.project_hostname = 'review.example.com'
    event.project_name = 'org/project'
    event.data = {'env': 'prod'}
    event.topic = 't'
    event.partition = 3
    event.offset = 7
    event.key = 'k'
    copy = KafkaTriggerEvent.fromDict(json.loads(json.dumps(event.toDict())))
    for attr in ('type', 'canonical_project_name', 'data', 'topic',
                 'partition', 'offset', 'key'):
        assert getattr(copy, attr) == getattr(event, attr)


# Trigger filters

def make_filters(conf):
    schema = v.Schema(v.Any([KafkaDriver().getTriggerSchema()],
                            KafkaDriver().getTriggerSchema()))
    schema(conf)
    trigger = KafkaDriver().getTrigger(None, conf)
    return trigger.getEventFilters('kafka', conf, None)


def make_event(**kw):
    event = KafkaTriggerEvent()
    event.connection_name = 'kafka'
    event.type = 'message'
    event.topic = 'zuul-triggers'
    event.key = None
    event.data = {}
    for k, val in kw.items():
        setattr(event, k, val)
    return event


def test_filter_empty_matches_everything_on_connection():
    [f] = make_filters([{}])
    assert f.matches(make_event(), None)
    assert not f.matches(make_event(connection_name='other'), None)


def test_filter_event_and_topic():
    [f] = make_filters({'event': ['deploy', 'release-.*'],
                        'topic': 'zuul-triggers'})
    assert f.matches(make_event(type='deploy'), None)
    assert f.matches(make_event(type='release-1'), None)
    assert not f.matches(make_event(type='other'), None)
    assert not f.matches(make_event(type='deploy', topic='x'), None)


def test_filter_negated_key():
    [f] = make_filters([{'key': {'regex': '^test-', 'negate': True}}])
    assert f.matches(make_event(key='prod-1'), None)
    assert not f.matches(make_event(key='test-1'), None)


def test_filter_data():
    [f] = make_filters([{'data': {'env': ['staging', 'production'],
                                  'meta.priority': 'high'}}])
    assert f.matches(make_event(data={'env': 'staging',
                                      'meta': {'priority': 'high'}}), None)
    assert not f.matches(make_event(data={'env': 'staging'}), None)
    assert not f.matches(make_event(data={'env': 'dev',
                                          'meta': {'priority': 'high'}}),
                         None)


def test_filter_schema_rejects_unknown_keys():
    with pytest.raises(v.Invalid):
        make_filters([{'bogus': 'x'}])
