# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
import logging
import threading
import time

import confluent_kafka
import voluptuous as v

from zuul.connection import BaseConnection
from zuul.exceptions import ChangeNotFound, ConfigurationError
from zuul.lib.logutil import get_annotated_logger

from zuul_kafka.model import KafkaTriggerEvent

# Options in zuul.conf starting with this prefix are passed verbatim to
# librdkafka, e.g. "rdkafka.security.protocol=SASL_SSL".
RDKAFKA_PREFIX = 'rdkafka.'

MESSAGE_SCHEMA = v.Schema({
    v.Required('project'): v.All(str, v.Length(min=1)),
    v.Exclusive('branch', 'ref'): v.All(str, v.Length(min=1)),
    v.Exclusive('ref', 'ref'): v.All(str, v.Length(min=1)),
    'oldrev': v.Any(str, None),
    'newrev': v.Any(str, None),
    'event': v.All(str, v.Length(min=1)),
    'tenant': str,
    'data': dict,
})


class InvalidMessage(Exception):
    pass


class KafkaConnection(BaseConnection):
    driver_name = 'kafka'
    log = logging.getLogger("zuul.KafkaConnection")

    # Number of times a valid message is retried when dispatching it
    # fails (e.g. ZooKeeper unavailable) before it is skipped.
    max_attempts = 3
    retry_delay = 5

    def __init__(self, driver, connection_name, connection_config):
        super().__init__(driver, connection_name, connection_config)
        self.topics = [t.strip() for t in
                       connection_config.get('topics', '').split(',')
                       if t.strip()]
        if not self.topics:
            raise ConfigurationError(
                "Kafka connection %s requires the topics option" %
                connection_name)
        if not connection_config.get('bootstrap_servers'):
            raise ConfigurationError(
                "Kafka connection %s requires the bootstrap_servers option" %
                connection_name)
        self.default_event = connection_config.get('default_event', 'message')
        self.consumer_factory = confluent_kafka.Consumer
        self.consumer = None
        self.consumer_thread = None
        self._stopped = threading.Event()

    def getConsumerConfig(self):
        config = {
            'bootstrap.servers': self.connection_config['bootstrap_servers'],
            # All schedulers share one consumer group so that each
            # message is handled by exactly one of them.
            'group.id': self.connection_config.get('group_id', 'zuul'),
            'auto.offset.reset': self.connection_config.get(
                'auto_offset_reset', 'latest'),
            'client.id': self.connection_config.get(
                'client_id', 'zuul-%s' % self.connection_name),
            'enable.auto.commit': False,
        }
        for key, value in self.connection_config.items():
            if key.startswith(RDKAFKA_PREFIX):
                config[key[len(RDKAFKA_PREFIX):]] = value
        return config

    def onLoad(self, zk_client, component_registry):
        # Only the scheduler consumes events; zuul-web, executors, etc.
        # load the connection without a scheduler.
        if not self.sched:
            return
        self.log.debug("Starting Kafka connection %s", self.connection_name)
        self._stopped.clear()
        self.consumer_thread = threading.Thread(
            name='KafkaConsumer-%s' % self.connection_name,
            target=self._run, daemon=True)
        self.consumer_thread.start()

    def onStop(self):
        self.log.debug("Stopping Kafka connection %s", self.connection_name)
        self._stopped.set()
        if self.consumer_thread:
            self.consumer_thread.join()
            self.consumer_thread = None

    def _run(self):
        # Wait until the scheduler has loaded the tenant layouts,
        # otherwise we would not be able to resolve projects.
        while not self._stopped.is_set():
            if self.sched.primed_event.wait(1):
                break
        while not self._stopped.is_set():
            try:
                self._consume()
            except Exception:
                self.log.exception("Error in Kafka consumer for %s:",
                                   self.connection_name)
                self._stopped.wait(self.retry_delay)

    def _consume(self):
        self.consumer = self.consumer_factory(self.getConsumerConfig())
        try:
            self.consumer.subscribe(self.topics)
            self.log.info("Subscribed to Kafka topics %s on %s",
                          self.topics, self.connection_name)
            while not self._stopped.is_set():
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().fatal():
                        raise confluent_kafka.KafkaException(msg.error())
                    self.log.warning("Kafka error on %s: %s",
                                     self.connection_name, msg.error())
                    continue
                self._processMessage(msg)
                self.consumer.commit(message=msg, asynchronous=False)
        finally:
            self.consumer.close()
            self.consumer = None

    def _processMessage(self, msg):
        for attempt in range(1, self.max_attempts + 1):
            try:
                self._handleMessage(msg)
                return
            except InvalidMessage as e:
                self.log.warning("Ignoring Kafka message %s[%s]@%s: %s",
                                 msg.topic(), msg.partition(), msg.offset(),
                                 e)
                return
            except Exception:
                self.log.exception(
                    "Error handling Kafka message %s[%s]@%s "
                    "(attempt %s of %s):", msg.topic(), msg.partition(),
                    msg.offset(), attempt, self.max_attempts)
                if attempt < self.max_attempts:
                    self._stopped.wait(self.retry_delay)

    def _parseMessage(self, msg):
        value = msg.value()
        if value is None:
            raise InvalidMessage("empty message")
        try:
            payload = json.loads(value)
        except ValueError as e:
            raise InvalidMessage("invalid JSON: %s" % e)
        try:
            return MESSAGE_SCHEMA(payload)
        except v.Invalid as e:
            raise InvalidMessage("invalid message: %s" % e)

    def _findProject(self, payload):
        tenant_name = payload.get('tenant')
        if tenant_name:
            tenant = self.sched.abide.tenants.get(tenant_name)
            if tenant is None:
                raise InvalidMessage("unknown tenant %s" % tenant_name)
            tenants = [tenant]
        else:
            tenants = list(self.sched.abide.tenants.values())
        for tenant in tenants:
            try:
                trusted, project = tenant.getProject(payload['project'])
            except Exception as e:
                # Ambiguous project name
                raise InvalidMessage(str(e))
            if project is not None:
                return tenant, project
        raise InvalidMessage("project %s not found" % payload['project'])

    def _makeEvent(self, msg, payload, tenant, project):
        event = KafkaTriggerEvent()
        event.type = payload.get('event', self.default_event)
        event.connection_name = self.connection_name
        event.project_hostname = project.canonical_hostname
        event.project_name = project.name
        if 'ref' in payload:
            event.ref = payload['ref']
            if event.ref.startswith('refs/heads/'):
                event.branch = event.ref[len('refs/heads/'):]
        else:
            branch = payload.get('branch')
            if not branch:
                metadata = tenant.layout.getProjectMetadata(
                    project.canonical_name)
                branch = (metadata and metadata.default_branch) or 'master'
            event.branch = branch
            event.ref = 'refs/heads/%s' % branch
        event.oldrev = payload.get('oldrev')
        event.newrev = payload.get('newrev')
        event.data = payload.get('data', {})
        event.topic = msg.topic()
        event.partition = msg.partition()
        event.offset = msg.offset()
        key = msg.key()
        if isinstance(key, bytes):
            key = key.decode('utf8', errors='replace')
        event.key = key
        # The event id points back at the message, so a job can read
        # the full message from Kafka (only the event id reaches jobs).
        event.zuul_event_id = 'kafka:%s:%s:%s' % (
            event.topic, event.partition, event.offset)
        event.timestamp = time.time()
        return event

    def _handleMessage(self, msg):
        payload = self._parseMessage(msg)
        tenant, project = self._findProject(payload)
        event = self._makeEvent(msg, payload, tenant, project)
        log = get_annotated_logger(self.log, event)
        log.debug("Received Kafka message %s[%s]@%s for %s",
                  event.topic, event.partition, event.offset,
                  project.canonical_name)

        # Like the timer driver, refresh the ref in the change cache
        # so the item is enqueued with the current branch head.
        change_key = project.source.getChangeKey(event)
        if change_key is None:
            raise InvalidMessage("unable to build a change key for %s" %
                                 event.ref)
        try:
            project.source.getChange(change_key, refresh=True, event=event)
        except ChangeNotFound:
            raise InvalidMessage("%s not found in %s" % (
                event.ref, project.canonical_name))

        self.logEvent(event)
        if payload.get('tenant'):
            event.arrived_at_scheduler_timestamp = time.time()
            self.sched.trigger_events[tenant.name].put(
                self.driver_name, event)
        else:
            self.sched.addTriggerEvent(self.driver_name, event)
