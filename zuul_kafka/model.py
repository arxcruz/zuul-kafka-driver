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

from zuul.model import EventFilter, FalseWithReason, TriggerEvent

_MISSING = object()


def lookup(data, path):
    """Return the value at a dotted path in nested dicts or _MISSING."""
    value = data
    for part in path.split('.'):
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = value[part]
    return value


class KafkaTriggerEvent(TriggerEvent):
    def __init__(self):
        super().__init__()
        self.topic = None
        self.partition = None
        self.offset = None
        self.key = None

    def toDict(self):
        d = super().toDict()
        d["topic"] = self.topic
        d["partition"] = self.partition
        d["offset"] = self.offset
        d["key"] = self.key
        return d

    def updateFromDict(self, d):
        super().updateFromDict(d)
        self.topic = d.get("topic")
        self.partition = d.get("partition")
        self.offset = d.get("offset")
        self.key = d.get("key")

    def _repr(self):
        r = super()._repr()
        if self.topic:
            r += ' %s[%s]@%s' % (self.topic, self.partition, self.offset)
        return r


class KafkaEventFilter(EventFilter):
    def __init__(self, connection_name, trigger, types=None, topics=None,
                 keys=None, data=None, debug=None):
        super().__init__(connection_name, trigger, debug)
        self.types = types or []
        self.topics = topics or []
        self.keys = keys or []
        # Mapping of dotted path -> list of regexes
        self.data = data or {}

    def __repr__(self):
        ret = '<KafkaEventFilter'
        ret += ' connection: %s' % self.connection_name
        if self.types:
            ret += ' types: %s' % ', '.join(x.pattern for x in self.types)
        if self.topics:
            ret += ' topics: %s' % ', '.join(x.pattern for x in self.topics)
        if self.keys:
            ret += ' keys: %s' % ', '.join(x.pattern for x in self.keys)
        if self.data:
            ret += ' data: %s' % ', '.join(
                '%s=%s' % (k, '|'.join(x.pattern for x in v))
                for k, v in self.data.items())
        ret += '>'
        return ret

    @staticmethod
    def _anyMatch(regexes, value):
        return any(r.match(value) for r in regexes)

    def matches(self, event, change):
        if not super().matches(event, change):
            return FalseWithReason("Connection %s does not match %s" % (
                self.connection_name, event.connection_name))

        # types, topics and keys are each ORed
        if self.types and not self._anyMatch(self.types, event.type or ''):
            return FalseWithReason("Types %s do not match %s" % (
                self.types, event.type))
        if self.topics and not self._anyMatch(self.topics,
                                              event.topic or ''):
            return FalseWithReason("Topics %s do not match %s" % (
                self.topics, event.topic))
        if self.keys and not self._anyMatch(self.keys, event.key or ''):
            return FalseWithReason("Keys %s do not match %s" % (
                self.keys, event.key))

        # data paths are ANDed, the regexes for a single path are ORed
        for path, regexes in self.data.items():
            value = lookup(event.data or {}, path)
            if value is _MISSING:
                return FalseWithReason("Data %s missing" % (path,))
            if not self._anyMatch(regexes, str(value)):
                return FalseWithReason("Data %s=%s does not match %s" % (
                    path, value, regexes))

        return True
