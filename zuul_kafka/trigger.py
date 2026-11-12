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

import voluptuous as v

from zuul.driver.util import scalar_or_list, to_list, make_regex, ZUUL_REGEX
from zuul.trigger import BaseTrigger

from zuul_kafka.model import KafkaEventFilter


class KafkaTrigger(BaseTrigger):
    name = 'kafka'

    def getEventFilters(self, connection_name, trigger_conf,
                        parse_context):
        efilters = []
        for trigger in to_list(trigger_conf):
            data = {
                path: [make_regex(x, parse_context) for x in to_list(value)]
                for path, value in trigger.get('data', {}).items()
            }
            f = KafkaEventFilter(
                connection_name=connection_name,
                trigger=self,
                types=[make_regex(x, parse_context)
                       for x in to_list(trigger.get('event'))],
                topics=[make_regex(x, parse_context)
                        for x in to_list(trigger.get('topic'))],
                keys=[make_regex(x, parse_context)
                      for x in to_list(trigger.get('key'))],
                data=data,
                debug=trigger.get('debug'),
            )
            efilters.append(f)

        return efilters


def getSchema():
    regex = v.Any(ZUUL_REGEX, str)
    kafka_trigger = {
        'event': scalar_or_list(regex),
        'topic': scalar_or_list(regex),
        'key': scalar_or_list(regex),
        'data': {str: scalar_or_list(regex)},
        'debug': bool,
    }
    return kafka_trigger
