# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.contrib.hooks.amqp_hook import AMQPHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AMQPSensor(BaseSensorOperator):
    """
    Wait for a message to be ready on an AMQP queue

    :param queue: the queue name to listen
    :type queue: str
    :param conn_id: The connection to run the sensor against
    :type conn_id: str
    """
    template_fields = ('queue',)

    @apply_defaults
    def __init__(self, queue, conn_id='amqp_default', *args, **kwargs):
        super(AMQPSensor, self).__init__(*args, **kwargs)
        self.queue = queue
        self.conn_id = conn_id

    def _create_hook(self):
        """Return connection hook."""
        return AMQPHook(conn_id=self.conn_id)

    def poke(self, context):
        with self._create_hook() as hook:
            logging.info('Poking for %s', self.queue)
            result = hook.get_queue_size(self.queue)
            message = hook.get_message(self.queue)
            print (message)
            hook.ack(hook.get_delivery_tag(message))
            return (result > 0)

