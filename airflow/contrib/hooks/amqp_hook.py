# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Ported to Airflow by Bolke de Bruin
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
#
import subprocess
import pika
from contextlib import contextmanager

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

import logging

class AMQPHook(BaseHook):
    """
    AMQPHook provides connection utilities with RabbitMQ
    """
    def __init__(self, conn_id='amqp_default'):
        self.conn_id = conn_id
        self.conn = None

    def get_conn(self):
        if self.conn is None:
            params = self.get_connection(self.conn_id)
            conn_params = pika.ConnectionParameters(
                host = params.host,
                port = params.port,
                virtual_host = params.extra_dejson.get('virtual_host', params.schema),
                credentials = pika.credentials.PlainCredentials(username=params.login, password=params.password),
            )
            self.conn = pika.BlockingConnection(conn_params)
        return self.conn

    def get_channel(self):
        return self.get_conn().channel()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        pass

    """
    Get the total size of messages that are stored in queue

    :param queue: queue name
    :type queue: str
    """
    def get_queue_size(self, queue):
        channel = self.get_channel()
        # redeclare the queue with passive flag
        requeue = channel.queue_declare(queue=queue, passive=True)
        size = requeue.method.message_count
        print ('There is total {0} message in queue {1}'.format(size, queue))
        return size

    """
    get a message from queue

    :param queue: queue name
    :type queue: str
    """
    def get_message(self, queue, no_ack = False):
        channel = self.get_channel()
        method, header, body = channel.basic_get(queue=queue, no_ack=no_ack)
        if method:
            print("Receive message. method:{0}\n, header:{1}\n, body:{2}\n".format(method, header, body))
            return dict(method = method, header = header, body = body )
        else:
            return None

    def get_delivery_tag(self, message):
        if message is not None:
            method = message.get('method')
            if method is not None:
                return method.delivery_tag
        else:
            return None

    def ack(self, delivery_tag, multiple = False):
        return self.get_channel().basic_ack(delivery_tag, multiple)

    def nack(self, delivery_tag, multiple = False, requeue = True):
        return self.get_channel().basic_nack(delivery_tag, multiple, requeue)


