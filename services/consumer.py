import json

import pika
from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel
from config.base import app_setting
from services.producer import Producer


class RabbitConfig:
    def __init__(self):
        self.host: str = app_setting.BROKER_HOST
        self.vhost: str = app_setting.BROKER_VHOST
        self.port: int = app_setting.BROKER_PORT
        self.username: str = app_setting.BROKER_USER
        self.password: str = app_setting.BROKER_PASS
        self.queue: str = app_setting.BROKER_QUEUE
        self.routing_key: str = app_setting.BROKER_ROUTING
        self.exchange: str = app_setting.BROKER_EXCHANGE
        self.connection: BlockingConnection
        self.channel: BlockingChannel

    def __credentials(self):
        """
        create cretendial / auth
        :return:
        """
        return pika.PlainCredentials(
            username=self.username,
            password=self.password
        )

    def __paramaters(self):
        """
        create param for consumer
        :return:
        """
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=self.__credentials()
        )

    def open_connection(self):
        """
        open connection rabbit
        :return:
        """
        self.connection = pika.BlockingConnection(parameters=self.__paramaters())
        self.channel = self.connection.channel()

    def queue_declare(self):
        """
        declare a queue
        :return:
        """
        self.channel.queue_declare(
            queue=self.queue,
            durable=False
        )

    def queue_bind(self):
        """
        bind a queue
        :return:
        """
        self.channel.queue_bind(
            queue=self.queue,
            exchange=self.exchange,
            routing_key=self.routing_key
        )

    def get_channel(self):
        return self.channel

    def close(self):
        self.channel.close()
        self.connection.close()


class Consumer:
    def __init__(self):
        self.queue: str = app_setting.BROKER_QUEUE
        self.producer = Producer()

    def consume(self):
        """
        main func consumer
        :return:
        """
        pika_client = RabbitConfig()
        pika_client.open_connection()
        pika_client.queue_declare()
        pika_client.queue_bind()
        channel = pika_client.get_channel()
        channel.basic_qos(prefetch_count=1000)
        print("start consuming...")
        for method_frame, properties, body in channel.consume(self.queue, inactivity_timeout=2):
            if body:
                message = json.loads(body.decode('utf-8'))
                self.producer.produce(message)


