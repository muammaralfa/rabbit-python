import json

import pika
from pika.exchange_type import ExchangeType
from config.base import app_setting


class Producer:
    def __init__(self):
        self.host: str = app_setting.BROKER_HOST
        self.vhost: str = app_setting.BROKER_VHOST
        self.port: int = app_setting.BROKER_PORT
        self.username: str = app_setting.BROKER_USER
        self.password: str = app_setting.BROKER_PASS
        self.exchange: str = app_setting.BROKER_EXCHANGE
        self.queue: str = app_setting.QUEUE
        self.routing: str = app_setting.ROUTING
        self.credentials = pika.PlainCredentials(self.username, self.password)
        self.params = pika.ConnectionParameters(
            host=self.host,
            virtual_host=self.vhost,
            port=self.port,
            credentials=self.credentials
        )
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.topic, durable=True)
        self.channel.queue_declare(queue=self.queue)
        self.channel.queue_bind(self.queue, exchange=self.exchange)

    def produce(self, message):
        """
        just send message into rabbit using basic public
        :param message:
        :return:
        """
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing,
            body=json.dumps(message).encode("utf-8")
        )

    def close(self):
        self.connection.close()