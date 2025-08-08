# -*- coding: utf-8 -*-
from confluent_kafka import Consumer


class ConfluentKafkaConsumer:
    def __init__(self, config, subscribes):
        """
        :param config: https://docs.confluent.io/current/installation/configuration/consumer-configs.html
        """
        if config.get('bootstrap_servers') is None:
            raise Exception('bootstrap_servers must be non-null string in config')
        config.update({
            'auto.offset.reset': config.get('auto.offset.reset', 'earliest')
        })
        self.consumer = Consumer(config)
        self.consumer.subscribe(subscribes)

    def poll_async(self, timeout):
        msg = self.consumer.poll(timeout)
        return ConfluentKafkaConsumer.get_message_with_check_error(msg)

    def poll(self):
        msg = self.consumer.poll()
        return ConfluentKafkaConsumer.get_message_with_check_error(msg)

    @staticmethod
    def get_message_with_check_error(msg):
        if msg is None:
            return
        elif msg.error():
            raise Exception(msg.error())
        return msg

    def close(self):
        self.consumer.close()
