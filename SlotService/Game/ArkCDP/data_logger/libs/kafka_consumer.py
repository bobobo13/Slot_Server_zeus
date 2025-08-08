# -*- coding: utf-8 -*-
from kafka import KafkaConsumer as Consumer, ConsumerRebalanceListener


class KafkaConsumer(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        print('on_partitions_revoked: ', revoked)

    def on_partitions_assigned(self, assigned):
        print('on_partitions_assigned: ', assigned)

    def __init__(self, config, subscribes):
        """
        :param config: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
        """
        if config.get('bootstrap_servers') is None:
            raise Exception('bootstrap_servers must be non-null string in config')
        self.flag = True
        # config.update({
        #     'auto.offset.reset': config.get('auto.offset.reset', 'earliest')
        # })
        self.consumer = Consumer(**config)
        self.consumer.subscribe(listener=self, **subscribes)

    def poll(self, timeout_ms=0, max_records=None, update_offsets=True):
        msg = self.consumer.poll()
        return msg

    def start_listener(self, cb):
        self.flag = True
        for message in self.consumer:
            if cb and self.flag:
                cb(message)

    def stop_listener(self):
        self.flag = False

    def close(self):
        self.consumer.close()


count = 0


def callback(msg):
    print msg


if __name__ == '__main__':
    _config = {
        'bootstrap_servers': '127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094',
        'group_id': 'group_a', 'client_id': 'A1'
    }
    # _topics = {'topics': ['dev_yak_AccountCreate']}
    _topics = {'pattern': '.*_yak_AccountCreate'}
    kafka = KafkaConsumer(_config, _topics)
    kafka.start_listener(callback)
    kafka.stop_listener()
