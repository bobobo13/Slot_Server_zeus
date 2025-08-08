# -*- coding: utf-8 -*-

from .kafka_consumer import KafkaConsumer

_config = {'bootstrap_servers': '127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094', 'group_id': 'group_b', 'client_id': 'A3'}
# _topics = {'topics': ['dev_yak_AccountCreate']}
_topics = {'pattern': '.*_yak_AccountCreate'}
KafkaConsumer(_config, _topics)

# sh kafka-topics.sh --alter --bootstrap-server kafka_kafka1_1:9092,kafka_kafka2_1:9092,kafka_kafka3_1:9092 --topic dev_yak_AccountCreate --partitions 2
