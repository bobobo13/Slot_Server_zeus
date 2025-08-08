# -*- coding: utf-8 -*-
import copy
import json
from functools import partial

from kafka.producer.future import RecordMetadata

from ..libs.io_event_queue import IOEventQueue
from ..libs.logger_utilities import get_logger, format_log, get_topic_name
from .data_logger_interface import DataLoggerInterface


class KafkaLogger(DataLoggerInterface):
    def __init__(self, kafka_producer, topic, sys_logger, error_logger=None, queue_concurrent=300, topic_prefix=None):
        self.logger = get_logger(self, sys_logger)
        if kafka_producer is None:
            self.logger.warn(
                "%s.%s kafka_producer is None, therefore logs won't be send" % (self.__class__.__name__, "__init__"))
        if not topic:
            raise Exception("topic is requisite")
        self.producer = kafka_producer
        self.topic = topic
        self.error_logger = error_logger
        self.kafka_logger_ioEventQueue = IOEventQueue(queue_concurrent, sys_logger, None, True)
        self.topic_prefix = topic_prefix

    def setup(self, name, config):
        self._init_kafka_topics(name, config)

    def send(self, type_name, log_data, timestamp, *args, **kwargs):
        self.kafka_logger_ioEventQueue.append(self._send, None, type_name, log_data, timestamp, *args, **kwargs)

    def close(self):
        self.producer.close()

    def _init_kafka_topics(self, name, config):
        for k in config.keys():
            config_by_type_name = config.get(k, {})
            custom_topic_prefix = None
            if config_by_type_name.get('IsCustom') is True:
                custom_topic_prefix = 'Custom'
            self._setup_kafka_topic(config_by_type_name, k, name, custom_topic_prefix)

    def _setup_kafka_topic(self, config_by_type_name, key, name, custom_prefix=None):
        if config_by_type_name.get(name):
            partition = config_by_type_name.get('KafkaTopicPartition', 2)
            kafka_topic_config = config_by_type_name.get('KafkaTopicConfig', None)
            topic = get_topic_name(self.topic_prefix, name, key, custom_prefix)
            self._set_topic_partitions(topic, partition, kafka_topic_config)

    def _set_topic_partitions(self, topic_name, count=2, kafka_topic_config=None):
        if self.producer is not None:
            self.producer.set_topic_partitions(topic_name, count, kafka_topic_config=kafka_topic_config)

    def _send(self, type_name, log_data, timestamp, *args, **kwargs):
        topic = kwargs.get('topic', self.topic)
        log_str, log_obj = format_log(type_name, copy.copy(log_data), timestamp)

        if self.producer is None:
            self._err_record('KafkaProducer is None', log_obj, topic)
            return

        # 先傳到內部的 _callback function 進行例外處理, 再呼叫客製的 callback function
        if kwargs.get('topic'):
            kwargs.pop('topic')
        kwargs['callback'] = partial(self._callback, kwargs.get('callback', None), topic)
        self.producer.send(topic, log_str, *args, **kwargs)

    def _callback(self, cb_func, topic, value_str, *args, **_):
        error = False
        result = args[0]
        value = json.loads(value_str)
        if type(result) is not RecordMetadata:
            error = True
            self._err_record(result, value, topic)
        if cb_func is not None:
            cb_func(error, self.__class__.__name__, result, value)

    def _err_record(self, err, value, topic):
        # value = json.loads(msg.value())
        type_name = value.get('TypeName')
        log_data = value.get('LogData')
        timestamp = value.get('DefineTime')
        if self.error_logger is not None:
            msg_dict = {
                'error_msg': str(err),
                'topic': topic
            }
            self.error_logger.send(type_name, log_data, timestamp, msg_dict=msg_dict)
        # self.logger.error('KafkaLogger _callback, error: {} topic:{} value:{}'.format(self.get_topic(), err, value))

    def get_topic(self):
        return self.topic
