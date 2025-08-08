# -*- coding: utf-8 -*-
import inspect
from functools import partial

import kafka
import six
from kafka import KafkaProducer as Producer, KafkaAdminClient
from kafka.admin import NewPartitions, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, UnknownTopicOrPartitionError, InvalidPartitionsError, \
    InvalidReplicationFactorError


class KafkaProducer:
    def __init__(self, producer_connect_info, admin_client_connect_info, sys_logger=None):
        """
        :param config: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
        """
        self.sys_logger = sys_logger

        if producer_connect_info.get('bootstrap_servers') is None:
            raise Exception('bootstrap_servers must be non-null string in config')

        self.is_connected = False
        try:
            self.producer = Producer(**producer_connect_info)
            self.kafka_admin_client = KafkaAdminClient(**admin_client_connect_info)
            self._check_connect()
        except kafka.errors.InvalidPartitionsError as e:
            self._log('warning', '({})'.format(e))
        except NoBrokersAvailable as e:
            self.producer = None
            self._log('error', str.format('({}) producer_connect_info: {}, admin_client_connect_info: {}',
                                          e, producer_connect_info, admin_client_connect_info))
            raise NoBrokersAvailable(e)

    def set_topic_partitions(self, topic_name, num_partitions=2, replication_factor=2, kafka_topic_config=None):
        # return self._delete_topic(topic_name)
        topic_partitions = {topic_name: NewPartitions(total_count=num_partitions)}
        self.set_topic_configs(kafka_topic_config, topic_name)
        try:
            self.kafka_admin_client.create_partitions(topic_partitions)
        except UnknownTopicOrPartitionError:
            self.create_topics(topic_name, num_partitions, replication_factor)
        except InvalidPartitionsError:
            # self.sys_logger.warning('[InvalidPartitionsError] %s', e)
            pass

    def set_topic_configs(self, kafka_topic_config, topic_name):
        topic__config_list = []
        if kafka_topic_config:
            self._log('info', 'topic_name: {}, kafka_topic_config:{}'.format(topic_name, kafka_topic_config))
            topic__config_list.append(ConfigResource(resource_type=ConfigResourceType.TOPIC, name=topic_name,
                                                     configs=kafka_topic_config))
        if len(topic__config_list) > 0:
            self._log('info', 'kafka_alter_configs: {}'.format(
                self.kafka_admin_client.alter_configs(config_resources=topic__config_list)))

    def create_topics(self, topic_name, num_partitions, replication_factor):
        # 部分環境可能只有一台 kafka，此時若 replication_factor 大於 1 會造成 create_topics 失敗
        try:
            return self._create_topics(topic_name, num_partitions, replication_factor)
        except InvalidReplicationFactorError:
            return self._create_topics(topic_name, num_partitions, 1)

    def _create_topics(self, topic_name, num_partitions, replication_factor):
        new_topics = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        return self.kafka_admin_client.create_topics(new_topics)

    def _delete_topic(self, topic_name):
        try:
            return self.kafka_admin_client.delete_topics([topic_name])
        except UnknownTopicOrPartitionError as e:
            self.sys_logger.warning('[UnknownTopicOrPartitionError] %s', e)

    def send(self, topic, value, *args, **kwargs):
        flush = kwargs.pop('flush', None)
        if six.PY3:
            self._produce(topic, value.encode('utf-8'), *args, **kwargs)
        else:
            self._produce(topic, value, *args, **kwargs)
        if flush:
            self._flush()

    def _produce(self, topic, value, *_, **kwargs):
        callback = partial(kwargs.get('callback'), value)
        if self._check_connect():
            try:
                self.producer.send(topic, value).add_both(callback)
            except KafkaTimeoutError as e:
                callback(e)
        else:
            callback('KafkaProducer can not connect Server')

    def _check_connect(self):
        result = False
        if self.producer:
            sender = getattr(self.producer, '_sender')
            client = getattr(sender, '_client')
            connect = getattr(client, '_conns')
            keys = connect.keys()
            for i in keys:
                result = result or (not client.is_disconnected(i))
        if self.is_connected != result:
            if result is True:
                self._log('info', 'KafkaProducer 連線正常 status: %s' % result)
            else:
                self._log('error', 'KafkaProducer 連線中斷 status: %s' % result)
            self.is_connected = result
        return result

    def close(self):
        self._flush()

    def _flush(self):
        self.producer.flush()

    def _log(self, log_type, msg):
        if self.sys_logger:
            s = '[{} {}] %s'.format(self.__class__.__name__, inspect.stack()[1][3])
            if log_type == 'info':
                self.sys_logger.info(s, msg)
            elif log_type == 'warning':
                self.sys_logger.warning(s, msg)
            elif log_type == 'error':
                self.sys_logger.error(s, msg)
