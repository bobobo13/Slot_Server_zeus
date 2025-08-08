# -*- coding: utf-8 -*-
import os
import socket
import time

from .libs.logger_utilities import get_topic_name


class ArkDataLoggerManager:
    def __init__(self, config, env, code_name, show_stats=False):
        # self.config = self._get_json_config(config_path)
        self.config = config
        self.env = env
        self.code_name = code_name
        self.log_auto_id = 0
        self.log_list = ['Mongo', 'Splunk', 'BigQuery', 'ELK']
        self.data_logger = {}
        if show_stats:
            self.show_stats()

    def register(self, name, logger):
        self.data_logger[name] = logger
        logger.setup(name, self.config)

    def send(self, type_name, log_data, timestamp, *args, **kwargs):
        log_data['i17game_send_key'] = self.get_i17game_send_key(type_name)
        log_list_by_config = self.config.get(type_name, {})
        for i in self.log_list:
            try:
                if log_list_by_config.get(i, False):
                    logger = self.data_logger.get(i)
                    if logger:
                        if i == 'ELK':
                            timestamp = self.format_ts(timestamp, 13)
                        topic_prefix = str.format('{}_{}', self.env, self.code_name)
                        topic = get_topic_name(topic_prefix, i, type_name, kwargs.get('topic_prefix'))
                        logger.send(type_name, log_data, timestamp, topic=topic, *args, **kwargs)
            except Exception as e:
                cb_func = kwargs.get('callback', None)
                if cb_func is not None:
                    cb_func(True, self.__class__.__name__ + '_' + type_name, e, log_data)
                else:
                    raise Exception('ArkDataLoggerManager send [%s] error: %s, log_data: %s' %
                                    (type_name, str(e), log_data))

    @staticmethod
    def format_ts(timestamp, length):
        ts_len = len(str(timestamp))
        timestamp = int(timestamp / pow(10, ts_len - length))
        return timestamp

    def show_stats(self):
        s = '{:20s} | {!s:10} | {!s:10} | {!s:10} | {!s:10}'.format('LogName', self.log_list[0], self.log_list[1],
                                                                    self.log_list[2], self.log_list[3])
        print(s)
        for i in sorted(self.config.items(), key=lambda x: x[1].get('Id')):
            s = '{:20s}'.format(i[0])
            for j in self.log_list:
                c = i[1]
                s += str.format(' | {!s:10}', c.get(j, False))
            print(s)

    def get_i17game_send_key(self, type_name):
        self.log_auto_id += 1
        return '{}_{}_{}_{}_{}'.format(socket.gethostname(), str(os.getpid()),
                                       str(self.log_auto_id), int(time.time() * 1000), type_name)
