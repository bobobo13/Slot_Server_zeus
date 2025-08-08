# -*- coding: utf-8 -*-
import json
import logging
import socket

from .libs.logger_utilities import get_topic_name


class ArkLoggingHandler(logging.StreamHandler):
    def __init__(self, code_name, env, logger=None, topic=None):
        super(ArkLoggingHandler, self).__init__()
        self.code_name = code_name
        self.env = env
        self.logger = logger
        self.topic = topic

    def emit(self, record):
        msg_obj = {
            'CodeName': self.code_name,
            'ProcessID': record.process,
            'PackageName': record.name,
            'AscTime': int(record.created * 1000),
            'LevelName': record.levelname,
            'FileName': record.filename,
            'FuncName': record.funcName,
            'Message': record.msg,
            'HostName': socket.gethostname(),
        }

        if record.args:
            msg_obj['Message'] = record.msg % record.args

        if type(record.msg) == dict:
            msg_obj['Message'] = json.dumps(msg_obj['Message'])
            msg_obj['Obj'] = record.msg

        if self.logger:
            type_name = 'ArkSysLog'
            topic = str.format('{}_{}_{}', self.code_name, self.env, type_name)
            topic_prefix = str.format('{}_{}', self.env, self.code_name)
            topic = get_topic_name(topic_prefix, 'elk', type_name)
            self.logger.send(type_name, msg_obj, msg_obj.get('AscTime'), topic=self.topic or topic)
