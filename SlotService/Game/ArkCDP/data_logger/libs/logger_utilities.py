# -*- coding: utf-8 -*-

import calendar
import datetime
import json
import logging
import re
import time

from bson import json_util


def get_logger(class_self, logger):
    if logger is None:
        logging.basicConfig()
        logger = logging.getLogger(str(class_self.__class__.__name__))
        logger.setLevel(logging.DEBUG)
    return logger


# 取得當前日期字串, 用yyyymmdd格式 , 帶時區參數
def datetime_gmt_yyyymmdd(gmt=0, timestamp=None):
    if timestamp is None:
        timestamp = time.time()
    struct_time = time.gmtime(timestamp + gmt)
    return time.strftime("%Y%m%d", struct_time)



def format_log(type_name, log_data, timestamp):
    if '_id' in log_data:
        log_data['_id'] = str(log_data.get('_id'))
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()
    if isinstance(timestamp, datetime.datetime):
        timestamp = datetime_to_timestamp(timestamp)
    log_data['DefineTime'] = timestamp
    log_data['CreateTime'] = timestamp
    log = {
        "TypeName": type_name,
        "LogData": log_data,
        "DefineTime": timestamp
    }
    log_str = json_dump(log)
    return log_str, log


def json_dump(data):
    try:
        return json.dumps(data, default=json_util.default)
    except UnicodeDecodeError:
        return json.dumps(data, encoding='unicode_escape')


def get_json_config(path):
    with open(path) as fh:
        json_config = json.load(fh)
    return json_config


def datetime_to_timestamp(utc_datetime):
    return int(calendar.timegm(utc_datetime.utctimetuple()) * 1000.0 + (utc_datetime.microsecond / 1000.0))


def get_topic_name(prefix, logger_name, type_name, custom_prefix=None):
    if custom_prefix is not None:
        type_name = custom_prefix + type_name
    topic_name = str.format('{}_{}', logger_name, _pascal_case_to_snake_case(type_name))
    if prefix is not None:
        topic_name = str.format('{}_{}', prefix, topic_name)
    return topic_name.lower()


def _pascal_case_to_snake_case(camel_case):
    """大驼峰（帕斯卡）转蛇形"""
    snake_case = re.sub(r"(?P<key>[A-Z])", r"_\g<key>", camel_case)
    return snake_case.lower().strip('_')
