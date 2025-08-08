import pymongo

from ..ark_data_logger_manager import ArkDataLoggerManager
from ..libs.io_event_queue import IOEventQueue
from ..libs.logger_utilities import get_logger, datetime_gmt_yyyymmdd
from .data_logger_interface import DataLoggerInterface


class MongoBackendLogger(DataLoggerInterface):
    def __init__(self, logger_name, db, queue_concurrent=300, error_logger=None, *_, **kwargs):
        sys_logger = kwargs.get('sys_logger', None)
        self.gmt_time_zone = kwargs.get('gmt_time_zone', 0)

        self.logger = get_logger(self, sys_logger)
        self.logger_name = logger_name
        self.db = db
        self.error_logger = error_logger

        self.io_event_queue = IOEventQueue(queue_concurrent, sys_logger, None, True)

    def send(self, type_name, log_data, timestamp, *args, **kwargs):
        self.io_event_queue.append(self._send, None, type_name, log_data, timestamp, *args, **kwargs)

    def _send(self, type_name, log_data, timestamp, *_, **kwargs):
        cb_func = kwargs.get('callback', None)
        try:
            mongo_coll_date = kwargs.get('mongo_coll_date')
            if mongo_coll_date is None:
                ts = ArkDataLoggerManager.format_ts(timestamp, 10)
                mongo_coll_date = datetime_gmt_yyyymmdd(self.gmt_time_zone, ts)
            if pymongo.get_version_string().startswith("3."):
                result = self.db[type_name + '_' + mongo_coll_date].insert_one(log_data)
            else:
                result = self.db[type_name + '_' + mongo_coll_date].insert(log_data)
            if cb_func is not None:
                cb_func(False, self.__class__.__name__, result, log_data)

        # except errors.ServerSelectionTimeoutError as e:
        except Exception as e:
            self.logger.warn('%s send error: %s' % (self.__class__, e))
            if self.error_logger is not None:
                msg_dict = {
                    'error_msg': str(e)
                }
                if log_data.get('_id') is not None:
                    log_data.pop('_id')
                self.error_logger.send(type_name, log_data, timestamp, msg_dict=msg_dict)
            if cb_func is not None:
                cb_func(True, self.__class__.__name__, e, log_data)
