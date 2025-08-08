import json
import pymongo
from datetime import datetime

from ..libs.logger_utilities import get_logger
from .data_logger_interface import DataLoggerInterface


class MongoLogger(DataLoggerInterface):
    def __init__(self, logger_name, db, coll_name, *_, **kwargs):
        self.expire = kwargs.get('expire')
        if self.expire is None:
            self.expire = 5184000
        sys_logger = kwargs.get('sys_logger', None)

        self.logger = get_logger(self, sys_logger)
        self.logger_name = logger_name
        self.coll_by_key = db[coll_name]
        self._db_init()

    def _db_init(self):
        try:
            self.coll_by_key.create_index([('logger_name', pymongo.ASCENDING),
                                           ('type_name', pymongo.ASCENDING),
                                           ('send_time', pymongo.ASCENDING)],
                                          unique=False)
            self.coll_by_key.create_index([('create_time', pymongo.ASCENDING)],
                                          expireAfterSeconds=self.expire, unique=False)
        except Exception as e:
            self.logger.warning('_db_init Error: ' + str(e))

    def send(self, type_name, log_data, timestamp, *args, **kwargs):
        dt = datetime.fromtimestamp(timestamp/1000.0/1000.0)
        insert_dic = {
            'logger_name': self.logger_name, 'type_name': type_name, 'log_data': json.dumps(log_data),
            'send_time': dt, 'create_time': datetime.utcnow()
        }
        msg_dict = kwargs.get('msg_dict', {})
        insert_dic.update(msg_dict)
        if pymongo.get_version_string().startswith("3."):
            self.coll_by_key.insert_one(insert_dic)
        else:
            self.coll_by_key.insert(insert_dic)

    def pop(self):
        return self.coll_by_key.find_one_and_delete({})
