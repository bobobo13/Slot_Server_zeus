#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import json

import pymongo
import redis
import sys
import traceback
# try:
#     from game_status_code import *
# except ImportError:
#     print("ImportError: game_status_code")

import six
if six.PY2:
    from Crypto.Hash import SHA as SHA1
else:
    from Crypto.Hash import SHA1
# import hmac
# import hashlib
# import os
import datetime
import time

"""
"""

GAME_STATE_CLEAN_TIME = 12       # 定期清理game_state的時間點 (小時數 0~23) (UTC+8時間)
GAME_STATE_CLEAN_INTERVAL = 600  # 多久檢查一次game_state

class UserManager(object):
    def __init__(self, **kwargs):
        self.logger = kwargs.get('logger')
        self._UserDao = None
        if 'UserInfoDb' in kwargs:
            self._UserDao = UserDao(**kwargs)


    def on_module_init(self, module_manager):
        self.env_obj = module_manager.get_class_instance('env')
        self.env = self.env_obj.get_env()
        self.logger = module_manager.get_class_instance('log_manager').get_logger()
        self.timer_service = module_manager.get_class_instance("timer_service")

        self._UserDao = UserDao(
            logger=self.logger,
            ArkDb=module_manager.get_class_instance('mongo_manager').get_wrap_database('pixiu'),
            UserInfoDb=module_manager.get_class_instance('mongo_manager').get_wrap_database('UserInfo'),

        )


        # if self.is_on_admin_server():
        #     self.timer_service.register("clean_game_state", GAME_STATE_CLEAN_INTERVAL, self.clean_game_state)


    def is_on_admin_server(self):
        if self.env_obj.get_admin_enable('clean_game_state'):
            return True
        else:
            return False

    def set_third_party_system(self, third_party_system):
        # deprecated
        pass

    def get_user_data(self, user_id, fields=None):
        return self._UserDao.get_user_data(user_id, fields)

    def set_user_data(self, user_id, set_dict=None, inc_dict=None, set_insert_dict=None, fields=None, upsert=False):
        return self._UserDao.set_user_data(user_id, set_dict, inc_dict, set_insert_dict, fields, upsert)

    def set_game_data(self, user_id, set_dict=None, inc_dict=None, set_insert_dict=None, fields=None, upsert=False):
        return self._UserDao.set_game_data(user_id, set_dict, inc_dict, set_insert_dict, fields, upsert)

    def get_game_data(self, user_id, fields=None, bSecondary=True):
        return self._UserDao.get_game_data(user_id, fields, bSecondary=bSecondary)

    def get_ark_id_by_uid(self, uid, from_type='bcompany'):
        return self._UserDao.get_ark_id_by_uid(uid, from_type)

    def add_guide(self, user_id, name, step):
        return self._UserDao.add_guide(user_id, name, step)

    def clean_cached_user_data(self, ark_id):
        self._UserDao.clean_cached_user_data(ark_id)


class UserDao(object):
    USER_DATA_CACHE_EXPIRE_TIME = 60
    def __init__(self, **kwargs):
        self.logger = kwargs.get('logger')
        # self.db_ark_db = kwargs.get('ArkDb')
        self.db_user = kwargs.get('UserInfoDb')

        if pymongo.get_version_string().startswith("2."):
            # self.col_ark_user = self.db_ark_db['ArkUser']
            self.col_user_info = self.db_user['UserInfo']
            self.col_game_data = self.db_user['GameData']
        else:
            # self.col_ark_user = self.db_ark_db['ArkUser'].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            self.col_user_info = self.db_user['UserInfo'].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            self.col_game_data = self.db_user['GameData'].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)

        # self.col_user_info.create_index([('ArkHash', pymongo.ASCENDING), ('ArkId', pymongo.ASCENDING)], unique=True)
        try:
            # self.col_user_info.create_index([('ArkId', pymongo.ASCENDING)])
            self.col_game_data.create_index([('ArkId', pymongo.ASCENDING)], unique=True)
            self.col_user_info.create_index([('ThirdPartyId', pymongo.ASCENDING), ('FromType', pymongo.ASCENDING)])
            self.col_user_info.create_index([('ThirdPartyName', pymongo.ASCENDING), ('MerchantId', pymongo.ASCENDING)])
        except:
            self.logger.error("create index error: %s" % traceback.format_exc())

        self.user_redis = kwargs.get('UserRedis') #, redis.StrictRedis(host='localhost', port=6379, db=0))

        self._PlayerDataCache = dict()


    def sha1(self, message):
        if six.PY2:
            h = SHA1.new()
            h.update(message)
        else:
            h = SHA1.new(bytes(message, 'utf-8'))
        return h.hexdigest()


    def set_user_data(self, user_id, set_dict=None, inc_dict=None, set_insert_dict=None, fields=None, upsert=False):
        user_sha1 = self.sha1(user_id)
        query = {'ArkHash': user_sha1, "ArkId": user_id}
        update = {}
        fields = self.__filter_fields_type(fields)
        result = {}
        try:
            if set_dict:
                update["$set"] = set_dict
            if inc_dict:
                update["$inc"] = inc_dict
            if set_insert_dict:
                update["$setOnInsert"] = set_insert_dict

            if update:
                if pymongo.get_version_string().startswith("2."):
                    result = self.col_user_info.find_and_modify(query, update=update, new=True, fields=fields, upsert=upsert)
                else:
                    result = self.col_user_info.find_one_and_update(query, update, fields, upsert=upsert, return_document=pymongo.ReturnDocument.AFTER)

            return result

        except:
            self.logger.error("%s.%s %s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            self.logger.error("query: %s, set_dict: %s, inc_dict: %s, fields: %s" % (query, set_dict, inc_dict, fields))
            return None


    def set_game_data(self, user_id, set_dict=None, inc_dict=None, set_insert_dict=None, fields=None, upsert=False):
        query = {"ArkId": user_id}
        update = {}
        fields = self.__filter_fields_type(fields)
        result = {}
        try:
            if set_dict:
                update["$set"] = set_dict
            if inc_dict:
                update["$inc"] = inc_dict
            if set_insert_dict:
                update["$setOnInsert"] = set_insert_dict

            if update:
                if pymongo.get_version_string().startswith("2."):
                    result = self.col_game_data.find_and_modify(query, update=update, new=True, fields=fields, upsert=upsert)
                else:
                    result = self.col_game_data.find_one_and_update(query, update, fields, upsert=upsert, return_document=pymongo.ReturnDocument.AFTER)

            return result

        except:
            self.logger.error("%s.%s %s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            self.logger.error("query: %s, set_dict: %s, inc_dict: %s, fields: %s" % (query, set_dict, inc_dict, fields))
            return None

    def get_game_data(self, user_id, fields=None, bSecondary=True):
        query = {"ArkId": user_id}
        result = None
        col_game_data = self.col_game_data
        if not bSecondary:
            col_game_data = self.col_game_data.with_options(read_preference=pymongo.ReadPreference.PRIMARY)
        try:
            result = col_game_data.find_one(query, {"_id": 0})
        except:
            self.logger.error("%s.%s %s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            self.logger.error("query: %s, fields: %s" % (query, fields))
        return result

    def get_user_data(self, user_id, fields=None):
        cacheData = self._PlayerDataCache.get(str(user_id))
        if cacheData is not None and (fields is None or all(key in cacheData for key in fields)):
            return copy.copy(cacheData)
        user_sha1 = self.sha1(user_id)
        query = {'ArkHash': user_sha1, "ArkId": user_id}
        result = None
        try:
            result = self.col_user_info.find_one(query, {"_id": 0})
            self.set_user_data_cache(user_id, result)
        except:
            self.logger.error("%s.%s %s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            self.logger.error("query: %s, fields: %s" % (query, fields))
        return copy.copy(result)

    def get_user_data_cache(self, user_id):
        if self.user_redis is not None:
            key = "UserDataCache:{user_id}".format(user_id=user_id)
            if self.user_redis.info("server")['redis_version'] >= '6.2':
                data = json.loads(self.user_redis.getex(key, ex=UserManager.USER_DATA_CACHE_EXPIRE_TIME))
            else:
                data = json.loads(self.user_redis.get(key))
            return json.loads(data)
        return self._PlayerDataCache.get(str(user_id))

    def set_user_data_cache(self, user_id, data):
        if self.user_redis is not None:
            key = "UserDataCache:{user_id}".format(user_id=user_id)
            self.user_redis.set(key, value=json.dumps(data), ex=UserManager.USER_DATA_CACHE_EXPIRE_TIME)
        else:
            self._PlayerDataCache[str(user_id)] = data

    def clean_cached_user_data(self, ark_id):
        self._PlayerDataCache.pop(ark_id, None)

    def get_ark_id_by_uid(self, uid, from_type='bcompany'):
        try:
            query = {"ThirdPartyId": str(uid), "FromType": from_type}
            result = self.col_user_info.find_one(query, {"ArkId": 1, "_id": 0})
            if result:
                return result.get("ArkId", "")
            return ''
        except:
            self.logger.error("%s.%s %s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            self.logger.error("uid: %s" % (uid))

            return ''

    def get_playing_specific_game_users(self, api_game_id):
        try:
            return self.col_user_info.find({"CurrentGameID": str(api_game_id)}, fields=["ArkId"])
        except:
            self.logger.error(
                "%s.%s %s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            self.logger.error("api_game_id: %s" % (api_game_id))
        return None

    def add_guide(self, user_id, name, step):
        if name is None:
            return None
        upd = { 'Guide': {name:step} }
        return self.set_user_data(user_id, upd)

    def __filter_fields_type(self, fields):

        _fields = {"_id": False}

        if isinstance(fields, dict):
            _fields = fields
        elif isinstance(fields, list):
            _fields.update({key: True for key in fields})
        elif fields:
            _fields.update({str(fields): True})

        if fields:
            _fields.update({"_id": False})

        return _fields


    def clean_game_state(self):
        try:
            now_datetime = datetime.datetime.now()
            clean_end_date = now_datetime + datetime.timedelta(days=-1, hours=-GAME_STATE_CLEAN_TIME)
            clean_end_datetime = datetime.datetime(clean_end_date.year, clean_end_date.month, clean_end_date.day, GAME_STATE_CLEAN_TIME, 0, 0)
            end_timestamp = int(time.mktime(clean_end_datetime.timetuple()))

            del_query = {"update_time": {"$lt": end_timestamp}}
            result = self.col_game_state.remove(del_query)
            if result and result.get('ok', 0) == 1 and result.get('n', 0) > 0:
                self.logger.info('[UserManager][__clean_game_state] clean {} game state before time:{}'.format(result['n'], end_timestamp))

        except:
            self.logger.error("[UserManager][__clean_game_state] callstack:{}".format(traceback.format_exc()))
            return None

    def clean_player_data_cache(self):
        self._PlayerDataCache = {}
