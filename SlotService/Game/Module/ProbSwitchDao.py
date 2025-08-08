# -*- coding: utf-8 -*-
import time

import pymongo, sys, traceback
from .ProbSwitchDb import ProbSwitchDb

class ProbSwitchDao:
    def __init__(self, DataSource=None, logger=None, bInitDb=True, **kwargs):  # kwargs = { ConfigFile:'', 'Host':'localhost', 'Port':27017, 'User':'', 'Password'='', 'DbName':'Capsule', 'PoolSize':30 }
        self._bDbLog = True
        self._Logger = logger
        self.DataSource = DataSource

        if self.DataSource is None:
            self._Logger.error("%s.%s %s game_db not init" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return

        if bInitDb:
            ProbSwitchDb.Initialize(DataSource=self.DataSource)

    def load_setting(self):
        try:
            collection = self.DataSource['ProbSwitchSetting']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find({}, self._Projection())
        except Exception as e:
            self._Logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return {}
        if cursor is None:
            return {}
        result = {}
        for i in cursor:
            result[i['GameName']] = i
        return result

    def get_prob_data(self, ark_id, game_name):
        result = None
        try:
            collection = self.DataSource['ProbSwitchValue']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            result = collection.find_one({'ark_id': ark_id, 'GameName': game_name}, self._Projection())
        except Exception as e:
            self._Logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return result
        return result

    def init_prob_data(self, ark_id, game_name, group_name, default_prob_id=None, is_default_type=False):
        result = None
        upd = {}
        qry = {'ark_id': ark_id, 'GameName': game_name}
        # init_data = {'Value': 0, 'UpdateTime': int(time.time())}
        doc = {"Group": group_name}
        if is_default_type and default_prob_id is not None:
            doc["DefaultProbId"] = default_prob_id
        upd["$set"] = doc
        # upd["$setOnInsert"] = init_data
        try:
            collection = self.DataSource['ProbSwitchValue']
            result = collection.update_one(qry, upd, upsert=True)
        except Exception as e:
            self._Logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return (result is not None)

    def inc_prob_data(self, ark_id, game_name, prob_type):
        quy = {'ark_id': ark_id, 'GameName': game_name}
        result = None
        upd={}
        upd['$inc'] = {"{}Value".format(prob_type): 1}
        upd['$set'] = {"UpdateTime": int(time.time())}
        try:
            collection = self.DataSource['ProbSwitchValue']
            collection.find_and_modify(query=quy, update=upd, upsert=True)
        except Exception as e:
            self._Logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return (result is not None)

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj
