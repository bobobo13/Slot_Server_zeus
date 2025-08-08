#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import pymongo
import sys
import traceback
from .RoutineProc import RoutineProc

class FunctionSwitch(RoutineProc):
    def __init__(self, logger, DataSource, bInitDb=False):
        self._bDbLog = True
        self._Logger = logger
        super(FunctionSwitch, self).__init__("FunctionSwitch", 30, func=self._reload, logger=logger)
        self.DataSource = DataSource
        if bInitDb:
            FunctionSwitch.Initialize(DataSource=self.DataSource)
        self._reload()
        # self.EnableJpWinIndependent(待確認)

    def _reload(self):
        self.fs_setting = self.load_setting()

    def get_fs_setting(self, game_name, platform_fs_data, group_name=""):
        fs_data = self.fs_setting.get(group_name)
        result = {}
        for k, v in fs_data.items():
            if k == "EnableGame":
                result[k] = game_name in v
                continue
            result[k] = v and platform_fs_data.get(k, False)
        return result

    def get_fs_setting_without_platform(self, game_name, group_name="", key=None):
        fs_data = self.fs_setting.get(group_name)
        result = copy.copy(fs_data)
        result["EnableGame"] = game_name in fs_data["EnableGame"]
        if key:
            return result.get(key, False)
        return result

    def load_setting(self):
        try:
            collection = self.DataSource['FunctionSwitchSetting']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find({}, self._Projection())
            # cursor = self.DataSource['FunctionSwitchSetting'].find({}, self._Projection(), read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
        except Exception as e:
            self._Logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return {}
        if cursor is None:
            return {}
        result = {}
        for i in cursor:
            result[i.pop('GroupName')] = i
        return result

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj

    def Initialize(strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', DataSource=None):
        if DataSource is None:
            return None
        # Setting
        setting = DataSource['FunctionSwitchSetting']
        setting.create_index([('GroupName', pymongo.ASCENDING)], unique=True)
        upd = {
            "EnableGame": ["LionDanceLegi", "CashKing", "Razor"],
            "EnableTestMode": False,
            "EnableBuffer": True,
            "EnablePreview": True,
            "EnableProbSwitch": True,
            "EnableBuyBonus": True,
            "EnableFreeGameCardBonus": True,
            "EnableDisconnectSettle": True,
            "EnableJpWinIndependent": True
        }
        setting.update_one({'GroupName': 'default'}, {'$setOnInsert': upd}, upsert=True)

        upd = {
            "EnableGame": ["LionDanceLegi", "CashKing", "Razor"],
            "EnableTestMode": True,
            "EnableBuffer": True,
            "EnablePreview": True,
            "EnableProbSwitch": True,
            "EnableBuyBonus": True,
            "EnableFreeGameCardBonus": True,
            "EnableSettleSpecialGame": True,
            "EnableJpWinIndependent": True
        }
        setting.update_one({'GroupName': 'test'}, {'$setOnInsert': upd}, upsert=True)


