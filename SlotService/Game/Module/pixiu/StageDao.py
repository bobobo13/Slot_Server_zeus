# -*- coding: utf-8 -*-
__author__ = 'feliciahu'

import sys, traceback
import datetime, time
import pymongo

from .StageDb import *
from ...Server.Database.DbConnectorEx import DbConnector


class StageDao:
    def __init__(self, DataSource=None, Logger=None, bInitDb=False, **kwargs):
        self._bDbLog = True
        self._Logger = Logger
        self._Connector = None
        self.DataSource = DataSource
        self.PlayerDao = kwargs.get('PlayerDao')
        if bInitDb:
            StageDb.Initialize(logger=self._Logger, DataSource=self.DataSource, FilePath="Script/Init/GameListNewSlot.csv")

    def LoadSetting(self):
        result = None
        try:
            collection = self.DataSource["StageSetting"].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            result = collection.find_one({}, self._Projection())
        except:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return result

    def LoadStage(self):
        try:
            collection = self.DataSource["Stage"].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find({}, self._Projection())
        except Exception as e:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None

        result = {}
        for i in cursor:
            result[(i['Type'],i['Name'])] = i
        return result

    def LoadInfo(self):
        try:
            collection = self.DataSource["StageInfo"].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find({}, self._Projection())
        except Exception as e:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None

        result = {}
        for i in cursor:
            if 'Channel' not in i:
                continue
            cv = (i['Channel'], i['Version'])
            if cv not in result:
                result[cv] = {}
            result[cv][(i['Type'], i['Name'])] = i['Overwrite']
        return result

    def GetPlayerData(self, ark_id):
        result = {'ark_id': ark_id, 'NickName': ark_id, 'Logo': '', 'KioskName': '', 'KioskId': 0}
        if self.PlayerDao is None:
            return {}
        Fields = ['Logo', 'NickName', 'KioskName', 'KioskId']
        r = self.PlayerDao.get_player_data(ark_id, fields=Fields)
        if r is not None:
            result['NickName'] = r.get('NickName', '')
            result['Logo'] = str(r.get('Logo', '')).lower()
            result['KioskName'] = r.get('KioskName', '')
            result['KioskId'] = r.get('KioskId', 0)
        return result

    def _LogError(self, s):
        if self._Logger is not None:
            self._Logger.error(s)

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj