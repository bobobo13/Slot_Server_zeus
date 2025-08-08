# -*- coding: utf-8 -*-
from ..Server.Database.DbConnectorEx import DbConnector
import pymongo
import sys, traceback
from .SlotBonusDb import SlotBonusDb


class SlotBonusDao:
    def __init__(self, DataSource=None, Logger=None, bInitDb=True, **kwargs): # kwargs = { ConfigFile:'', 'Host':'localhost', 'Port':27017, 'User':'', 'Password'='', 'DbName':'Capsule', 'PoolSize':30 }
        self.name = kwargs.get('Name', "SlotBonus")
        self._bDbLog = True
        self._Logger = Logger
        self._Connector = None
        self.commonLogExFunc = kwargs.get("CommonLogExFunc")
        self.DataSource = DataSource
        if self.DataSource is None:
            nam, host, port, user, pwd, ps = self.name, 'localhost', 27017, '', '', 30  # default
            # from configure file
            strConfigFile = kwargs.get('ConfigFile')
            strSession = kwargs.get('Session', 'SlotGame')
            cfg = DbConnector.GetDbConfig(strConfigFile, strSession)
            if cfg is not None:
                nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigFile, strSession, cfg)
            # from arguments
            strDbName, strHost, nPort = kwargs.get('DbName', nam), kwargs.get('Host', host), kwargs.get('Port', port)
            strUser, strPassword, nPoolSize = kwargs.get('User', user), kwargs.get('Password', pwd), kwargs.get('PoolSize', ps)
            # connect database
            self._Connector = DbConnector(strDbName, strHost, nPort, strUser, strPassword, nPoolSize)
            self.DataSource = self._Connector.DataSource

        if bInitDb:
            SlotBonusDb.Initialize(data_source=self.DataSource)
            SlotBonusDb.init_game_setting_and_info(data_source=self.DataSource)
            # SlotBonusDb.init_slot_config(data_source=self.DataSource)

    def load_setting(self):
        try:
            collection = self.DataSource['BonusSetting']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            result = collection.find_one({}, self._Projection())
        except Exception as e:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return result

    def load_info(self):
        try:
            collection = self.DataSource['BonusInfo']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            result = collection.find({}, self._Projection())
        except Exception as e:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return result

    def load_model(self):
        cursor = None
        try:
            collection = self.DataSource['BonusModel']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find({}, self._Projection())
        except Exception as e:
            self._LogError(
                "%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        if cursor is None:
            return {}
        result = {}
        for i in cursor:
            if 'Name' not in i:
                continue
            # if 'BetList' in i:
            #     i['BetList'] = [dict(i['DefBetList'], **b) for b in i['BetList']]
            result.update({i['GameName']: [i]}) if i['GameName'] not in result else result[i['GameName']].append(i)
        return result

    def load_slot_config(self):
        try:
            collection = self.DataSource['BonusSlotConfig']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            result = collection.find({}, self._Projection())
        except Exception as e:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return result

    # def load_game_info(self):
    #     try:
    #         cursor = self.DataSource['BonusGameInfo'].find({},{'_id':0}, read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
    #     except Exception as e:
    #         self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
    #         return {}
    #     if cursor is None:
    #         return {}
    #     result = {}
    #     for i in cursor:
    #         if i['GameName'] not in result:
    #             result[i['GameName']] = {}
    #         result[i['GameName']][i.get('ProbId', "")] = i
    #     return result

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj

    def _LogError(self, s):
        if self._Logger is not None:
            self._Logger.error(s)

    def DbLog(self, strArkId, name, type, gameNo, serialNo, bet, extrabet, cost, probId='', **kwargs):
        upd={'GameName': name, 'Type': type, 'GameNo': gameNo, 'SerialNo': serialNo, 'Bet': bet, 'ExtraBet': extrabet, 'Cost':cost, 'ProbId': probId}
        upd.update(kwargs)
        self.commonLogExFunc(strArkId, upd, strColName=self.name, bSendSplunk=False)

if __name__ == "__main__":
    import logging
    # from Common.PlatformUtil import PlatformUtil
    logger = logging.getLogger("test")
    ConfigPath = 'D:\\SVN_FILE\\iGaming\\trunk\\Server\\H5\\pixiu\\Game/config/local/'
    # platformUtil = PlatformUtil(logger)
    slotBonusDao = SlotBonusDao(Logger=logger, strConfigPath=ConfigPath)
    # _tabSetting = iGamingLoginDao.LoadSetting()
    # group = 'gw99'
    # version = platformUtil.GetVersion(group.lower())
    # setting = _tabSetting.get(version.lower(), _tabSetting.get('default'))
    result = slotBonusDao.LoadModel()

    print(result)
