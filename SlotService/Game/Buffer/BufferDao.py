# -*- coding: utf-8 -*-
import pymongo, sys, traceback
from ..Server.Database.DbConnector import DbConnector
import time
from ..Module.MathTool import *
from .BufferDb import BufferDb

class BufferDao(object):
    INCR_INTERVAL = 30

    def __init__(self, DataSource=None, Logger=None, bInitDb=True, **kwargs):
        self.name = kwargs.get('Name')
        self._bDbLog = True
        self._Logger = Logger
        self.LogDao = kwargs.get('logDao')
        self.DataSource = DataSource

        if self.DataSource is None:
            nam, host, port, user, pwd, ps = 'GameRate', 'localhost', 27017, '', '', 30  # default
            strConfigFile = kwargs.get('ConfigFile')
            strSession = kwargs.get('Session', 'GameRate')
            cfg = DbConnector.GetDbConfig(strConfigFile, strSession)
            if cfg is not None:
                nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigFile, strSession, cfg)
            # from arguments
            strDbName, strHost, nPort = kwargs.get('DbName', nam), kwargs.get('Host', host), kwargs.get('Port', port)
            strUser, strPassword, nPoolSize = kwargs.get('User', user), kwargs.get('Password', pwd), kwargs.get('PoolSize', ps)
            # connect database
            self._Connector = DbConnector(strDbName, strHost, nPort, strUser, strPassword, nPoolSize)
            self.DataSource = self._Connector.DataSource

        if self.DataSource is None:
            self._Logger.error("%s.%s %s game_db not init" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return

        self._setting = None
        self._buffer_value_cache = dict()

        file_path = kwargs.get("FilePath")
        if bInitDb:
            BufferDb.Initialize(logger=self._Logger, DataSource=self.DataSource, buffer_name=self.name, FilePath=file_path)


    def load_setting(self):
        coll = self.DataSource["{}Setting".format(self.name)].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
        try:
            result = coll.find({}, self._Projection())
        except Exception as e:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        self._setting = {(doc["GameName"], doc["Version"]): doc for doc in result}
        return result

    def get_buffer_setting(self, gameName, version, group):
        if self._setting is None:
            self.load_setting()
        if self._setting is None:
            return None
        return self._setting.get((gameName, version))


    def get_buffer_value(self, gameName, version, group):
        if (gameName, version, group) not in self._buffer_value_cache:
            self._buffer_value_cache[(gameName, version, group)] = {"addValue": 0, "bet": 0, "win": 0, "currentValue": self._get_buffer_value(gameName, version, group), "lastUpdateTime": 0}
        return self._buffer_value_cache[(gameName, version, group)]["currentValue"] + self._buffer_value_cache[(gameName, version, group)]["addValue"]

    def _get_buffer_value(self, gameName, version, group):
        coll = self.DataSource["{}Value".format(self.name)].with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
        ret = None
        result = 0
        qry = {"GameName": gameName, "Version": version, "Group": group}
        try:
            ret = coll.find_one(qry)
        except:
            self._Logger.error("[SlotBufferDao db_get_buffer_value, Logo={}, KioskId={}, GameName={}, Exception={}".format(version, group, gameName, traceback.format_exc()))
        if ret is not None:
            result = ret['Value']
        return result

    def incr_buffer_value(self, gameName, version, group, addValue, bet, win):
        if (gameName, version, group) not in self._buffer_value_cache:
            self._buffer_value_cache[(gameName, version, group)] = {"addValue": 0, "bet": 0, "win": 0, "currentValue": self._get_buffer_value(gameName, version, group), "lastUpdateTime": 0}
        incr_buffer = self._buffer_value_cache[(gameName, version, group)]
        incr_buffer["addValue"] += addValue
        incr_buffer["bet"] += bet
        incr_buffer["win"] += win
        incr_buffer["lastUpdateTime"] = int(time.time())
        return True

    def update_buffer_value(self):
        for key in self._buffer_value_cache:
            incr_buffer = self._buffer_value_cache[key]
            if incr_buffer["addValue"] == 0:
                continue
            if incr_buffer["lastUpdateTime"] + self.INCR_INTERVAL > int(time.time()):
                continue

            add_value, incr_buffer["addValue"] = incr_buffer["addValue"], 0
            bet, incr_buffer["bet"] = incr_buffer["bet"], 0
            win, incr_buffer["win"] = incr_buffer["win"], 0
            incr_buffer["currentValue"] = self._upd_buffer_value(key[0], key[1], key[2], add_value, bet, win)
            incr_buffer["lastUpdateTime"] = int(time.time())


    def _upd_buffer_value(self, gameName, version, group, addValue, bet, win):
        col = "{}Value".format(self.name)
        result = None
        qry = {"GameName": gameName, "Version": version, "Group": group}
        upd = {}
        upd["$inc"] = {"Value": floor_float(addValue, 6), "BetAmount": floor_float(bet, 6), "WinAmount": floor_float(win, 6)}
        upd["$set"] = {"UpdateTime": int(time.time())}
        try:
            result = self.DataSource[col].find_one_and_update(qry, update=upd, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
        except Exception as e:
            self._Logger.error("[BufferDao] _upd_buffer_value, Logo={}, KioskId={}, GameName={}, Exception={}".format(version, group, gameName, traceback.format_exc()))
        return result["Value"]

    def _LogError(self, s):
        if self._Logger is not None:
            self._Logger.error(s)

    def DbLog(self, version, group, game_name, ark_id, buffer_value, no_win_gate, game_rate, max_win, trigger_no_win, buffer_lv_gate, **kwargs):
        col = "{}Log".format(self.name)
        if self.LogDao is None:
            return
        upd = {'Version': version, 'Group': group, 'GameName': game_name, 'ark_id': ark_id}
        upd.update({'BufferValue': buffer_value, 'NoWinGate': no_win_gate, 'GateRate': game_rate})
        upd.update({'MaxWin': max_win, 'TriggerNoWin': trigger_no_win})
        upd.update({'BufferGateLv': buffer_lv_gate})
        upd.update(kwargs)

        try:
            self.LogDao.CommonLog(upd, col)
        except Exception as e:
            self._LogError("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj