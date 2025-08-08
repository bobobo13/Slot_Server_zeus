# -*- coding: utf-8 -*-
__author__ = "eitherwang"

import datetime
import time
import gevent
import traceback
from .TimerTrigger import MongoTimerTrigger as TimerTrigger
from .RoutineProc import RoutineProc
import inspect

class DisconnectSettle(RoutineProc):
    def __init__(self, Logger, SlotDao, DataSource=None, **kwargs):
        self._Logger = Logger
        self._SlotDao = SlotDao
        self._SettleDao = DisconnectSettleDao(Logger, DataSource)
        self.TimerTrigger = TimerTrigger(DataSource)
        self._do_fever_all_func = kwargs.get("DoFeverAllFunc")
        self._settle_player_game_func = kwargs.get("SettlePlayerGameFunc")
        self._save_game_state_func = kwargs.get("SaveGameStateFunc")
        self._force_lock_game_state_func = kwargs.get("ForceLockGameStateFunc")

        self.TimerTrigger.Register("SlotGameSettle", self._TimerTriggerForceSettle)
        self._setting = None
        self._Enable = False
        self._SETTLE_WAIT_SECONDS = 60
        while True:
            self._LoadSetting()
            if self._setting is not None:
                break
            time.sleep(0.1)
        if kwargs.get("RegOnKickPlayer"):
            kwargs["RegOnKickPlayer"](self.ForceSettle)

        self.TimerTrigger.start_later(5)
        self._next_reload = {
            'load_setting': {'Func': self._LoadSetting, 'NextReload': 0, 'Interval': 60},
            'load_fever_client_action': {'Func': self._LoadFeverClientAction, 'NextReload': 0, 'Interval': 60},
        }
        super(DisconnectSettle, self).__init__("DisconnectSettleReload", 60, self.Reload, logger=Logger)
        self.Reload(True)

    def IsEnable(self):
        return self._Enable

    def Reload(self, bForce=False):
        now = time.time()
        for func_name, item in self._next_reload.items():
            func, _fNextReload, interval = item['Func'], item['NextReload'], item['Interval']
            if (not bForce) and (_fNextReload > 0) and _fNextReload > now:
                continue
            item['NextReload'] = int(now) + int(interval)
            func()

    def _LoadSetting(self):
        NewSetting = self._SettleDao.LoadSetting()
        if NewSetting is None:
            return
        self._setting = NewSetting
        self._Enable = NewSetting["Enable"]
        self._BestMode = NewSetting["BestMode"]
        self._SETTLE_WAIT_SECONDS = NewSetting["SettleWaitSeconds"]
        self._MaxNextFeverCallTimes = NewSetting["MaxNextFeverCallTimes"]
        self._bSaveResultAsync = NewSetting["SaveResultAsync"]
        # self._FEVER_DEFAULT_ACTION = NewSetting["FeverDefaultAction"]

    def _LoadFeverClientAction(self):
        self.fever_client_action =  self._SettleDao.LoadFeverClientAction()

    def ForceSettle(self, ark_id):
        if not self.IsEnable():
            return
        self.TimerTrigger.ForceTrigger(ark_id, "DisconnectSettle")

    def OnStartGame(self, ark_id, game_name, lock):
        if not self.IsEnable():
            return
        if lock < 0:    # already in settle
            return

        self.CleanTimer(ark_id)
        self.DoSettle(ark_id, game_name)

    def cmd_after(self, ark_id, game_name, can_settle_now=False):
        """
        Called when spin/next_fever enter special game
        can_settle_now = (len(ret_user_game_state["current_sg_id"]) <= 0)
        """
        if not self.IsEnable():
            return
        if not can_settle_now:
            self.TimerTrigger.Set(ark_id, "DisconnectSettle", self._SETTLE_WAIT_SECONDS, data={"GameName": game_name})
            return
        gevent.spawn(self._SettleAndSaveResult, ark_id, game_name, is_save_result=True)
        gevent.spawn(self.CleanTimer, ark_id)

    def _SettleAndSaveResult(self, ark_id, GameName, is_save_result=False):
        self.DoSettle(ark_id, GameName, is_save_result)

    def _TimerTriggerForceSettle(self, ark_id, data):
        GameName = data["GameName"]
        self.DoSettle(ark_id, GameName)

    def do_fever_all(self, ark_id, game_name):
        fever_action = self.fever_client_action.get(game_name) or self.fever_client_action.get("default")
        if fever_action is None:
            self._Logger.error("[DisconnectSettle] Cannot Get FeverClientAction. ark_id={}, GameName={}".format(ark_id, game_name))
            return
        code = self._do_fever_all_func(self, ark_id, game_name, fever_action)
        if code != 0:
            return code
        # self._SaveGameStateEndSettle(ark_id, game_name, GameState)
        return

    def DoSettle(self, ark_id, game_name, is_save_result=False):
        game_state = self._GetLockGameState(ark_id, game_name)
        if game_state is None:
            return
        if len(game_state["current_sg_id"]) > 0:  # game_state 在特殊遊戲需要幫忙產完後續盤面
            self.do_fever_all(ark_id, game_name)

        self._settle_player_game_func(ark_id, game_name, is_save_result)
        self._SaveGameStateEndSettle(ark_id, game_name, game_state)
        return

    def _SaveGameStateEndSettle(self, ark_id, game_name, game_state):
        return self._save_game_state_func(ark_id, game_name, game_state, bet_type="DISCONNECT")

    def _CanSettleNow(self):
        if self._BestMode not in ["ReplayOnly"]:
            return False
        return True

    def _GetLockGameState(self, ark_id, game_name):
        user_game_state = self._force_lock_game_state_func(ark_id, game_name)
        if user_game_state is None:
            self._Logger.error("[DisconnectSettle] Cannot GetLockGameState. ark_id={}, GameName={}".format(ark_id, game_name))
            return None
        return user_game_state

    def CleanTimer(self, ark_id):
        self.TimerTrigger.Unset(ark_id, "DisconnectSettle")

import pymongo
class DisconnectSettleDao():
    def __init__(self, Logger, DataSource, **kwargs):
        self._Logger = Logger
        self._DataSource = DataSource
        self._LogDataSource = kwargs.get("LogDataSource", DataSource)
        DisconnectSettleDb.Initialize(DataSource=self._DataSource, LogDataSource=self._LogDataSource)

    def LoadSetting(self):
        try:
            collection = self._DataSource["SettleSetting"]
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find()
        except:
            self._Logger.error("%s.%s %s " % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return None
        if cursor is None:
            return None
        return cursor[0]

    def LoadFeverClientAction(self):
        result = {}
        try:
            collection = self._DataSource["SettleFeverClientAction"]
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find()
        except:
            self._Logger.error("%s.%s %s " % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return None
        if cursor is None:
            return None
        for doc in cursor:
            result[doc["GameName"]] = doc['FeverClientAction']
        return result

    def SaveErrorGameState(self, ark_id, strGameName, originalGameState, resultCode):
        data = {"ark_id": ark_id, "GameName": strGameName, "_ResultCode": resultCode, "_Time": datetime.datetime.now()}
        data.update(originalGameState)
        try:
            self._LogDataSource["ErrorGameState"].insert(data)
        except:
            self._Logger.error("%s.%s %s user_id: %s, game_id: %s, " % (str(self.__class__.__name__), get_function_name(), traceback.format_exc(), ark_id, strGameName))

class DisconnectSettleDb:
    @staticmethod
    def Initialize(strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', DataSource=None, LogDataSource=None):
        # if DataSource is None:
        #     DataSource = DbConnector.Connect(strDbName, strHost, nPort, strUser, strPassword)
        if DataSource is None:
            return None
        if LogDataSource is None:
            return None
        DataSource["SettleSetting"].update_one({}, {"$setOnInsert": {"Enable": True, "BestMode": "ReplayOnly", "SettleWaitSeconds": 60, "MaxNextFeverCallTimes": 200, "SaveResultAsync":True}, }, upsert=True)

        LogDataSource["ErrorGameState"].create_index([("ark_id", pymongo.ASCENDING), ("GameName", pymongo.ASCENDING)])
        LogDataSource["ErrorGameState"].create_index([("_Time", pymongo.ASCENDING)], expireAfterSeconds=30 * 24 * 60 * 60)  # 30 days

        DataSource["SettleFeverClientAction"].create_index([("GameName", pymongo.ASCENDING)])
        DataSource["SettleFeverClientAction"].update_one({"GameName": "default"}, {"$setOnInsert": {"FeverClientAction": {"0": 0, "1": 0 , "2": 0 , "3": 0 }}}, upsert=True)

def get_function_name():
    frame = inspect.currentframe()
    return frame.f_code.co_name if frame else "Unknown"