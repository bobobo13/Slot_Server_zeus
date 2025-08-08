# -*- coding: utf-8 -*-
__author__ = 'eitherwang'
import importlib
import traceback
from SlotServer.Common.RoutineProc import RoutineProc
from . import BaseIngameJp
from . import IngameJpDao

class InGameJpMgr(object):
    UPDATE_TIME = 3
    def __init__(self, logger, **kwargs):
        self._jpMod = dict()
        self.logger = logger
        self._MongoDao = kwargs.get("MongoDao") or IngameJpDao.IngameJpMongoDao(**kwargs)
        self._PoolDao = kwargs.get("PoolDao") or IngameJpDao.IngameJpPoolDao(**kwargs)
        # self._tabGameInfo = kwargs.get("GameInfo")
        # self._tabGameInfo = dict()
        self.parseGroup = kwargs.get("ParseGroup", self._parseGroup)
        self._tabJpInfo = dict()
        self._routineproc = RoutineProc("IngameJp", InGameJpMgr.UPDATE_TIME, self.update, logger=self.logger)

    def load_jp_module(self, tabGameSetting, **kwargs):
        for game_id, gameSetting in tabGameSetting.items():
            if "progressive_level_num" not in gameSetting:
                continue
            if game_id in self._jpMod:
                continue
            module_path = 'Slot.' + game_id + '.' + game_id + 'IngameJp'
            try:
                mod = importlib.import_module(module_path)
                self._jpMod[game_id] = eval("mod.{}IngameJp".format(game_id))(game_id, gameSetting, self.logger, MongoDao=self._MongoDao, PoolDao=self._PoolDao, **kwargs)
            except ImportError:
                self._jpMod[game_id] = BaseIngameJp.BaseIngameJp(game_id, gameSetting, self.logger, MongoDao=self._MongoDao, PoolDao=self._PoolDao, **kwargs)

            except:
                self.logger.error(traceback.format_exc())
                self.logger.error('[IngameJP] Failed to initialize Calculator (game_id={})'.format(game_id))
            self._jpMod[game_id].updJpInfo("", "", gameSetting["jp_info"])

    def update(self):
        self._PoolDao.update()

    def __getattr__(self, name):
        # if name in self.__dict__:
        #     return self.__dict__[name]
        return self.dispatchMethod(name)

    def dispatchMethod(self, funcName):
        def wrapper(game_id, Group, *args, **kwargs):
            if not self.is_ingame_jp_game(game_id):
                raise Exception("GameID:{} is not IngameJp!".format(game_id))
            # if funcName not in self._calculators[game_id].__dict__:
            #     raise Exception("funcName:{} not exist!".format(funcName))
            strInfoKey, strPoolGroup = self.parseGroup(Group)
            return getattr(self._jpMod[game_id], funcName)(strInfoKey, strPoolGroup, *args, **kwargs)
        return wrapper

    def is_ingame_jp_game(self, game_id):
        return game_id in self._jpMod

    def _parseGroup(self, Group):
        return "", Group

    def getAllJpStatus(self, Channel):
        all_jp_status = {}
        for game_id in self._jpMod:
            strInfoKey, strPoolGroup = self.parseChannel(Channel)
            all_jp_status[game_id] = getattr(self._jpMod[game_id], "GetJpStatus")(strInfoKey, strPoolGroup)
        return all_jp_status

