#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "eitherwang"

import logging
from .Stage import Stage
from ..RoutineProc import RoutineProc
import six

class IgStage(Stage):
    def __init__(self, DataSource=None, Logger=None, bInitDb=False, **kwargs):
        self._tabKey = {
            "Name": {},
            "Id": {},
        }

        super(IgStage, self).__init__(DataSource, Logger, bInitDb=True, **kwargs)
        self.__Reload = RoutineProc("Stage", 60, self.Reload)

    def Reload(self, bForce=True):
        Stage.Reload(self, bForce)
        for key, stage in self._stageConfig.items():
            self._tabKey["Name"][str(stage["Name"])] = key
            self._tabKey["Id"][int(stage["GameId"])] = key

    def GetInfo(self, key):
        if type(key) == int:
            return self.GetInfoById(key)
        elif six.PY2 and type(key) in [str, unicode]:
            return self.GetInfoByName(key)
        elif six.PY3 and type(key) == str:
            return self.GetInfoByName(key)

    def GetInfoByName(self, strGameName):
        if strGameName not in self._tabKey["Name"]:
            return None
        return self._stageConfig[self._tabKey["Name"][strGameName]]

    def GetInfoById(self, nGameId):
        nGameId = int(nGameId)
        if nGameId not in self._tabKey["Id"]:
            return None
        return self._stageConfig[self._tabKey["Id"][nGameId]]

    def GetGameId(self, strGameName):
        info = self.GetInfoByName(strGameName)
        if info is None:
            return None
        return info["GameId"]

    def GetGameName(self, nGameId):
        info = self.GetInfoById(nGameId)
        if info is None:
            return None
        return info["Name"]

    def IsArkGame(self, key):
        info = self.GetInfo(key)
        if info is None:
            return None
        return info["Platform"] == "Ark"

    def GetGameList(self, strType=None, strPlatform=None):
        result = []
        gameData = self._stageConfig
        for key, info in gameData.items():
            if strType is not None and strType != info.get("Type"):
                continue
            if strPlatform is not None and strPlatform != info.get("Platform"):
                continue
            gtype, gname = key
            result.append(gname)
        return result

    def GetArkGameList(self, strType=None):
        return self.GetGameList(strType, strPlatform="Ark")

    def GetGameListByType(self, strType=None, strPlatform=None, bLobbyType=False):
        result = {}
        gameData = self._stageConfig
        for k in gameData.values():
            gameType, gameName, plat = k.get('Type'), k.get('Name'), k.get('Platform')
            if (gameType is None) or (gameName is None):
                continue
            if (strType is not None) and (strType != gameType):
                continue
            if (not bLobbyType) and (gameType == 'Lobby'):
                continue
            if (strPlatform is not None) and (strPlatform != plat):
                continue
            if gameType not in result:
                result[gameType] = []
            result[gameType].append(gameName)
        return result

    def GetMissionGameList(self, rule=None):
        result = []
        gameData = self._stageConfig
        for key, info in gameData.items():
            if not info.get("Enable", True):
                continue
            if info.get("Platform") != 'Ark':
                continue
            if info.get('Type') not in ['Fish', 'Slot']:
                continue
            d = {'Name':info.get('Name'), 'GameId':info.get('GameId'), 'Type':info.get('Type')}
            result.append(d)
        return result

    def GetMissionGameListByType(self):
        gameList = self.GetMissionGameList()
        result = {}
        for k in gameList:
            gameType, gameName = k['Type'], k['Name']
            if gameType not in result:
                result[gameType] = []
            result[gameType].append(gameName)
        return result

    def IsEnable(self, strGameName, ark_id=None, strChannel=None, strVersion=None):
        info = self.GetInfo(strGameName)
        if info is None:
            return None
        if any((strChannel, strVersion)):
            currentStageSwitch = self._GetCurrStageInfo(strChannel, strVersion)
            result = self._ReplaceStageInfo(currentStageSwitch)
            type = info["Type"]
            if (type, strGameName) not in result:
                return False
        return info.get("Enable", False)

    def GetStage(self, ark_id, strChannel, strVersion):
        currentStageSwitch = self._GetCurrStageInfo(strChannel, strVersion)
        result = self._ReplaceStageInfo(currentStageSwitch)

        rtn = {}
        for stage in result:
            typ, name = stage
            stageInfo = result[stage]
            is_default = False
            for defaultKey in [("", ""), (typ, "")]:
                if stage == defaultKey:
                    is_default = True
                    break
                if defaultKey in result:
                    stageInfo.update(result[defaultKey])
            if is_default:
                continue
            if not stageInfo["Enable"]:
                continue
            stageInfo.pop('Type', None)
            stageInfo.pop('Comment', None)
            if typ not in rtn:
                rtn[typ] = []
            rtn[typ].append(stageInfo)
            for stagetype in stageInfo["StageType"]:
                if type(stageInfo["StageType"]) == dict and (not stageInfo["StageType"][stagetype]):
                    continue
                if stagetype not in rtn:
                    rtn[stagetype] = []
                rtn[stagetype].append(result[stage])
        for stagetype in rtn:
            rtn[stagetype].sort(key=lambda x: x["Sequence"])
        return rtn

    def _GetCurrStageInfo(self, strChannel, strVersion):
        emptyDict = {}
        result = {}
        channelSplit = strChannel.split("_")
        for lvl in range(len(channelSplit)+1):
            key = "_".join(channelSplit[:lvl])
            result.update(self._stageInfo.get((key, ""), emptyDict))
            result.update(self._stageInfo.get((key, strVersion), emptyDict))
        return result

    def GetMultiLangTitle(self, strGameName, strLang):
        info = self.GetInfoByName(strGameName)
        if info is None or 'Title' not in info:
            return strGameName
        return info['Title'].get(strLang, info['Title'].get('en-us', strGameName))

if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger("Test")
    logger.setLevel(logging.DEBUG)

    stage = IgStage(Logger=logger, FilePath="GameList.csv", bDropDb=True)
    r = stage.GetGameList()
    print(r)

    r = stage.GetArkGameList("Slot")
    print(r)

    r = stage.GetInfoByName("FuXingGaoZhao")
    print(r)

    r3 = stage.GetInfoByName("FuXingGaoZhao")
    print(r3)
    r4 = stage.GetInfo("FuXingGaoZhao")
    print(r4)
    r4 = stage.GetInfoById(5217)
    print(r4)


    assert stage.IsArkGame("FuXingGaoZhaoJP") == False
    assert stage.IsEnable("FuXingGaoZhaoJP") == True