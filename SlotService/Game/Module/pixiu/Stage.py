# -*- coding: utf8 -*-
__author__ = 'feliciahu'

import copy
import datetime, time

import gevent
from .StageDao import StageDao


class Stage(object):
    def __init__(self, DataSource=None, Logger=None, bInitDb=False, **kwargs):
        self.Logger = Logger
        self.Dao = StageDao(DataSource, self.Logger, bInitDb, **kwargs)
        self._stageConfig = None
        self._fNextReload = 0
        while self._stageConfig is None:
            self.Reload()
            gevent.sleep(0.1)
        self.Logger.info('Stage init ok.')

    def Update(self):
        # if not self.Enable:
        #     return
        self.Reload(False)

    def Reload(self, bForce=True):
        if (not bForce) and (self._fNextReload > time.time()):
            return
        self._fNextReload = time.time() + 60

        self._stageConfig = self.Dao.LoadStage()
        self._stageInfo = self.Dao.LoadInfo()
        self._stageSetting = self.Dao.LoadSetting()

    def _GetCurrStageInfo(self, strChannel, strVersion):
        # strChannel = "" if strChannel not in self._stageInfo else strChannel
        # strVersion = "" if strVersion not in self._stageInfo.get(strChannel, {}) else strVersion
        # return self._stageInfo.get(strChannel, {}).get(strVersion, {})
        result = self._stageInfo.get((strChannel, strVersion), None)
        if result is None:
            result = self._stageInfo.get((strChannel, ""), None)
        if result is None:
            result = self._stageInfo.get(("", strVersion), None)
        if result is None:
            result = self._stageInfo.get(("", ""), None)
        return result

    def _ReplaceStageInfo(self, currentStageSwitch, originInfo=None):
        if originInfo is None:
            originInfo = self._stageConfig
        result = {stage: copy.copy(originInfo[stage]) for stage in originInfo}
        if currentStageSwitch is None:
            return result
        for stage in currentStageSwitch:
            if stage[1] == "" and stage not in result:
                result[stage] = {}
            result[stage].update(currentStageSwitch[stage])
        return result

    # Command
    def GetStage(self, ark_id, strChannel, strVersion):
        currentStageSwitch = self._GetCurrStageInfo(strChannel, strVersion)
        result = self._ReplaceStageInfo(currentStageSwitch)

        rtn = {}
        for stage in result:
            typ, name = stage
            if typ not in rtn:
                rtn[typ] = []
            result[stage].pop('Type', None)
            result[stage].pop('Comment', None)
            rtn[typ].append(result[stage])
        return rtn

    def GetStageType(self, ark_id):
        result = self._stageSetting.get('Type',None)
        return result