#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import traceback
import time
import os
import logging
from ..Module.MathTool import *
from .BufferDao import BufferDao
from ..Module.RoutineProc import RoutineProc

class Buffer(RoutineProc):
    CONFIG = "game.cfg"
    UPDATE_INTERVAL = 30
    MIN_WIN = 0

    def __init__(self, Logger, DataSource=None, **kwargs):
        self.logger = Logger
        name = kwargs.get('Name', "")
        # BuyBonusBuffer
        self.name = "{}{}".format(name, "Buffer")
        kwargs['Name'] = self.name

        self.BufferDao = BufferDao(DataSource, self.logger, True, ConfigFile=kwargs.get('ConfigPath', "") + Buffer.CONFIG, **kwargs)  # Init Database
        self.isTestEnv = kwargs.get("IsTestEnv", False)
        self._Enable = True

        self.GetPlayerDataFunc = kwargs.get('GetPlayerDataFunc')
        self.get_info_func = kwargs.get('GetInfoFunc', self._GetInfo)
        info_register_func = kwargs.get('InfoRegisterFunc')
        if info_register_func is not None:
            info_register_func(self.name, self.BufferDao.load_setting)

        self.getArkGameListFunc = kwargs.get('GetArkGameListFunc')
        self._fNextReload = 0
        self.logger.info("[{}] init".format(self.name))
        super(Buffer, self).__init__(self.name, Buffer.UPDATE_INTERVAL, func=self.Reload, logger=Logger)

    def get_gate_and_max_win(self, version, group, game_name, ark_id, nTotalBet=0, balance=None, **kwargs):
        no_win_gate, max_win = 0, 1000
        if version is None or group is None:
            self.logger.error("[{}][get_gate_and_max_win] version={}, group={}, game_name={}, version/group failed. Exception={}".format(self.name, version, group, game_name, traceback.format_exc()))
            return None, None
        if ark_id is None:
            self.logger.error("[{}][get_gate_and_max_win] version={}, group={}, game_name={}, get ark_id failed. Exception={}".format(self.name, version, group, game_name, traceback.format_exc()))
            return None, None

        player_data = kwargs.get('player_data')
        if player_data is None:
            player_data = self.GetPlayerDataFunc(ark_id)
        if player_data is None:
            self.logger.error("[{}][get_gate_and_max_win] version={}, group={}, game_name={}, get player_data failed. Exception={}".format(self.name, version, group, game_name, traceback.format_exc()))
            return None, None
        player_data = dict(player_data, GameName=game_name)

        buffer_setting = self.get_info_func(None, self.name, userData=player_data, bWarningLog=False)
        if buffer_setting is None:
            self.logger.debug("[{}] version:{}, group:{}, gameName:{} not in bufferSetting.".format(self.name, version, group, game_name))
            if self.isTestEnv:
                return no_win_gate, max_win
            return None, None

        if not self._Enable or not buffer_setting['Enable']:
            self.logger.warning("[{}] Enable false".format(self.name))
            return no_win_gate, buffer_setting['MaxWin']

        buffer_value = self.BufferDao.get_buffer_value(game_name, version, group)

        trigger_no_win, ctrl_lv, no_win_gate, max_win, buffer_gate_lv = self._check_buffer_control_level(buffer_setting, buffer_value)

        # 保證最小一個TotalBet不會砍牌
        max_win = max_win if max_win >= nTotalBet else nTotalBet

        if no_win_gate > 0:
            self.BufferDao.DbLog(version, group, game_name, ark_id, buffer_value, no_win_gate, kwargs.get("rtp"), max_win, trigger_no_win, buffer_gate_lv)

        log_msg = "[{}] {}:{}:{}, buffer_value={}, no_win_gate={}, max_protect_win={}, trigger_no_win={}, buffer_gate_lv={}".format(self.name, version, group, game_name, buffer_value, no_win_gate, max_win, trigger_no_win, buffer_gate_lv)
        self.logger.log(logging.WARN if trigger_no_win else logging.INFO, log_msg)
        return no_win_gate, max_win

    def incr_buffer(self, version, group, game_name, bet, win, buffer_rate=None, **kwargs):
        ark_id = kwargs.get('ark_id')
        if version is None or group is None:
            self.logger.error("[{}][incr_buffer] version={}, group={}, game_name={}, version/group failed. Exception={}".format(self.name, version, group, game_name, traceback.format_exc()))
            return
        player_data = kwargs.get('player_data')
        if player_data is None:
            if ark_id is None:
                self.logger.error("[{}][incr_buffer] version={}, group={}, game_name={},  no ark_id.".format(self.name, version, group, game_name))
                return
            player_data = self.GetPlayerDataFunc(ark_id)
        if player_data is None:
            self.logger.error("[{}][incr_buffer] version={}, group={}, game_name={}, get player_data failed. Exception={}".format(self.name, version, group, game_name, traceback.format_exc()))
            return
        player_data = dict(player_data, GameName=game_name)

        buffer_setting = self.get_info_func(None, self.name, userData=player_data, bWarningLog=False)
        if buffer_setting is None:
            self.logger.debug("[{}][incr_buffer] version={}, group={}, game_name={}, get buffer_setting failed. Exception={}".format(self.name, version, group, game_name, traceback.format_exc()))
            return

        buffer_rate = buffer_rate or buffer_setting.get("buffer_rate", 0.98)

        add_value = floor_float(bet * buffer_rate - win, 6)
        try:
            self.BufferDao.incr_buffer_value(game_name, version, group, add_value, bet, win)
        except:
            self.logger.error("[{}][incr_buffer] failed. Exception={}".format(self.name, traceback.format_exc()))

    def _check_buffer_control_level(self, buffer_setting, buffer_value):
        '''
        trigger_no_win：是否觸發no win
        ctrlLv：Buffer值在第幾階
        gate：Buffer值在第幾階的gate
        max_win：最大贏分上限
        buffer_gate_lv：贏分上限控在哪階門檻值
        '''
        ctrl_lv, gate, max_win, buffer_gate_lv = None, None, 0, None
        trigger_no_win = False

        if 'CtrlLevel' not in buffer_setting or (len(buffer_setting['CtrlLevel']) <= 0):
            return trigger_no_win, -1, gate, max_win, -1
        ctrl_lv_list = buffer_setting.get('CtrlLevel')

        # 計算贏分上限控在哪階門檻值
        for level_idx, level_item in enumerate(ctrl_lv_list):
            if ctrl_lv is None:
                if buffer_value >= level_item['BufferGateValue']:
                    ctrl_lv = level_idx-1
                # 最高階
                elif level_idx == len(ctrl_lv_list)-1:
                    ctrl_lv = level_idx

            if buffer_gate_lv is None and level_item['BufferGate'] >= random.random():
                buffer_gate_lv = level_idx
                max_win = buffer_value - level_item['BufferGateValue'] if buffer_value >= level_item['BufferGateValue'] else 0

        no_win_gate = ctrl_lv_list[ctrl_lv]['NoWinGate']  if ctrl_lv >=0 else 0

        trigger_no_win = (no_win_gate >= random.random())
        max_win =max_win if not trigger_no_win else 0

        return trigger_no_win, ctrl_lv, no_win_gate, max_win, buffer_gate_lv


    # 一分鐘Reload一次
    def Reload(self, bForce=True):
        # 既不強制reload,且Reload時間還沒到
        if (not bForce) and (self._fNextReload > 0) and (self._fNextReload > time.time()):
            return
        self._fNextReload = time.time() + 30  # 0.5分鐘reload
        self.BufferDao.load_setting()
        self.BufferDao.update_buffer_value()

    def _GetInfo(self, user_id, serviceName=None, userData=None, infoDict=None, dimensionList=None, transformDict=None, bWarningLog=True):
        return self.BufferDao.get_buffer_setting(userData["GameName"], "", "")




if __name__ == '__main__':
    from GuaiGuaiLib.Common.Dimension import Dimension

    os_path = 'D:\\SVN_FILE\\iGaming\\branches\\Server\\Ark\\SlotBonus\\pixiu\\Game'
    ConfigPath = 'D:\\SVN_FILE\\iGaming\\branches\\Server\\Ark\\SlotBonus\\pixiu\\Game/config/local/'
    module_list = ['mongo_manager', 'config_manager', 'log_manager', 'timer_service', 'user_manager']

    os.chdir(os_path)
    print(os.getcwd())

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    Dimension = Dimension(ConfigFile=ConfigPath, Logger=logger, GetUserDataFunc=loader.get_class_instance("user_manager").get_user_data)
    buffer = Buffer(logger, Name='BuyBonus', ConfigPath=ConfigPath, GetPlayerDataFunc=loader.get_class_instance("user_manager").get_user_data ,GetInfoFunc=Dimension.GetInfo, InfoRegisterFunc=Dimension.InfoRegister, Channel='H5sea')

    version = "THB"
    group = 2
    game_name = 'LegendOfTheWhiteSnake'
    ark_id = '10000031'
    nTotalBet = 50000
    bet = 10
    win = 1100

    for i in range(10000000):
        no_win_gate, max_win = buffer.get_gate_and_max_win(version, group, game_name, ark_id, nTotalBet)
        buffer.incr_buffer(version, group, game_name, bet, win, jpBet=0)




