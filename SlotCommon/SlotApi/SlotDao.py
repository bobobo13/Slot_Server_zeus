# -*- coding: utf-8 -*-
import os
import pymongo, sys, traceback
import pymongo.errors
import importlib


class SlotDao:
    def __init__(self, logger=None):
        self.logger = logger
        self.game_states = {}

    def LoadSlotGameSetting(self, code_name):
        result = {}
        try:
            Setting = importlib.import_module(f'{code_name}.Game.Slot.{code_name}.{code_name}Setting',
                                              package=code_name).Setting
            result[code_name] = Setting
        except Exception as e:
            self.logger.error(f"Error loading SlotGameSetting: {e}")
        return result

    def LoadSlotGameInfo(self, code_name):
        result = {}
        try:
            Info = importlib.import_module(f'{code_name}.Game.Slot.{code_name}.{code_name}Info', package=code_name).GameInfo
            for i in Info:
                if i['game_id'] not in result:
                    result[i['game_id']] = {}
                if 'ProbId' not in i:
                    self.logger.warn('ProbId not found in %s' % i)
                    continue
                result[i['game_id']][i['ProbId']] = i
        except Exception as e:
            self.logger.error(f"Error loading SlotGameInfo: {e}")
        return result

    def GetGameState(self, ark_id):
        return self.game_states.get(ark_id, None)

    def SetGameState(self, ark_id, game_state):
        self.game_states[ark_id] = game_state
        return game_state
