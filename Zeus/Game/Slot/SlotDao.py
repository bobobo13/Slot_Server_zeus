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
        skipName = ['.SVN', 'COMMON', 'SLOTCOMMON', 'SLOTAPI', 'SLOTDAO', 'SLOTDB', 'SLOTSYSTEM']
        folder_path = os.path.dirname(os.path.abspath(__file__))
        print(code_name)
        for file_name in os.listdir(folder_path):
            if file_name.upper() in skipName or not file_name[0].isupper() or (not os.path.isdir(os.path.join(folder_path, file_name))):
                continue
            Setting = importlib.import_module('.Game.Slot.{}.{}Setting'.format(file_name, file_name), package=code_name).Setting
            result[file_name] = Setting
        return result

    def LoadSlotGameInfo(self, code_name):
        result = {}
        skipName = ['.SVN', 'COMMON', 'SLOTCOMMON', 'SLOTAPI', 'SLOTDAO', 'SLOTDB', 'SLOTSYSTEM']
        folder_path = os.path.dirname(os.path.abspath(__file__))
        for file_name in os.listdir(folder_path):
            if file_name.upper() in skipName or not file_name[0].isupper() or (not os.path.isdir(os.path.join(folder_path, file_name))):
                continue
            Info = importlib.import_module('.Game.Slot.{}.{}Info'.format(file_name, file_name), package=code_name).GameInfo
            for i in Info:
                if i['game_id'] not in result:
                    result[i['game_id']] = {}
                if 'ProbId' not in i:
                    self.logger.warn('ProbId not found in %s' % i)
                    continue
                result[i['game_id']][i['ProbId']] = i
        return result

    def GetGameState(self, ark_id):
        return self.game_states.get(ark_id, None)

    def SetGameState(self, ark_id, game_state):
        self.game_states[ark_id] = game_state
        return game_state
