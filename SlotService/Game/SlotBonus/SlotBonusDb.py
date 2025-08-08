#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import os
import importlib
import pymongo
from ..Server.Database.DbConnectorEx import DbConnector

class SlotBonusDb:
    @staticmethod
    def Initialize(strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', data_source=None):
        if data_source is None:
            DataSource = DbConnector.Connect(strDbName, strHost, nPort, strUser, strPassword)
        if data_source is None:
            return None

        # Setting
        setting = data_source['BonusSetting']
        upd = {"Enable": True}
        setting.update_one({'Version': 'default'}, {'$setOnInsert': upd}, upsert=True)

        # Info
        info = data_source['BonusInfo']
        info.create_index([('Version', pymongo.ASCENDING),('Group', pymongo.ASCENDING)], unique=True)
        upd = {"Enable": True}
        upd["Name"] = ["LegendOfTheWhiteSnake_0"]
        info.update_one({'Version': 'default'}, {'$setOnInsert': upd}, upsert=True)
        info.update_one({'Version': 'THB'}, {'$setOnInsert': upd}, upsert=True)

        # BetModel
        betModel = data_source['BonusModel']
        betModel.create_index([('Name', pymongo.ASCENDING)], unique=True)
        upd = {}

        # LegendOfTheWhiteSnake
        upd['GameName'] = "LegendOfTheWhiteSnake"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "FREE GAME"  # Client顯示用
        upd['BetLines'] = 40
        upd['BetMode'] = {
            'Bet':False,
            'ExtraBet':True
        }
        upd['ExtraBet'] = True
        upd['EnableJp'] = False

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB1",
            'WinOddsControl':{
                "LowerBound":[0, 140, 230],
                "Gate":[[0,100],[64, 100],[100, 100]]
            },
            "CostMulti": 100,
            "ExtraCostMulti": 100,
        }

        # upd['WinOddsControl'] = {
        #     "LowerBound":[0,1,2,3,5,10,20,30,50,100,200],
		# 	"Gate":[[20,100], [30,100], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [40, 100], [80, 100], [1, 1]]
        # }
        # line_bet = [0.01, 0.02, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1.5, 2, 2.5, 4, 5, 7.5, 10, 15, 25, 30]
        # upd['BetList'] = [{"LineBet": i} for i in line_bet]
        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)

        betModel.update_one({'Name': 'LegendOfTheWhiteSnake_0'}, {'$setOnInsert': upd}, upsert=True)

        # SpinOfFate
        upd = {}
        upd['GameName'] = "SpinOfFate"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "FREE GAME"  # Client顯示用
        upd['BetLines'] = 50
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True


        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB1",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 60,
            "ExtraCostMulti": 60,
        }

        # upd['WinOddsControl'] = {
        #     "LowerBound":[0,1,2,3,5,10,20,30,50,100,200],
        # 	"Gate":[[20,100], [30,100], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [40, 100], [80, 100], [1, 1]]
        # }
        # line_bet = [0.01, 0.02, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1.5, 2, 2.5, 4, 5, 7.5, 10, 15, 25, 30]
        # upd['BetList'] = [{"LineBet": i} for i in line_bet]
        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'SpinOfFate_0'}, {'$setOnInsert': upd}, upsert=True)

        upd = {}
        upd['GameName'] = "SpinOfFate"
        upd['SpecialGame'] = "1"
        upd['SpecialGameType'] = "FEATURE GAME"  # Client顯示用
        upd['BetLines'] = 50
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB2",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 45,
            "ExtraCostMulti": 45,
        }

        # upd['WinOddsControl'] = {
        #     "LowerBound":[0,1,2,3,5,10,20,30,50,100,200],
        # 	"Gate":[[20,100], [30,100], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [40, 100], [80, 100], [1, 1]]
        # }
        # line_bet = [0.01, 0.02, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1.5, 2, 2.5, 4, 5, 7.5, 10, 15, 25, 30]
        # upd['BetList'] = [{"LineBet": i} for i in line_bet]
        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'SpinOfFate_1'}, {'$setOnInsert': upd}, upsert=True)
        
        # BuddhaSpin
        upd = {}
        upd['GameName'] = "BuddhaSpin"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "FREE GAME"  # Client顯示用
        upd['BetLines'] = 50
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB1",
            "WinOddsControl": {
                "LowerBound": [-1, 10, 24],
                "Gate": [[100, 100], [20, 100], [0, 100]]
            },
            "CostMulti": 50,
            "ExtraCostMulti": 50,
        }

        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'BuddhaSpin_0'}, {'$setOnInsert': upd}, upsert=True)
        
        # BuddhaSpin
        upd = {}
        upd['GameName'] = "BuddhaSpin"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "SUPER FREE GAME"  # Client顯示用
        upd['BetLines'] = 50
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB2",
            "WinOddsControl": {
                "LowerBound": [-1, 30, 40, 450, 750, 1000, 2000],
                "Gate": [[100, 100], [5, 100], [0, 100], [35, 100], [55, 100], [70, 100], [100, 100]]
            },
            "CostMulti": 150,
            "ExtraCostMulti": 150,
        }

        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'BuddhaSpin_1'}, {'$setOnInsert': upd}, upsert=True)

        # SweetBonanza
        upd = {}
        upd['GameName'] = "SweetBonanza"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "SweetBonanza_0"  # Client顯示用
        upd['BetLines'] = 100
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB1",
            "WinOddsControl": {
                "LowerBound": [-1, 10, 24],
                "Gate": [[100, 100], [20, 100], [0, 100]]
            },
            "CostMulti": 50,
            "ExtraCostMulti": 50,
        }

        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'SweetBonanza_0'}, {'$setOnInsert': upd}, upsert=True)
        
        # SweetBonanza
        upd = {}
        upd['GameName'] = "SweetBonanza"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "SweetBonanza_1"  # Client顯示用
        upd['BetLines'] = 100
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB2",
            "WinOddsControl": {
                "LowerBound": [-1, 30, 40, 450, 750, 1000, 2000],
                "Gate": [[100, 100], [5, 100], [0, 100], [35, 100], [55, 100], [70, 100], [100, 100]]
            },
            "CostMulti": 150,
            "ExtraCostMulti": 150,
        }

        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'SweetBonanza_1'}, {'$setOnInsert': upd}, upsert=True)

        # FortuneArrow
        upd = {}
        upd['GameName'] = "FortuneArrow"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "FortuneArrow_0"  # Client顯示用
        upd['BetLines'] = 50
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB1",
            "WinOddsControl": {
                "LowerBound": [-1, 5, 100, 150, 250, 500],
                "Gate": [[100, 100], [0, 100], [50, 100], [90, 100], [95, 100], [100, 100]]
            },
            "CostMulti": 40,
            "ExtraCostMulti": 40,
        }

        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'FortuneArrow_0'}, {'$setOnInsert': upd}, upsert=True)
        
        # FortuneArrow
        upd = {}
        upd['GameName'] = "FortuneArrow"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "FortuneArrow_1"  # Client顯示用
        upd['BetLines'] = 50
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB2",
            "WinOddsControl": {
                "LowerBound": [-1, 50, 400, 500, 1000, 2000, 4000],
                "Gate": [[100, 100], [0, 100], [33, 100], [90, 100], [97, 100], [99, 100], [100, 100]]
            },
            "CostMulti": 200,
            "ExtraCostMulti": 200,
        }

        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2024, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'FortuneArrow_1'}, {'$setOnInsert': upd}, upsert=True)

        # LittlePiggies
        upd = {}
        upd['GameName'] = "LittlePiggies"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "LittlePiggies_0"  # Client顯示用
        upd['BetLines'] = 25
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB1",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 60,
            "ExtraCostMulti": 60,
        }

        # upd['WinOddsControl'] = {
        #     "LowerBound":[0,1,2,3,5,10,20,30,50,100,200],
        # 	"Gate":[[20,100], [30,100], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [40, 100], [80, 100], [1, 1]]
        # }
        # line_bet = [0.01, 0.02, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1.5, 2, 2.5, 4, 5, 7.5, 10, 15, 25, 30]
        # upd['BetList'] = [{"LineBet": i} for i in line_bet]
        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2025, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'LittlePiggies_0'}, {'$setOnInsert': upd}, upsert=True)

        upd = {}
        upd['GameName'] = "LittlePiggies"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "LittlePiggies_1"  # Client顯示用
        upd['BetLines'] = 25
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB2",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 60,
            "ExtraCostMulti": 60,
        }

        # upd['WinOddsControl'] = {
        #     "LowerBound":[0,1,2,3,5,10,20,30,50,100,200],
        # 	"Gate":[[20,100], [30,100], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [40, 100], [80, 100], [1, 1]]
        # }
        # line_bet = [0.01, 0.02, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1.5, 2, 2.5, 4, 5, 7.5, 10, 15, 25, 30]
        # upd['BetList'] = [{"LineBet": i} for i in line_bet]
        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2025, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'LittlePiggies_1'}, {'$setOnInsert': upd}, upsert=True)

        upd = {}
        upd['GameName'] = "LittlePiggies"
        upd['SpecialGame'] = "0"
        upd['SpecialGameType'] = "LittlePiggies_2"  # Client顯示用
        upd['BetLines'] = 25
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = True

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB3",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 60,
            "ExtraCostMulti": 60,
        }

        # upd['WinOddsControl'] = {
        #     "LowerBound":[0,1,2,3,5,10,20,30,50,100,200],
        # 	"Gate":[[20,100], [30,100], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [40, 100], [80, 100], [1, 1]]
        # }
        # line_bet = [0.01, 0.02, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1.5, 2, 2.5, 4, 5, 7.5, 10, 15, 25, 30]
        # upd['BetList'] = [{"LineBet": i} for i in line_bet]
        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2025, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'LittlePiggies_2'}, {'$setOnInsert': upd}, upsert=True)

        # SugarRush
        upd = {}
        upd['GameName'] = "SugarRush"
        upd['SpecialGame'] = "2"
        upd['SpecialGameType'] = "SugarRush_2"  # Client顯示用
        upd['BetLines'] = 100
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = False

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB1",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 60,
            "ExtraCostMulti": 60,
        }

        # upd['WinOddsControl'] = {
        #     "LowerBound":[0,1,2,3,5,10,20,30,50,100,200],
        # 	"Gate":[[20,100], [30,100], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [0, 1], [40, 100], [80, 100], [1, 1]]
        # }
        # line_bet = [0.01, 0.02, 0.04, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 1.5, 2, 2.5, 4, 5, 7.5, 10, 15, 25, 30]
        # upd['BetList'] = [{"LineBet": i} for i in line_bet]
        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2026, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'SugarRush_2'}, {'$setOnInsert': upd}, upsert=True)

        # SugarRush
        upd = {}
        upd['GameName'] = "SugarRush"
        upd['SpecialGame'] = "3"
        upd['SpecialGameType'] = "SugarRush_3"  # Client顯示用
        upd['BetLines'] = 100
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = False

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "BB2",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 60,
            "ExtraCostMulti": 60,
        }

        upd['StartTime'] = datetime.datetime(2024, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2026, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'SugarRush_3'}, {'$setOnInsert': upd}, upsert=True)

        # WuJinPen
        upd = {}
        upd['GameName'] = "WuJinPen"
        upd['SpecialGame'] = "1"
        upd['SpecialGameType'] = "WuJinPen_1"  # Client顯示用
        upd['BetLines'] = 25
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = False

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "90",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 100,
            "ExtraCostMulti": 100,
        }

        upd['StartTime'] = datetime.datetime(2025, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2027, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'WuJinPen_1'}, {'$setOnInsert': upd}, upsert=True)

        # Zeus
        upd = {}
        upd['GameName'] = "Zeus"
        upd['SpecialGame'] = "1"
        upd['SpecialGameType'] = "Zeus_1"  # Client顯示用
        upd['BetLines'] = 25
        upd['BetMode'] = {
            'Bet': True,
            'ExtraBet': False
        }
        upd['ExtraBet'] = False
        upd['EnableJp'] = False

        upd['DefBetList'] = {
            'Currency': "Coin",
            "ProbId": "90",
            'WinOddsControl': {
                "LowerBound": [300, 500],
                "Gate": [[0, 100], [0, 100]]
            },
            "CostMulti": 100,
            "ExtraCostMulti": 100,
        }

        upd['StartTime'] = datetime.datetime(2025, 4, 1, 0, 0, 0)
        upd['EndTime'] = datetime.datetime(2027, 4, 8, 0, 0, 0)
        betModel.update_one({'Name': 'Zeus_1'}, {'$setOnInsert': upd}, upsert=True)

    @staticmethod
    def init_game_setting_and_info(data_source):
        path = os.path.dirname(os.path.abspath(__file__))
        info_suf = 'BonusInfo'
        ignore_file = {"common", "__init__.py", "__init__.pyc"}

        fold_path = os.path.join(path, "BonusGame")
        bonus_info_game_list = get_game_name_list(fold_path, ignore_file, suf='{}'.format(info_suf))
        #  GameInfo
        info = data_source['BonusGameInfo']
        info.create_index([('GameName', pymongo.ASCENDING), ('ProbId', pymongo.ASCENDING)], unique=True)
        game_list = [game for game in bonus_info_game_list if game in bonus_info_game_list]

        for game in game_list:
            gameInfo = importlib.import_module("Game.SlotBonus.BonusGame.{}{}".format(game, info_suf)).GameInfo
            for upd in gameInfo:
                if 'ProbId' not in upd:
                    continue
                if 'game_name' not in upd:
                    continue
                info.update_one({'GameName': upd['game_name'], 'ProbId': upd['ProbId']}, {'$setOnInsert': upd}, upsert=True)

    @staticmethod
    def init_slot_config(data_source):
        setting = data_source['BonusSlotConfig']
        setting.create_index([('Version', pymongo.ASCENDING), ('Group', pymongo.ASCENDING), ('GameName', pymongo.ASCENDING)], unique=True)
        upd = {}
        upd['DecGameRate'] = 0
        setting.update_one({'Version': 'default'}, {'$setOnInsert': upd}, upsert=True)

def get_game_name_list(binus_folder_path, ignore_file, suf=None):
    bonus_game_list = []
    for bonus_file_name in os.listdir(binus_folder_path):
        full_suf = None
        if bonus_file_name in ignore_file:
            continue
        for item in [".py", ".pyc"]:
            if not bonus_file_name.endswith(item):
                continue
            full_suf = suf + item if suf is not None and type(suf) is str else item
            break
        if  full_suf is None:
            continue
        game_name =bonus_file_name.replace(full_suf, '')
        if game_name not in bonus_game_list:
            bonus_game_list.append(game_name)
    return bonus_game_list

if __name__ == "__main__":
    os_path = 'D:\\SVN_FILE\\iGaming\\trunk\\Server\\H5\\pixiu\\Game'
    os.chdir(os_path)
    print(os.getcwd())
    data_source = DbConnector.Connect('BuyBonus', '127.0.0.1', 27017, '', '')
    SlotBonusDb.init_game_setting_and_info(data_source)