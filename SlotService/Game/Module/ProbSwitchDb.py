#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymongo

class ProbSwitchDb:
    @staticmethod
    def Initialize(strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', DataSource=None):
        if DataSource is None:
            return None

        # Setting
        setting = DataSource['ProbSwitchSetting']
        setting.create_index([('GameName', pymongo.ASCENDING)], unique=True)
        # Type: ["SpinCount", "FeverCount", "OddsCount"]
        upd = {
            "Enable": True,
            "Result": ["Group1", "Group2", "Group3"],
            "Weight": [1, 2, 1],
            # "Group1": {
            #     "Type": ["SpinCount", "FeverCount", "OddsCount"],
            #     "Result": ["A", "B", "A"],
            #     "SpinCount": {"Condition": [-1, 200, 800]},
            #     "FeverCount": {"Condition": [-1, 200, 800], "CountSpecialGame":[1, 2]},
            #     "OddsCount": {"Condition": [-1, 200, 800], "Odds": 150}
            #     },
            "Group1": {
                "Type": ["SpinCount", "FeverCount", "OddsCount"],
                "Level": "Lower",
                "Result": ["A", "B", "A"],
                "SpinCount": {"Condition": [-1, 200, 800]},
                "FeverCount": {"Condition": [-1, 200, 800], "CountSpecialGame": [1, 2]},
                "OddsCount": {"Condition": [-1, 200, 800], "Odds": 150}
            },
            "Group2": {
                "Type": ["SpinCount", "FeverCount", "OddsCount"],
                "Level": "Higher",
                "Result": ["A", "B"],
                "SpinCount": {"Condition": [-1, 800]},
                "FeverCount": {"Condition": [-1, 800], "CountSpecialGame": [1, 2]},
                "OddsCount": {"Condition": [-1, 800], "Odds": 150}
            },
            "Group3": {
                "Type": ["SpinCount", "FeverCount", "OddsCount"],
                "Level": "Lower",
                "Result": ["B", "A"],
                "SpinCount": {"Condition": [-1, 200]},
                "FeverCount": {"Condition": [-1, 200], "CountSpecialGame": [1, 2]},
                "OddsCount": {"Condition": [-1, 200], "Odds": 150}
            },
        }
        setting.update_one({'GameName': ''}, {'$setOnInsert': upd}, upsert=True)
        # setting.update({'GameName': 'SpinOfFate'}, {'$setOnInsert': upd}, upsert=True)

        upd = {
            "Enable": True,
            "Result": ["Default"],
            "Weight": [1],
            "DefaultChanceResult": ["A", "B"],
            "DefaultChanceWeight": [1, 0],
        }
        setting.update_one({'GameName': 'XXXX'}, {'$setOnInsert': upd}, upsert=True)


        #  ProbSwitchValue
        setting = DataSource['ProbSwitchValue']
        setting.create_index([('ark_id', pymongo.ASCENDING), ('GameName', pymongo.ASCENDING)], unique=True)