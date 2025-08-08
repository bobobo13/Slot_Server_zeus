import datetime, time
import pymongo

from Common.DbConnector import DbConnector

class SlotBonusDb:
    @staticmethod
    def Initialize(strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', DataSource=None):
        if DataSource is None:
            DataSource = DbConnector.Connect(strDbName, strHost, nPort, strUser, strPassword)
        if DataSource is None:
            return None

        # Info
        info = DataSource['Info']
        upd = {"EnableJp":False, "ActRatio":1, "BufferRatio":1, 'Enable': True}
        upd['BetId'] = 'THB001'
        upd['Logo'] = {'Logo': 'OCMS', 'kioskId': 13}
        upd['Game'] = ['RomaPlus_0']
        info.update({'Version': 'THB'}, {'$setOnInsert': upd}, upsert=True)

        # BetModel
        betModel = DataSource['BetModel']
        upd = {"Name": "RomaPlus_0"}
        upd['ExtraBet'] = False
        upd['DefBetList'] = {
            "Currency": "Coin",
            "Win": {"Max":10,"Min":50}
        }
        upd['BetList'] = [
            {
                "LineBet":0.9,
                "Cost":125,
                "ExtraCost": 150
            },
            {
                "LineBet":1.5,
                "Cost":225,
                "ExtraCost": 250,
                "Currency": "THB",
                "Win":{"Max":20,"Min":80}
            }
        ]
        betModel.update({'BetId': 'THB001'}, {'$setOnInsert': upd}, upsert=True)