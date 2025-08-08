# -*- coding: utf8 -*-
__author__ = 'eitherwang'

import pymongo
from ...Server.Database.DbConnectorEx import DbConnector
import csv, os

class StageDb:
    field = ['Platform', 'GameId', 'Type', 'Name', 'GameNameCht', 'GameNameEn', 'Enable']
    @staticmethod
    def Initialize(logger, strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', DataSource=None, bDropDb=False, **kwargs):
        if DataSource is None:
            DataSource = DbConnector.Connect(strDbName, strHost, nPort, strUser, strPassword)
        logger.info("[StageDb] Database: %s", DataSource)
        if DataSource is None:
            logger.error("[StageDb] Initialize failed")
            return None

        StageCol = DataSource['Stage']
        StageCol.create_index([('Type', pymongo.ASCENDING), ('Name', pymongo.ASCENDING)],unique=True)
        filePath = kwargs.get("FilePath", "Script/Init/GameListNewSlot.csv")
        if isinstance(filePath, str) and os.path.isfile(filePath):
            try:
                with open(filePath,  "r", encoding="utf-8-sig") as file:
                    if 'GameList' in filePath:
                        StageDb.WriteGameList(logger, StageCol, file)
            except Exception as e:
                logger.error("[StageDb] WriteGameList failed", exc_info=True)
        else:
            logger.error("[StageDb] File:{} not found".format(os.getcwd()+"/"+filePath))

    @staticmethod
    def WriteGameList(logger, col, file):
        reader = csv.DictReader(file)
        for row in reader:
            for key in StageDb.field:
                if key not in row:
                    logger.warn("[StageDb] {} not in {}".format(key, row))
                    continue
            plat, gid, gtype, gname, gname_cht, gname_en, enable = row["Platform"], int(row["GameId"]), row["Type"], row["Name"], row["GameNameCht"], row["GameNameEn"], (row["Enable"]=="TRUE")
            qry = {"Type": gtype, 'Name': gname}
            upd = {
                "Sequence": gid,
                "Enable": enable,
                "Tag": [],
                "GameId": gid,
                "Title": {
                    "en-us": gname_en,
                    "zh-tw": gname_cht,
                },
                "Icon": {
                    "en-us": gname_en,
                },
                "StageType": {},
                "Platform": plat,
                "Currency": ["Coin"],
                "CreditRange": [20000, 20000000],
                "BetRange": [2000],
                "Requirement": {"Memory": 0, "Disk": 0},
                "Text": "",
                "Comment": gname_cht
            }
            col.update_one(qry, {'$setOnInsert': upd}, upsert=True)