# -*- coding: utf-8 -*-
from __future__ import absolute_import
from builtins import str
__author__ = 'eitherwang'
from .iWallet import iWallet
import pymongo, traceback, pymongo.errors
import sys
import copy

class MongoWallet(iWallet):
    CREDIT_TYPE = {
        "Coin": 100000,
    }

    def __init__(self, Logger=None, DataSource=None, **kwargs):  # kwargs = { dbAsset=None, ConfigFile:'', 'Host':'localhost', 'Port':27017, 'User':'', 'Password'='', 'DbName':'Item', 'PoolSize':30 }
        self._bDbLog = True
        self.logger = Logger
        self._Connector = None
        self._CreditType = MongoWallet.CREDIT_TYPE
        self.DataSource = DataSource

        # Asset
        self.col_Asset = self.DataSource["Asset"]
        self.col_Asset.create_index([('ark_id', pymongo.DESCENDING)], unique=True)

    def IsCredit(self, strType):
        return (strType in self._CreditType)

    def InitPlayer(self, ark_id, item=None):
        initDict = copy.copy(self._CreditType)
        if item is not None:
            initDict.update(item)
        self.col_Asset.update({"ark_id": ark_id}, {"$setOnInsert": initDict}, upsert=True)

    def _CheckType(self, strType):
        if strType not in self._CreditType:
            self.logger.error("[AssetDAO] strType={} not in ASSET_TYPE")
            return False
        return True

    def GetCredit(self, ark_id, TypeList, **kwargs):
        qry = {"ark_id": ark_id}
        fields = self._Projection((TypeList))
        try:
            if pymongo.version_tuple[0] <= 2:
                ret = self.col_Asset.find_one(qry, fields=fields)
            else:
                ret = self.col_Asset.find_one(qry, projection=fields)
        except:
            self.logger.error("[AssetDAO] Cost, ark_id={}, sType={}, exception={} ".format(ark_id, TypeList, traceback.format_exc()))
            return None
        if ret is None:
            return None
        ret["Code"] = 0
        return ret

    def Transaction(self, ark_id, SubDict, AddDict, **kwargs):
        qry = {"ark_id": ark_id}
        incData = {}
        LimitDict = kwargs.get("Limit")
        for CreditType in AddDict:
            subVal = SubDict.get(CreditType, 0)
            addVal = AddDict[CreditType]
            changeVal = addVal - subVal
            limit = None
            if LimitDict is not None:
                limit = LimitDict.get(CreditType)
            qry[CreditType] = {"$gte": subVal}
            if changeVal > 0 and limit is not None:
                qry[CreditType]["$lte"] = limit - changeVal
            incData[CreditType] = changeVal
        upd = {"$inc": incData}

        fields = self._Projection(list(AddDict.keys()))
        ret = None
        try:
            ret = self.col_Asset.find_one_and_update(qry, update=upd, fields=fields, upsert=False, return_document=pymongo.ReturnDocument.AFTER)
        except pymongo.errors.DuplicateKeyError:
            self.logger.error("[AssetDAO] DuplicateKeyError AddCredit, ark_id={}, SubDict={}, AddDict={}, exception={} ".format(ark_id, SubDict, AddDict, traceback.format_exc()))
            return {"Code": -1}
        except:
            self.logger.error("[AssetDAO] AddCredit, ark_id={}, SubDict={}, AddDict={}, exception={} ".format(ark_id, SubDict, AddDict, traceback.format_exc()))
            return {"Code": -999}
        if ret is None:
            return {"Code": -2}
        ret["Code"] = 0
        return ret

    def AddCreditMulti(self, ark_id, AddDict, **kwargs):
        if (AddDict is None) or (len(AddDict) <=0 ):
            return None

        incData = {}
        for creditType, amount in list(AddDict.items()):
            if amount < 0:
                continue
            incData[creditType] = amount
        if len(incData) <= 0:
            return None

        qry = {'ark_id': ark_id}
        upd = {'$inc': incData}
        try:
            result = self.col_Asset.find_and_modify(qry, upd, fields=self._Projection(list(AddDict.keys())), new=True)
        except:
            self.logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return result

    def SubCreditMulti(self, ark_id, SubDict, **kwargs):
        if (SubDict is None) or (len(SubDict) <=0 ):
            return None

        qry = {'ark_id': ark_id}
        subData = {}
        for creditType, amount in list(SubDict.items()):
            if amount < 0:
                continue
            qry[creditType] = { '$gte': amount}
            subData[creditType] = -amount
        if len(subData) <= 0:
            return None

        upd = { '$inc':subData }

        try:
            result = self.col_Asset.find_and_modify(qry, upd, fields=self._Projection(list(SubDict.keys())), new=True)
        except:
            self.logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return result


    def _Projection(self, Fields=()):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj



