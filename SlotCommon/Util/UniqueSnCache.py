#!/usr/bin/env python
# -*- coding: utf-8 -*-
from Database.DbConnectorEx import DbConnector


class UniqueSnCache(object):
    _INSTANCE = {}
    GAME_SN_CACHE_SIZE = 100
    GAME_SN_DEFAULT = 100000000000  # 1千億/12碼
    SN_FIELD = 'accumulation_num'

    @classmethod
    def getSn(cls, snType, SubType=""):
        if snType not in cls._INSTANCE:
            cls._INSTANCE[snType] = cls(snType)
        return cls._INSTANCE[snType].get_sn(SubType)

    @classmethod
    def getSnSeq(cls, snType, nAmount, SubType=""):
        if snType not in cls._INSTANCE:
            cls._INSTANCE[snType] = cls(snType)
        return cls._INSTANCE[snType].get_sn_seq(nAmount, SubType)

    def __init__(self, snType, DataSource=None, sn_field=None, sn_add_range=None, sn_default=None, **kwargs):
        self.DataSource = DataSource
        if self.DataSource is None:
            nam, host, port, user, pwd, ps = 'GameSN', 'localhost', 27017, '', '', 30  # default
            # from configure file
            strConfigFile = kwargs.get('ConfigFile')
            strSession = kwargs.get('Session', 'GameSN')
            cfg = DbConnector.GetDbConfig(strConfigFile, strSession)
            if cfg is not None:
                nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigFile, strSession, cfg)
            # from arguments
            strDbName, strHost, nPort = kwargs.get('DbName', nam), kwargs.get('Host', host), kwargs.get('Port', port)
            strUser, strPassword, nPoolSize = kwargs.get('User', user), kwargs.get('Password', pwd), kwargs.get(
                'PoolSize', ps)
            # connect database
            self._Connector = DbConnector(strDbName, strHost, nPort, strUser, strPassword, nPoolSize)
            self.DataSource = self._Connector.DataSource

        self.sn_collection = self.DataSource[snType]
        self.sn_now = -1
        self.sn_max = -1
        self.sn_field = sn_field or UniqueSnCache.SN_FIELD
        self.sn_add_range = sn_add_range or UniqueSnCache.GAME_SN_CACHE_SIZE
        self.sn_default = sn_default or UniqueSnCache.GAME_SN_DEFAULT

    def get_sn(self, SubType=""):
        if self.sn_now < 0 or self.sn_now + 1 > self.sn_max:
            query = {"SubType": SubType}
            update = dict()
            update["$inc"] = {self.sn_field: self.sn_add_range}

            result = self.sn_collection.find_and_modify(query, update=update, new=False, upsert=False)
            # print 'get_sn ', self.sn_collection.name, ' result1: ', result
            if result != None:
                self.sn_now = result[self.sn_field]
            else:
                update = dict()
                update["$setOnInsert"] = {self.sn_field: self.sn_default}
                result = self.sn_collection.find_and_modify(query, update=update, new=True, upsert=True)
                # print 'get_sn ', self.sn_collection.name, ' result2: ', result
                update = dict()
                update["$inc"] = {self.sn_field: self.sn_add_range}
                result = self.sn_collection.find_and_modify(query, update=update, new=False, upsert=False)
                # print 'get_sn ', self.sn_collection.name, 'result3: ', result
                self.sn_now = result[self.sn_field]
            self.sn_max = self.sn_now + self.sn_add_range - 1

        else:
            self.sn_now = self.sn_now + 1

        # print 'sn_collection.name ', self.sn_collection.name, ' self.sn_now: ', self.sn_now, ', self.sn_max: ', self.sn_max
        return self.sn_now

    def get_sn_seq(self, nAmount, SubType=""):
        if nAmount <= 0:
            return []
        if nAmount < 100:
            r = [self.get_sn(SubType) for _ in range(nAmount)]
            assert len(r) == nAmount
            return r

        query = {"SubType": SubType}
        update = {"$inc": {self.sn_field: nAmount}}
        result = self.sn_collection.find_and_modify(query, update=update, new=False, upsert=False)
        if result is not None:
            sn_head = result[self.sn_field]
        else:
            updInsert = {"$setOnInsert": {self.sn_field: self.sn_default}}
            result = self.sn_collection.find_and_modify(query, update=updInsert, new=True, upsert=True)
            if result is None:
                raise Exception("get_sn_seq failed")
            result = self.sn_collection.find_and_modify(query, update=update, new=False, upsert=False)
            sn_head = result[self.sn_field]
            # print 'get_sn ', self.sn_collection.name, 'result3: ', result
        r = range(sn_head, sn_head + nAmount)
        assert len(r) == nAmount
        return r


if __name__ == "__main__":
    import pymongo

    db = pymongo.MongoClient("mongodb://localhost:27017/")["testSn"]

    testtype1 = "testtype1"
    db[testtype1].drop()
    r1 = UniqueSnCache.getSn(testtype1)
    print(r1)
    r2 = UniqueSnCache.getSnSeq(testtype1, 1)
    print(r2)
    r3 = UniqueSnCache.getSnSeq(testtype1, 10)
    print(r3)
    r3 = UniqueSnCache.getSnSeq(testtype1, 100)
    print(r3)
    r3 = UniqueSnCache.getSnSeq(testtype1, 99)
    print(r3)
    r2 = UniqueSnCache.getSnSeq(testtype1, 1)
    print(r2)
    r3 = UniqueSnCache.getSnSeq(testtype1, 10)
    print(r3)
