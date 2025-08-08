#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "eitherwang"

import sys
import traceback
import time
import datetime
import pymongo
import gevent
import pymongo.errors
import redis
import configparser as ConfigParser
from SlotServer.Database.DbConnectorEx import DbConnector
# DB Access

class IngameJpMongoDao:
    JP_WINNER_HISTORY_VALID_TIME = 60 * 86400

    def __init__(self, logger=None, bInitDb=True, **kwargs): # kwargs = { ConfigFile:'', 'Host':'localhost', 'Port':27017, 'User':'', 'Password'='', 'DbName':'Item', 'PoolSize':30 }
        self.logger = logger

        self.getPlayerData = kwargs.get("GetPlayerData")
        self.DataSource = kwargs.get("IngameJpDataSource", kwargs.get("DataSource"))
        if self.DataSource is None:
            nam, host, port, user, pwd, ps = 'InGameJackpot','localhost',27017,'','',30
            # from configure file
            strConfigFile = kwargs.get('ConfigFile')
            strSection = kwargs.get('Section', 'InGameJackpot')
            cfg = DbConnector.GetDbConfig(strConfigFile, strSection)
            if cfg is not None:
                nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigFile, strSection, cfg)
            # from arguments
            strDbName, strHost, nPort = kwargs.get('DbName',nam), kwargs.get('Host',host), kwargs.get('Port',port)
            strUser, strPassword, nPoolSize = kwargs.get('User',user), kwargs.get('Password',pwd), kwargs.get('PoolSize',ps)
            # connect database
            # self._Connector = DbConnector(strDbName, strHost, nPort, strUser, strPassword, nPoolSize)
            self._Connector = DbConnector(strHost, nPort, strUser, strPassword, nPoolSize)
            self.DataSource = self._Connector.getDb(strDbName)


        self.DataSource['InGameJackpotWinner'].create_index([('Timestamp', pymongo.DESCENDING)], background=True)
        self.DataSource['InGameJackpotWinner'].create_index([('GameId', pymongo.ASCENDING)], background=True)
        #self._DataSource['InGameJackpotWinner'].create_index([('game_id', pymongo.ASCENDING), ('jp_level', pymongo.ASCENDING)], background=True)
        self.DataSource['InGameJackpotWinner'].create_index([('GameId', pymongo.ASCENDING), ('JpLevel', pymongo.ASCENDING), ('Timestamp', pymongo.DESCENDING)], background=True)
        self.DataSource['InGameJackpotWinner'].create_index([('ExpireTime', pymongo.DESCENDING)], background=True, expireAfterSeconds=self.JP_WINNER_HISTORY_VALID_TIME)


    def AddWinnerList(self, game_id, currency, user_id, third_party_id, third_party_name, jp_type, jp_level, award):
        doc = {
            'GameId': game_id,
            'Currency': currency,
            'ArkId': user_id,
            'ThirdPartyId': third_party_id,
            'ThirdPartyName': third_party_name,
            'JpType': jp_type,
            'JpLevel': int(jp_level),
            'Award': award,
            'Timestamp': time.time(),
            'ExpireTime': datetime.datetime.utcnow(),
        }
        self.DataSource['InGameJackpotWinner'].insert(doc)
        self.logger.info('[%s] add_in_game_jp_winner_history: %s' % (self.__class__.__name__, doc))


    # def db_get_winner_list(self, num):
    #     winner_dict = dict()
    #     for game_id in self._calculators:
    #         # 固定都找出最大的那組JP
    #         cursor = self._DataSource['InGameJackpotWinner'].find(
    #             {
    #                 'GameId': str(game_id),
    #                 'JpLevel': int(0),
    #             },
    #             read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED
    #         ).sort([('Timestamp', pymongo.DESCENDING)]).limit(num)
    #
    #         winner_list = list()
    #         for i in cursor:
    #             dic = {}
    #             dic['game_id'] = i['GameId']
    #             dic['jp_coin'] = i['Award']
    #             dic['jp_type'] = i['JpType']
    #             dic['user_id'] = i['ArkId']
    #             dic['nickname'] = i['ThirdPartyName']
    #             winner_list.append(dic)
    #         winner_dict[game_id] = winner_list
    #     self.jackpot_winner = winner_dict


class IngameJpPoolDao(object):
    def __init__(self, logger=None, bInitDb=True, **kwargs): # kwargs = { ConfigFile:'', 'Host':'localhost', 'Port':27017, 'User':'', 'Password'='', 'DbName':'Item', 'PoolSize':30 }
        self.logger = logger
        # self._TempIncPool = {"Award": {}, "Buffer": {}}
        self._TempIncPool = {}
        self.getPlayerData = kwargs.get("GetPlayerData")

        self.DataSource = kwargs.get("IngameJpDataSource", kwargs.get("DataSource"))
        if self.DataSource is None:
            nam, host, port, user, pwd, ps = 'InGameJackpot','localhost',27017,'','',30
            # from configure file
            strConfigFile = kwargs.get('ConfigFile')
            strSection = kwargs.get('Section', 'InGameJackpot')
            cfg = DbConnector.GetDbConfig(strConfigFile, strSection)
            if cfg is not None:
                nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigFile, strSection, cfg)
            # from arguments
            strDbName, strHost, nPort = kwargs.get('DbName',nam), kwargs.get('Host',host), kwargs.get('Port',port)
            strUser, strPassword, nPoolSize = kwargs.get('User',user), kwargs.get('Password',pwd), kwargs.get('PoolSize',ps)
            # connect database
            # self._Connector = DbConnector(strDbName, strHost, nPort, strUser, strPassword, nPoolSize)
            # self.DataSource = self._Connector.DataSource

            self._Connector = DbConnector(strHost, nPort, strUser, strPassword, nPoolSize)
            self.DataSource = self._Connector.getDb(strDbName)

        self.DataSource["Pool"].create_index([("JpName", pymongo.ASCENDING), ("PoolGroup", pymongo.ASCENDING)], unique=True)


    def update(self):
        tempData = self._TempIncPool
        self._TempIncPool = {}
        # bulk = self.DataSource["Pool"].initialize_unordered_bulk_op()
        # bulk = BulkWriteOperation()
        count = 0
        for key, val in tempData.items():
            gevent.sleep(0.001)
            qry = {"JpName": key[0], "PoolGroup": key[1]}
            inc = {}
            for poolType in val:
                for level in val[poolType]:
                    inc["{}.{}".format(poolType, level)] = val[poolType][level]
            # bulk.update(qry, upd, upsert=True)
            # count += 1
            try:
                self.DataSource["Pool"].update(qry, {"$inc": inc, "$setOnInsert": {"Lock": 0}}, upsert=True)
            except:
                self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        # if count <= 0:
        #     return
        # try:
        #     bulk.execute()
        # except:
        #     self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))

    def InitPool(self, JpName, strPoolGroup, aPool, bPool):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        upd = {"$set": {"Award": aPool, "Buffer": bPool}, "$setOnInsert": {"Lock": 0}}
        r = None
        try:
            r = self.DataSource["Pool"].find_and_modify(qry, upd, upsert=True, new=True)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        if r is None:
            return None
        return r["Award"], r["Buffer"]

    def getLock(self, JpName, strPoolGroup):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup, "Lock": 0}
        upd = {"$set": {"Lock": 1}}
        r = None
        try:
            r = self.DataSource["Pool"].find_and_modify(qry, upd, upsert=True, new=True)
        except pymongo.errors.DuplicateKeyError:
            pass
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return r is not None

    def releaseLock(self, JpName, strPoolGroup):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup, "Lock": 1}
        upd = {"$set": {"Lock": 0}}
        r = None
        try:
            r = self.DataSource["Pool"].find_and_modify(qry, upd, upsert=True, new=True)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return r is not None

    def _AsyncIncPool(self, poolType, JpName, strPoolGroup, level, value):
        if (JpName, strPoolGroup) not in self._TempIncPool:
            self._TempIncPool[(JpName, strPoolGroup)] = {"Award": {}, "Buffer": {}}
        if str(level) not in self._TempIncPool[(JpName, strPoolGroup)][poolType]:
            self._TempIncPool[(JpName, strPoolGroup)][poolType][str(level)] = 0
        self._TempIncPool[(JpName, strPoolGroup)][poolType][str(level)] += value

    def _SyncIncPool(self, poolType, JpName, strPoolGroup, level, value):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        upd = {"$inc": {"{}.{}".format(poolType, level): value}, "$setOnInsert": {"Lock": 0}}
        try:
            self.DataSource["Pool"].update(qry, upd, upsert=True)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))

    def IncAwardPool(self, JpName, strPoolGroup, level, value, is_async=True):
        if value <= 0:
            return
        method = self._AsyncIncPool if is_async else self._SyncIncPool
        method("Award", JpName, strPoolGroup, level, value)

    def DecAwardPool(self, JpName, strPoolGroup, level, value, is_async=False):
        if value <= 0:
            return
        value = -value
        method = self._AsyncIncPool if is_async else self._SyncIncPool
        method("Award", JpName, strPoolGroup, level, value)

    def IncBufferPool(self, JpName, strPoolGroup, level, value, is_async=True):
        if value <= 0:
            return
        method = self._AsyncIncPool if is_async else self._SyncIncPool
        method("Buffer", JpName, strPoolGroup, level, value)

    def DecBufferPool(self, JpName, strPoolGroup, level, value, is_async=False):
        if value <= 0:
            return
        value = -value
        method = self._AsyncIncPool if is_async else self._SyncIncPool
        method("Buffer", JpName, strPoolGroup, level, value)

    def GetBothPool(self, JpName, strPoolGroup, secondary=True):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        fields = self._Projection(["Award", "Buffer"])
        readPreference = pymongo.ReadPreference.SECONDARY_PREFERRED if secondary else pymongo.ReadPreference.PRIMARY
        r = None
        try:
            if pymongo.version_tuple[0] <= 2:
                r = self.DataSource['Pool'].find_one(qry, fields, read_preference=readPreference)
            else:
                col = self.DataSource["Pool"].with_options(read_preference=readPreference)
                r = col.find_one(qry, fields)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return (r["Award"], r["Buffer"]) if r is not None else (None, None)

    def GetAwardPool(self, JpName, strPoolGroup, secondary=True):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        fields = self._Projection(["Award"])
        readPreference = pymongo.ReadPreference.SECONDARY_PREFERRED if secondary else pymongo.ReadPreference.PRIMARY
        r = None
        try:
            if pymongo.version_tuple[0] <= 2:
                r = self.DataSource['Pool'].find(qry, fields, read_preference=readPreference)
            else:
                col = self.DataSource["Pool"].with_options(read_preference=readPreference)
                r = col.find_one(qry, fields)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return r["Award"] if r is not None else None

    def GetBufferPool(self, JpName, strPoolGroup, secondary=True):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        fields = self._Projection(["Buffer"])
        readPreference = pymongo.ReadPreference.SECONDARY_PREFERRED if secondary else pymongo.ReadPreference.PRIMARY
        r = None
        try:
            if pymongo.version_tuple[0] <= 2:
                r = self.DataSource['Pool'].find(qry, fields, read_preference=readPreference)
            else:
                col = self.DataSource["Pool"].with_options(read_preference=readPreference)
                r = col.find_one(qry, fields)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return r["Buffer"] if r is not None else None


    def GetAwardPoolVal(self, JpName, strPoolGroup, level, secondary=False):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        fields = self._Projection(["Award"])
        readPreference = pymongo.ReadPreference.SECONDARY_PREFERRED if secondary else pymongo.ReadPreference.PRIMARY
        r = None
        try:
            if pymongo.version_tuple[0] <= 2:
                r = self.DataSource['Pool'].find(qry, fields, read_preference=readPreference)
            else:
                col = self.DataSource["Pool"].with_options(read_preference=readPreference)
                r = col.find_one(qry, fields)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        if r is None or "Award" not in r:
            return None
        return r["Award"][str(level)] if str(level) in r["Award"] else None

    def GetBufferPoolVal(self, JpName, strPoolGroup, level, secondary=False):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        fields = self._Projection(["Buffer"])
        readPreference = pymongo.ReadPreference.SECONDARY_PREFERRED if secondary else pymongo.ReadPreference.PRIMARY
        r = None
        try:
            if pymongo.version_tuple[0] <= 2:
                r = self.DataSource['Pool'].find(qry, fields, read_preference=readPreference)
            else:
                col = self.DataSource["Pool"].with_options(read_preference=readPreference)
                r = col.find_one(qry, fields)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        if r is None or "Buffer" not in r:
            return None
        return r["Buffer"][str(level)] if str(level) in r["Buffer"] else None

    def SetBufferPool(self, JpName, strPoolGroup, dictPoolValue):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        upd = {"$set": {"Buffer": dictPoolValue}, "$setOnInsert": {"Lock": 0}}
        r = False
        try:
            r = self.DataSource["Pool"].find_and_modify(qry, upd, upsert=True, new=True)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return r

    def SetAwardPool(self, JpName, strPoolGroup, dictPoolValue):
        qry = {"JpName": JpName, "PoolGroup": strPoolGroup}
        upd = {"$set": {"Award": dictPoolValue}, "$setOnInsert": {"Lock": 0}}
        r = False
        try:
            r = self.DataSource["Pool"].find_and_modify(qry, upd, upsert=True, new=True)
        except:
            self.logger.error(
                "%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return r

    def incPool(self, JpName, strPoolGroup, addAwardPool, addBufferPool):
        for level in addAwardPool:
            self.IncAwardPool(JpName, strPoolGroup, level, addAwardPool[level])
            self.IncBufferPool(JpName, strPoolGroup, level, addBufferPool[level])

    def revertPool(self, JpName, strPoolGroup, addAwardPool, addBufferPool):
        for level in addAwardPool:
            self.DecAwardPool(JpName, strPoolGroup, level, addAwardPool[level], is_async=True)
            self.DecBufferPool(JpName, strPoolGroup, level, addBufferPool[level], is_async=True)

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj




class IngameJpRedisDao:
    JP_TYPE_IN_GAME = "in-game"
    REDIS_KEY_MAP = {
        "AwardPool": "{}_{}-jp-pool",
        "BufferPool": "{}_{}-baby-fund-pool",
        "Lock": 'in-game-jackpot-lock:{}_{}',
    }

    def __init__(self, logger, DataSource=None, bInitDb=True, **kwargs):
        self.logger = logger
        self.RedisCli = DataSource
        if self.RedisCli is None:
            host, port, db = 'localhost', 6379, 2
            strConfigFile = kwargs.get('ConfigFile')
            strSection = kwargs.get('Section', 'IngameJackpot')
            confParser = ConfigParser.RawConfigParser()
            confParser.read(strConfigFile)
            host = confParser.get(strSection, 'RedisHost')
            port = confParser.getint(strSection, 'RedisPort')
            db = confParser.getint(strSection, 'RedisDb')

            self.RedisCli = redis.StrictRedis(host, port, db)
        self.pipe = self.RedisCli.pipeline()

    def update(self):
        runPipe = self.pipe
        self.pipe = self.RedisCli.pipeline()
        try:
            runPipe.execute()
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))

    def getRedisCli(self, is_async=False):
        if is_async:
            return self.pipe
        return self.RedisCli

    def InitPool(self, JpName, strPoolGroup, aPool, bPool):
        self.SetAwardPool(JpName, strPoolGroup, aPool)
        self.SetBufferPool(JpName, strPoolGroup, bPool)
        return None

    def getLock(self, JpName, strPoolGroup):
        lockKey = IngameJpRedisDao.REDIS_KEY_MAP["Lock"].format(JpName, strPoolGroup)

        for retry_times in xrange(50):
            # 取得鎖，成功取得會回傳1
            # 設定ttl，以免中途連不上redis造成lock不會被釋放
            ret = self.getRedisCli().set(lockKey, 1, nx=True, ex=2)
            if ret:
                return True
            retry_times += 1
            gevent.sleep(0.1)
        return False

    def releaseLock(self, JpName, strPoolGroup):
        lockKey = IngameJpRedisDao.REDIS_KEY_MAP["Lock"].format(JpName, strPoolGroup)
        self.getRedisCli().delete(lockKey)

    def IncAwardPool(self, JpName, strPoolGroup, level, value, is_async=True):
        if value <= 0:
            return
        redis_client = self.getRedisCli(is_async)
        aPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["AwardPool"].format(JpName, strPoolGroup)
        jp_field = str(level)
        redis_client.hincrbyfloat(aPoolKey, jp_field, float(value))

    def DecAwardPool(self, JpName, strPoolGroup, level, value, is_async=False):
        if value <= 0:
            return
        value = -value
        redis_client = self.getRedisCli(is_async)
        aPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["AwardPool"].format(JpName, strPoolGroup)
        jp_field = str(level)
        redis_client.hincrbyfloat(aPoolKey, jp_field, float(value))

    def IncBufferPool(self, JpName, strPoolGroup, level, value, is_async=True):
        if value <= 0:
            return
        redis_client = self.getRedisCli(is_async)
        bPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["BufferPool"].format(JpName, strPoolGroup)
        jp_field = str(level)
        redis_client.hincrbyfloat(bPoolKey, jp_field, float(value))

    def DecBufferPool(self, JpName, strPoolGroup, level, value, is_async=False):
        if value <= 0:
            return
        value = -value
        redis_client = self.getRedisCli(is_async)
        bPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["BufferPool"].format(JpName, strPoolGroup)
        jp_field = str(level)
        redis_client.hincrbyfloat(bPoolKey, jp_field, float(value))

    def GetBothPool(self, JpName, strPoolGroup, secondary=False):
        aPool = self.GetAwardPool(JpName, strPoolGroup)
        bPool = self.GetBufferPool(JpName, strPoolGroup)
        return aPool, bPool

    def GetAwardPool(self, JpName, strPoolGroup, secondary=False):
        aPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["AwardPool"].format(JpName, strPoolGroup)
        try:
            r = self.getRedisCli().hgetall(aPoolKey)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        if len(r) <= 0:
            return None
        pool = {k: float(r[k]) for k in r}
        return pool

    def GetBufferPool(self, JpName, strPoolGroup, secondary=False):
        bPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["BufferPool"].format(JpName, strPoolGroup)
        try:
            r = self.getRedisCli().hgetall(bPoolKey)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        if len(r) <= 0:
            return None
        pool = {k: float(r[k]) for k in r}
        return pool

    def GetAwardPoolVal(self, JpName, strPoolGroup, level, secondary=False):
        aPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["AwardPool"].format(JpName, strPoolGroup)
        jp_field = str(level)
        try:
            r = self.getRedisCli().hget(aPoolKey, jp_field)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return r

    def GetBufferPoolVal(self, JpName, strPoolGroup, level, secondary=False):
        bPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["BufferPool"].format(JpName, strPoolGroup)
        jp_field = str(level)
        try:
            r = self.getRedisCli().hget(bPoolKey, jp_field)
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return r

    def SetBufferPool(self, JpName, strPoolGroup, dictPoolValue):
        redis_client = self.getRedisCli()
        bPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["BufferPool"].format(JpName, strPoolGroup)
        # if self.get_redis_lock(strPoolGroup):
        try:
            redis_client.hmset(bPoolKey, dictPoolValue)
            return True
        except:
            self.logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return False

    def SetAwardPool(self, JpName, strPoolGroup, dictPoolValue):
        redis_client = self.getRedisCli()
        aPoolKey = IngameJpRedisDao.REDIS_KEY_MAP["AwardPool"].format(JpName, strPoolGroup)
        # if self.get_redis_lock(strPoolGroup):
        try:
            redis_client.hmset(aPoolKey, dictPoolValue)
            return True
        except:
            self.logger.error("%s.%s \n%s" % (
            str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return False

    def incPool(self, JpName, strPoolGroup, addAwardPool, addBufferPool):
        for level in addAwardPool:
            self.IncAwardPool(JpName, strPoolGroup, level, addAwardPool[level])
            self.IncBufferPool(JpName, strPoolGroup, level, addBufferPool[level])

    def revertPool(self, JpName, strPoolGroup, addAwardPool, addBufferPool):
        for level in addAwardPool:
            self.DecAwardPool(JpName, strPoolGroup, level, addAwardPool[level], is_async=True)
            self.DecBufferPool(JpName, strPoolGroup, level, addBufferPool[level], is_async=True)


