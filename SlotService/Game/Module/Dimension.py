# -*- coding: utf-8 -*-
__author__ = 'poching'
# Dimension
# from DimensionDao import DimensionDao
# DimensionDao
import sys, traceback, time, copy
import pymongo
from ..Server.Database.DbConnector import DbConnector
from ..Server.Common.RoutineProc import RoutineProc


class Dimension:
    def __init__(self, DataSource=None, Logger=None, **kwargs):
        self.logger = Logger
        self._fNextReload, self._NextReloadSec = 0, 60
        if self.logger is None:
            import logging
            logging.basicConfig()
            self.logger = logging.getLogger("Dimension")
            self.logger.setLevel(logging.DEBUG)
        self.dao = DimensionDao(DataSource, Logger, **kwargs)
        self._tabLoadInfo = {}
        self._tabDimension = {}
        self._tabTransform = {}
        self._tabDataTransform = {}
        # self._tabDimensionInfo = {}
        self._cacheUserData = {}  # {ark_id: data, expireTs}
        self._cacheExpireSecond = 60
        self._cacheReleaseSecond = 3600
        self._cacheReleaseTs = time.time() + self._cacheReleaseSecond
        self._GetUserDataFunc = kwargs.get("GetUserDataFunc")
        self._GetUserVipFunc = kwargs.get('GetUserVipFunc')
        self.Reload(True)

        if not kwargs.get("bHookReload", False):
            self.__Reload = RoutineProc("Dimension", 60, self.Reload)
        else:
            self.logger.warning("[Dimension]Remember to HOOK RELOAD by yourself !!")

    def InfoRegister(self, serviceName, loadInfoFunc, dimensionList=None, transformDict=None):
        if not callable(loadInfoFunc):
            raise Exception("loadInfoFunc should be callable")

        if serviceName in self._tabLoadInfo:
            self.logger.warning("[Dimension]Override LoadInfoFunc, service_name={}".format(serviceName))
        else:
            self.logger.info("[Dimension]Set LoadInfoFunc, service_name={}".format(serviceName))
        self._tabLoadInfo[serviceName] = {"Func": loadInfoFunc, "Data": {}, "ReloadTs": 0}

        if dimensionList is not None:
            if serviceName in self._tabDimension:
                self.logger.warning("[Dimension]Override Dimension, service_name={}".format(serviceName))
            else:
                self.logger.info("[Dimension]Set Dimension, service_name={}".format(serviceName))
            self._tabDimension[serviceName] = dimensionList

        if transformDict is not None:
            if serviceName in self._tabTransform:
                self.logger.warning("[Dimension]Override Transform, service_name={}".format(serviceName))
            else:
                self.logger.info("[Dimension]Set Transform, service_name={}".format(serviceName))
            self._tabTransform[serviceName] = transformDict

    def GetDimension(self, user_id, userData=None, bDetail=True, serviceName='default'):
        if userData is None:
            userData = self._GetUserData(user_id)
        if userData is None:
            self.logger.error("[Dimension]user_id={} get user data failed, userData is None".format(user_id))
            return None
        transform = self._tabTransform[serviceName]
        if not self._CheckDimensionKey(userData, transform):
            self.logger.warning("[Dimension]user_id={} miss dimension data, userData={}".format(user_id, userData))
            return None
        # print "!!!@@@", userData
        trans_key_data = self._TransformRawData(userData, transform)
        ret = userData if bDetail else {}
        data_transform_map = self._tabDataTransform[serviceName]
        ret.update(self._ParseDimension(trans_key_data, data_transform_map))
        # print "!!!@@@", ret
        if ret is None:
            self.logger.warning("[Dimension]user_id={} GetDimension return None, userData={}".format(user_id, userData))
        return ret

    def GetInfo(self, user_id, serviceName=None, userData=None, infoDict=None, dimensionList=None, transformDict=None, bWarningLog=True):
        if userData is None:
            userData = self._GetUserData(user_id)
        if userData is None:
            self.logger.error("[Dimension]user_id={} get user data failed, userData is None".format(user_id))
            return None
        transform = self._tabTransform[serviceName]
        if not self._CheckDimensionKey(userData, transform):
            self.logger.warning("[Dimension]user_id={} miss dimension data, userData={}".format(user_id, userData))
            return None
        info_data = infoDict if infoDict is not None else self._GetInfoData(serviceName)
        if info_data is None:
            self.logger.error("[Dimension]GetInfo FAILED, info data NOT EXIST")
            return None
        # print "!!!@@@", info_data
        dimension_order = dimensionList if dimensionList is not None else self._tabDimension[serviceName]
        transform = transformDict if transformDict is not None else self._tabTransform[serviceName]
        transform_data = self._TransformRawData(userData, transform)
        user_info_key = self._ParseUserInfoKey(transform_data, dimension_order)
        # print "!!!@@@", user_info_key
        for k in user_info_key:
            if k in info_data:
                return info_data[k]
        if bWarningLog:
            self.logger.warning("[Dimension] [{}]user_id={} GetInfo return None, user_info_key={}".format(serviceName, user_id, user_info_key))
        return None

    # ===============以上為對外Method====================================================================================

    def _GetUserData(self, user_id):
        if user_id in self._cacheUserData and self._cacheUserData[user_id].get('ExpireTs', 0) > time.time():
            return self._cacheUserData[user_id]
        if self._GetUserDataFunc is None:
            return
        ret = self._GetUserDataFunc(user_id)
        v = self._GetPlayerVip(user_id)
        if v is not None:
            ret['vip_level'] = v

        user_data = copy.deepcopy(ret)
        user_data['ExpireTs'] = time.time() + self._cacheExpireSecond
        self._cacheUserData[user_id] = user_data
        return ret

    def _GetPlayerVip(self, ark_id):
        if self._GetUserVipFunc is None:
            return
        return self._GetUserVipFunc(ark_id).get('VipLevel', 0)

    def _CheckDimensionKey(self, data, transform):
        if type(data) is not dict:
            return False
        for v in transform.values():
            if v is None:
                continue
            if isinstance(v, list):
                for e in v:
                    if e in data:
                        continue
                    return False
                continue
            if v in data:
                continue
            return False
        return True

    def _TransformRawData(self, userData, transform):
        user_dimension = {}
        for t, f in transform.items():
            if f is None:
                user_dimension[t] = None
            elif type(f) is not list:
                user_dimension[t] = userData.get(f)
            else:
                user_dimension[t] = []
                temp = ''
                for fe in f:
                    urd = userData.get(fe)
                    if urd is None:
                        continue
                    if temp != '':
                        temp += '_'
                    temp += str(urd)
                    user_dimension[t].append(temp)
        return user_dimension

    def _ParseDimension(self, user_dimension: dict, data_transform_map: dict) -> dict:
        ret = {}
        for d, v in user_dimension.items():
            before_trans = v if not isinstance(v, list) else v[-1]
            trans_target = data_transform_map.get(d, {})

            if isinstance(trans_target, str):
                ret[d] = trans_target
            else:
                ret[d] = trans_target.get(before_trans, before_trans)
        return ret

    def _ParseUserInfoKey(self, user_dimension, dimensionOrder):
        temp_list = [user_dimension.get(d) for d in dimensionOrder]
        user_info_key = []
        for temp_item in temp_list:
            pre = tuple() if len(user_info_key) <= 0 else user_info_key[-1]
            if type(temp_item) is not list:
                user_info_key.append(pre + (temp_item,))
            else:
                for temp_elem in temp_item:
                    user_info_key.append(pre + (temp_elem,))
        user_info_key.reverse()
        return user_info_key

    def _GetInfoData(self, serviceName):
        info_data = self._tabLoadInfo.get(serviceName)
        if info_data is None:
            return
        if info_data['ReloadTs'] < time.time() - self._NextReloadSec:
            d = self._tabDimension[serviceName]
            raw_info = info_data['Func']()
            result = {}
            for i in raw_info:
                kl = []
                for e in d:
                    if e in i:
                        kl.append(i[e])
                k = tuple(kl)
                result[k] = i
            info_data['Data'] = result
            info_data['ReloadTs'] = time.time()
        return info_data['Data']

    def Update(self):
        self.Reload()

    def Reload(self, bForce=False):
        if (not bForce) and (self._fNextReload > 0) and (self._fNextReload > time.time()):
            return
        self._fNextReload = time.time() + self._NextReloadSec
        setting = self.dao.LoadSetting()
        self._tabDimension['default'] = setting['default']['Dimension']
        self._tabTransform['default'] = setting['default']['Transform']
        self._tabDataTransform['default'] = {}
        for n, v in setting.items():
            if n == 'default':
                continue
            self._tabDimension[n] = v.get('Dimension', self._tabDimension['default'])
            self._tabTransform[n] = v.get('Transform', self._tabTransform['default'])
            self._tabDataTransform[n] = v.get('DataTransform', {})
        # self._tabDimensionInfo['default'] = self.dao.LoadInfo()
        # for k, f in self._tabLoadInfo.items():
        #     self.logger.debug("[Dimension]Reload Info, service={}".format(k))
        #     f['Data'] = f['Func']()
        if time.time() > self._cacheReleaseTs:
            self._cacheUserData = {}
            self._cacheReleaseTs = time.time() + self._cacheReleaseSecond


class DimensionDao:
    def __init__(self, DataSource=None, Logger=None, **kwargs):
        self._Logger = Logger
        self.DataSource = DataSource
        if self.DataSource is None:
            nam, host, port, user, pwd, ps = 'Dimension', 'localhost', 27017, '', '', 30  # default
            # from configure file
            strConfigFile = kwargs.get('ConfigFile')
            strSession = kwargs.get('Session', 'Dimension')
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

    def LoadSetting(self):
        try:
            collection = self.DataSource['Setting']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find({}, self._Projection())
        except Exception as e:
            self._Logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        res = {}
        for d in list(cursor):
            if "Name" in d:
                res[d["Name"]] = d
        return res

    def LoadInfo(self):
        try:
            collection = self.DataSource['Info']
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
            cursor = collection.find({}, self._Projection())
        except Exception as e:
            self._Logger.error("%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None
        return list(cursor)

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj


if __name__ == "__main__":
    def GetUserData(user_id):
        import pymongo
        mn = pymongo.MongoClient()
        db = mn['Player']
        res = db['PlayerData'].find_one({"ArkId":user_id})
        return res
    def GetLBInfo():
        import pymongo
        mn = pymongo.MongoClient()
        db = mn['LeaderBoard']
        res = db['Info'].find({})
        return res
    def GetBingoInfo():
        return {}
    d = Dimension(GetUserDataFunc=GetUserData)
    # d.InfoRegister("LeaderBoard", GetLBInfo)
    # id, 10000001,10000002,10000003,10000004,10000006
    # print d.GetDimension('10000001')
    # print d.GetInfo('10000001', serviceName='LeaderBoard')

    # d.InfoRegister("Bingo", GetBingoInfo)
    # print "d.GetInfo('10000001', serviceName='Bingo')"
    # print d.GetInfo('10000001', serviceName='Bingo')
    # print "d.GetDimension('10000001', serviceName='Bingo', bDetail=False)"
    # print d.GetDimension('10000001', serviceName='Bingo', bDetail=False)
    # print "d.GetDimension('10000001', serviceName='Bingo')"
    # print d.GetDimension('10000001', serviceName='Bingo')
    # print "d.GetDimension('10000001')"
    # print d.GetDimension('10000001')
    # print "d.GetDimension('10000001', bDetail=False, serviceName='leaderboard')"
    # print d.GetDimension('10000001', bDetail=False, serviceName="leaderboard")

    # print "d.GetDimension('10000001', serviceName='Data_Trans_Test')"
    # print d.GetDimension('10000001', serviceName='Data_Trans_Test')

    print ("d.GetDimension('10000001', serviceName='TableGroup')")
    print (d.GetDimension('10000001', serviceName='TableGroup'))

    pass
