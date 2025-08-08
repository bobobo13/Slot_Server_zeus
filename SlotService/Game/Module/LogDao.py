# -*- coding: utf-8 -*-
from future import standard_library
standard_library.install_aliases()
from builtins import str
__author__ = 'allenlin'

import traceback
import os,configparser
import sys, datetime, random, copy
import gevent
import pymongo
import time
from pymongo import WriteConcern
# from ArkEngine.ArkSDK.ArkUtilities import Singleton
# from ArkEngine.ArkSDK.MongoDOM import MongoDOM
# from ArkEngine.ArkSDK import ArkChrono

from ..ArkEngine.ArkSDK.ArkUtilities import Singleton
from ..ArkEngine.ArkSDK.MongoDOM import MongoDOM
from ..ArkEngine.ArkSDK import ArkChrono
try:
    from ..ArkEngine.I17gameSDK.I17gameLog import I17gameLog
except:
    from ..Server.I17gameLog import I17gameLog

from .job_queue_service import JobQueueService
from future.utils import with_metaclass
try:
    from ..ArkCDP.data_logger.modules.kafka_logger import KafkaLogger
except:
    KafkaLogger = None

from .RoutineProc import RoutineProc

class LogDao(with_metaclass(Singleton, MongoDOM)):
    def __init__(self,server=None,logger=None,isTestAnotherCol=False, **kwargs):
        self.mongo_driver = 2 if pymongo.get_version_string().startswith("2.") else 3
        MongoDOM.__init__(self,server,logger)
        self._ensureIndexList_dict={}
        self.isTestAnotherCol=isTestAnotherCol
        self._col_dict={}
        self.logPath = kwargs.get('logConfigPath')
        if self.logPath is None and self.arkpath is not None:
            self.logPath = str(os.path.join(self.server.getArkPath(self.arkpath), 'config', self.server.version))
        if self.logPath is not None:
            logConfigPath=os.path.join(self.logPath, 'ark_log.cfg')
            self.initByConfigPath(logConfigPath)
        self.backendLogDataSrc = kwargs.get('BackendLogDataSrc')
        self.gameLogDataSrc = kwargs.get('GameLogDataSrc')
        self.__logJobQueue = JobQueueService("BackendLogJobs", self.logger, workers=8)
        # self._KafkaLogger = None
        # if KafkaLogger is not None and not server.is_test:
        #     self._KafkaLogger = KafkaLogger(logger, **kwargs)

        if True or kwargs.get('RoutineProcEnable'):
            self.routine_service = RoutineService(logger)
            self.routine_service.register(self.put_queue_log)
            self.log_data = {}
            self.chunk_size = 5000

        # self.indexedCollections = {
        #     "GameConsume":  [{ "keys":["OrderID"]            , "unique":True  }, { "keys":["UserID"], "unique":False }],
        #     "SessionActive":[{ "keys":["UserID", "LoginTime"], "unique":False }],
        #     "SessionLength":[{ "keys":["UserID", "StartTime"], "unique":False }],
        #     "AccountCreate":[{ "keys":["UserID"]             , "unique":False }],
        #     "AccountBind":  [{ "keys":["AutoId"], "unique":False }, { "keys":["FromType"], "unique":False }, { "keys":["OriFromType"], "unique":False }, { "keys":["Status"], "unique":False }],
        # }
        # gevent.spawn(self.updateIndex)

    def updateIndex(self):
        self.dateStr = ""
        while True:
            try:
                todayStr = self.__getTodayDateRepr()
                if self.dateStr != todayStr:
                    self.dateStr = todayStr
                    gevent.sleep(float(random.randint(0,2000))/1000)  # prevent thunder effect
                    for k, constraint in list(self.indexedCollections.items()):
                        self.__buildIndex(k, constraint)
            except:
                self.logger.error('[LogDAO]updateIndex Err!callstack={}'.format(traceback.format_exc()))

            gevent.sleep(1)

    def __buildIndex(self, colName, constraints):
        col=self.backendLogCol(colName, ArkChrono.datetime_gmt(self.backendLogGMTTime), False)
        for c in constraints:
            try:
                idx = [(key, pymongo.ASCENDING) for key in c["keys"]]
                is_unique =  c["unique"]
                col.create_index(idx, unique=is_unique)
            except:
                self.logger.error('[LogDAO]__buildIndex Err!callstack={}'.format(traceback.format_exc()))

    def __getTodayDateRepr(self):
        GMTTime=self.backendLogGMTTime
        return ArkChrono.datetime_gmt(GMTTime).strftime('%Y%m%d')
        #return datetime.now().strftime('%Y%m%d')

    def isUserInWhiteList(self,ark_id):
        try:
            return self.isTestAnotherCol and self.server.getIsTestAcc(ark_id)
        except:
            return False


    def logAccountCreate(self,baseDoc):
        self.CommonLog(baseDoc, 'AccountCreate')

    def logSessionActive(self, baseDoc):
        self.CommonLog(baseDoc, 'SessionActive')

    def logSessionLength(self, baseDoc):
        self.CommonLog(baseDoc, 'SessionLength')

    def logGameConsume(self, baseDoc):
        self.CommonLog(baseDoc, 'GameConsume')

    def logSessionCoin(self, baseDoc):
        self.CommonLog(baseDoc, 'SessionCoinLog')

    def logDetailBetWin(self, baseDoc):
        self.CommonLog(baseDoc, 'DetailBetWinLog', bSendSplunk=False)

    def async_bulk_insert_log(self, ds, ColName, log):
        if (ds, ColName) not in self.log_data:
            self.log_data[(ds, ColName)] = []
        self.log_data[(ds, ColName)].append(log)

    def _InsertManyLog(self, ds, col, log_list):
        if len(log_list) <= 0:
            return {}
        # Coll = ds[col]
        Coll = ds.get_collection(col, write_concern=WriteConcern(w=1))
        # operations = [InsertOne(log) for log in log_list]
        # result = Coll.bulk_write(operations, ordered=False)
        result = Coll.insert_many(log_list, ordered=False)
        return {
            "nInserted": len(result.inserted_ids),
            "writeErrors": [],
            "writeConcernErrors": []
        }

    def put_queue_log(self):
        for key, doc in self.log_data.items():
            ds, col = key
            if len(doc) <= 0:
                continue
            self.log_data[key] = []
            for i in range(0, len(doc), self.chunk_size):
                chunk = doc[i:i + self.chunk_size]
                self.__logJobQueue.push_job(self._InsertManyLog, ds, col, chunk)

    def addSessionGameLog(self, baseDoc, bSessionGame=True, bSessionBetWin=True):
        if bSessionGame:
            self.logSessionGame(baseDoc)
        if bSessionBetWin:
            self.logSessionBetWin(baseDoc)

    def logSessionGame(self, baseDoc, **kwargs):
        date_key = kwargs.get("DateKey", datetime.datetime.now().strftime("%Y%m%d"))
        ColName = "{}_{}".format('SessionGame', date_key)
        ds = self.backendLogDB()
        self.async_bulk_insert_log(ds, ColName, baseDoc)

    def logSessionBetWin(self, baseDoc, **kwargs):
        coinLog = {
            "WagersId": baseDoc.get("WagersId"),
            "Currency": baseDoc.get("Currency", "Coin"),
            "Bet": baseDoc.get("BetCoin", 0),
            "Win": baseDoc.get("WinAmount", 0),
            "ValueAfter": baseDoc.get('ValueAfter', 0),
            "ValueBefore": baseDoc.get('ValueBefore', 0),
            "CreateTs": time.time(),
        }

        date_key = kwargs.get("DateKey", datetime.datetime.now().strftime("%Y%m%d"))
        ColName = "{}_{}".format('SessionBetWin', date_key)
        ds = self.backendLogDB()
        self.async_bulk_insert_log(ds, ColName, coinLog)

    def logFishSessionBetWin(self, ArkId, SessionID=None, Theme=None, StartTimeTs=-1, LastTimeTs=-1, StartTime='', LastTime='',
                         LogoId=-1, KioskId=-1, AccountId=-1, GameId=-1, EventId=-1, SingleBet=-1, TotalBet=-1, TotalBetCount=-1,
                         TotalWinCount=-1, TotalWin=-1, BetCreditType=-1, WinCreditType=-1, EndWinCredit=-1, EndBetCredit=-1,
                         DeviceType=-1, Channel=-1, bSendSplunk=False, **data):
        baseDoc = dict()
        baseDoc["ArkId"] = ArkId
        baseDoc["SessionID"] = SessionID
        baseDoc['Theme'] = Theme
        baseDoc["StartTimeTs"] = StartTimeTs
        baseDoc["LastTimeTs"] = LastTimeTs
        baseDoc["StartTime"] = StartTime
        baseDoc["LastTime"] = LastTime
        baseDoc["LogoId"] = LogoId
        baseDoc["KioskId"] = KioskId
        baseDoc["AccountId"] = AccountId
        baseDoc["GameId"] = GameId
        baseDoc["EventId"] = EventId
        baseDoc["SingleBet"] = SingleBet
        baseDoc["TotalBet"] = TotalBet
        baseDoc["TotalBetCount"] = TotalBetCount
        baseDoc["TotalWinCount"] = TotalWinCount
        baseDoc["TotalWin"] = TotalWin
        baseDoc["BetCreditType"] = BetCreditType
        baseDoc["WinCreditType"] = WinCreditType
        baseDoc["EndWinCredit"] = EndWinCredit
        baseDoc["EndBetCredit"] = EndBetCredit
        baseDoc["DeviceType"] = DeviceType
        baseDoc["Channel"] = Channel
        self.CommonLog(baseDoc, "FishSessionBetWin", bSendSplunk=bSendSplunk)

    # TODO 確認是否保留
    def logLogin_out(self, baseDoc):
        self.CommonLog(baseDoc, 'LoginLogout')

    def gameLogDB(self):
        if self.gameLogDataSrc is not None:
            return self.gameLogDataSrc
        return self.logDB(self.gameLogMongo, self.gameLogDBName, self.gameLogDBUser, self.gameLogDBPwd, self.gameLogGMTTime)

    def gameLogCol(self,col_name,createDateTime,isTest):
        return self.logCol(self.gameLogDB(), col_name, createDateTime, isTest)

    def gameInsertLog(self,col_name,ark_id,*args,**kwargs):
        col = self.gameLogCol(col_name, ArkChrono.datetime_gmt(self.gameLogGMTTime), self.isUserInWhiteList(ark_id))
        return self.insertLog(col,*args,**kwargs)

    def insertPcuLog(self,col_name,ark_id,*args,**kwargs):
        log_time = datetime.datetime.strptime(str(args[0]['date']), "%Y%m%d")+ datetime.timedelta(hours=args[0]['hour'])
        col = self.gameLogCol(col_name, log_time, self.isUserInWhiteList(ark_id))
        return self.insertLog(col,*args,**kwargs)

    def backendLogDB(self):
        if self.backendLogDataSrc is not None:
            return self.backendLogDataSrc
        return self.logDB(self.backendLogMongo, self.backendLogDBName, self.backendLogDBUser, self.backendLogDBPwd, self.backendLogGMTTime)

    def backendLogCol(self,col_name,createDateTime,isTest):
        return self.logCol(self.backendLogDB(), col_name, createDateTime, isTest)

    def synchronousBackendInsertLog(self,col_name,ark_id,*args,**kwargs):
        col = self.backendLogCol(col_name, ArkChrono.datetime_gmt(self.backendLogGMTTime), self.isUserInWhiteList(ark_id))
        if self.mongo_driver == 2:
            return col.insert(*args, **kwargs)
        else:
            return col.insert_one(*args, **kwargs)

    def backendInsertLog(self,col_name,ark_id,*args,**kwargs):
        col = self.backendLogCol(col_name, ArkChrono.datetime_gmt(self.backendLogGMTTime), self.isUserInWhiteList(ark_id))
        self.insertLog(col,*args,**kwargs)

    def backendAsyncInsertLog(self, col_name, ark_id, *args, **kwargs):
        create_datetime = ArkChrono.datetime_gmt(self.backendLogGMTTime)
        self.__logJobQueue.push_job(self.asyncInsertLog, col_name, ark_id, create_datetime, *args, **kwargs)

    '''
    def backendInsertLogByDate(self,col_name,ark_id,date_time=None,*args,**kwargs):
        CreateDateTime = date_time if date_time is not None else ArkChrono.datetime_gmt(self.backendLogGMTTime)
        col=self.backendLogCol(col_name, CreateDateTime, self.isUserInWhiteList(ark_id))
        return self.insertLog(col,*args,**kwargs)
    '''
    def backendBulkInsertLogByDate(self,col_name,ark_id,date_time=None,logList=None):
        if logList == None:
            return
        CreateDateTime = date_time if date_time is not None else ArkChrono.datetime_gmt(self.backendLogGMTTime)
        col = self.backendLogCol(col_name, CreateDateTime, self.isUserInWhiteList(ark_id))
        return self.bulkInsertLog(col,logList)

    def insertLog(self, col , *args, **kwargs):
        self.__logJobQueue.push_job(col.insert if self.mongo_driver == 2 else col.insert_one, *args, **kwargs)

    def asyncInsertLog(self, col_name, ark_id, create_datetime, *args, **kwargs):
        is_white = self.isUserInWhiteList(ark_id)
        col = self.backendLogCol(col_name, create_datetime, is_white)
        if self.mongo_driver == 2:
            col.insert(*args, **kwargs)
        else:
            col.insert_one(*args, **kwargs)

    def bulkInsertLog(self,col,log_list):
        self.__logJobQueue.push_job(self.asyncBulkInsertLog, col, log_list)

    def insertSplunkLog(self, *args, **kwargs):
        self.__logJobQueue.push_job(self.SplunkLog.splunk_send_define_log, *args, **kwargs)

    def asyncBulkInsertLog(self, col, log_list):
        bulk = col.initialize_unordered_bulk_op()
        for log in log_list:
            bulk.insert(log)
        return bulk.execute()

    def setIndex(self,col_name,**kwargs):
        if col_name not in self._ensureIndexList_dict:
            self._ensureIndexList_dict[col_name]=[]
        self._ensureIndexList_dict[col_name].append(kwargs)

    def logCol(self,db,col_name,createDateTime,isTest):
        result=None
        col_name_date = col_name
        if self.isTestAnotherCol and isTest:
            col_name_date = col_name_date + '_Test'
        col_name_date=col_name_date + '_' + createDateTime.strftime('%Y%m%d')

        if col_name_date not in self._col_dict:
            result=db[col_name_date]
            self._col_dict[col_name_date]=result
            if col_name in self._ensureIndexList_dict:
                ensureIndex=self._ensureIndexList_dict[col_name]
                for args in ensureIndex:
                    KeyName = 'key_or_list' if self.mongo_driver == 3 else 'keys'
                    if args.get(KeyName) is not None:
                        result.create_index(args[KeyName], unique=args.get('unique'), expireAfterSeconds=args.get('expireAfterSeconds'))
                    else:
                        result.create_index(**args)
        else:
            result=self._col_dict[col_name_date]
        return result

    def logDB(self,mongo,DBName,DBUser,DBPwd,GMTTime):
        logdb = mongo[DBName]
        if DBUser is not None:
            logdb.authenticate(DBUser,DBPwd, source='admin')
        return logdb

    def initByConfigPath(self,logConfigPath):
        self.logConfigPath=logConfigPath
        logConfig = configparser.RawConfigParser()
        logConfig.read(self.logConfigPath, encoding='utf-8')
        self.initByConfigParser(logConfig)

    def initByConfigParser(self,logConfig):
        self.logConfig=logConfig
        self.gameLogMongo = self._mongoClientConfigParse(self.logConfig, 'GameLog')
        self.backendLogMongo = self._mongoClientConfigParse(self.logConfig, 'BackendLog')
        self.tournamentLogMongo = self._mongoClientConfigParse(self.logConfig, 'GameLog') #tournamentLogMongo=self._mongoClientConfigParse(self.logConfig,'TournamentBackendLog')

        self.gameLogDBName, self.gameLogDBUser, self.gameLogDBPwd, self.gameLogGMTTime = self._getDBInfoByConfigParser(self.logConfig, 'GameLog')
        self.backendLogDBName, self.backendLogDBUser, self.backendLogDBPwd, self.backendLogGMTTime = self._getDBInfoByConfigParser(self.logConfig, 'BackendLog')
        self.tournamentLogDBName, self.tournamentLogDBUser, self.tournamentLogDBPwd, self.tournamentLogGMTTime = self._getDBInfoByConfigParser(self.logConfig, 'GameLog') #databaseName,MongoUser,MongoPwd,GMTTime=self._getDBInfoByConfigParser(self.logConfig,'TournamentBackendLog')

        self.SplunkLog = I17gameLog(self.logPath)

    def CommonLogWithSplunk(self, baseDoc, strColName, FieldFilter=None, strSplunkName=None, BackendMethod=backendAsyncInsertLog, bErrorKeep=False, bSendSplunk=True):
        return self.CommonLog(baseDoc, strColName, FieldFilter, strSplunkName, BackendMethod, bErrorKeep, bSendSplunk)

    def CommonLog(self, baseDoc, strColName, FieldFilter=None, strSplunkName=None, BackendMethod=backendAsyncInsertLog, bErrorKeep=False, bSendSplunk=False):
        """
        By default, this is Non-Blocking
        """
        log = copy.deepcopy(baseDoc) # 避免放在queue中準備入Mongo的資料被加上_id
        err = None
        # filter
        if FieldFilter is not None:
            try:
                log, err = FieldFilter(baseDoc)
            except:
                self.logger.error("Except by fileter!! %s.%s %s \r\n baseDoc:%s \r\n log:%s, common err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc(),baseDoc,log,err))
                if not bErrorKeep:
                    return
            if err is not None:
                self.logger.error("%s.%s filter result is None %s, log:%s, common err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,baseDoc,log,err))
                if not bErrorKeep:
                    return
        slog = copy.copy(log) # 避免放在queue中準備入Mongo的資料被加上_id
        # backend
        try:
            ark_id = str(baseDoc.get('ark_id',0))
            BackendMethod(self, strColName, ark_id, log)
        except:
            self.logger.error("Except by backend!! %s.%s %s \r\n baseDoc:%s \r\n log:%s, common err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc(),baseDoc,log,err))
        # splunk
        if bSendSplunk:
            try:
                strName = strSplunkName if (strSplunkName is not None) and (len(strSplunkName) > 0) else strColName
                self.insertSplunkLog(strName, slog, slog.get('CreateTime'))
            except:
                self.logger.error("Except by splunk!! %s.%s %s \r\n baseDoc:%s \r\n log:%s, common err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc(),baseDoc,slog,err))
        # # kafka
        # if self._KafkaLogger is not None:
        #     topic = "CommonLog_"+ strColName
        #     self._KafkaLogger.send_data(topic, slog)

    def CommonLogList(self, baseDocList, strColName, FieldFilter=None, strSplunkName=None, date_time=None, bGetArkId=False, bSendSplunk=True):
        """
        This is Blocking
        """
        log = None
        err = None
        # filter
        ark_id = None
        log_list = baseDocList
        if FieldFilter is not None:
            log_list = []
            for baseDoc in baseDocList:
                if bGetArkId and (ark_id is None) and (baseDoc.get('UserID') is not None):
                    ark_id = str(baseDoc.get('UserID',0))
                try:
                    log, err = FieldFilter(baseDoc)
                except:
                    self.logger.error("Except by fileter!! %s.%s %s \r\n baseDoc:%s \r\n log:%s, common err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc(),baseDoc,log,err))
                    continue
                if err is not None:
                    self.logger.warn("%s.%s filter result is None %s, log:%s, err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,baseDoc,log,err))
                    continue
                log_list.append(log)
        slog_list = copy.deepcopy(log_list) # 避免放在queue中準備入Mongo的資料被加上_id
        # backend
        try:
            self.backendBulkInsertLogByDate(strColName, ark_id, date_time, log_list)
        except:
            self.logger.error("Except by backend!! %s.%s %s \r\n baseDoc:%s \r\n log:%s, common err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc(),baseDocList,log,err))
        # splunk
        if bSendSplunk:
            for log in slog_list:
                try:
                    strName = strSplunkName if (strSplunkName is not None) and (len(strSplunkName) > 0) else strColName
                    self.insertSplunkLog(strName, log, log.get('CreateTime'))
                except:
                    self.logger.error("Except by splunk!! %s.%s %s \r\n baseDoc:%s \r\n log:%s, common err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc(),baseDocList,log,err))
                    continue

    def _logGameConsume(self, baseDoc, strColName, strColFailed, FieldFilter, strSplunkName=None, strSplunkFailed=None):
        log=None
        err=None
        try:
            log,err=FieldFilter(baseDoc)
            if err is not None:
                self.logger.warn("%s.%s filter result is None %s, log:%s, err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,baseDoc,log,err))

            slog = copy.deepcopy(log) # 避免放在queue中準備入Mongo的資料被加上_id
            ark_id = str(baseDoc.get('UserID',0))
            if 'ErrorCode' not in slog:
                self.synchronousBackendInsertLog(strColName,ark_id,slog)
                strName = strSplunkName if (strSplunkName is not None) and (len(strSplunkName) > 0) else strColName
                self.SplunkLog.splunk_send_define_log(strName, slog, slog.get('CreateTime'))
                return True
                #self.backendInsertLog('GameConsume',ark_id,log)
            else:
                self.synchronousBackendInsertLog(strColFailed,ark_id,slog)
                strName = strSplunkFailed if (strSplunkFailed is not None) and (len(strSplunkFailed) > 0) else strColFailed
                self.SplunkLog.splunk_send_define_log(strName, slog, slog.get('CreateTime'))
                return False
                #self.backendInsertLog('GameConsumeFailed',ark_id,log)
        except:
            self.logger.error("%s.%s %s \r\n baseDoc:%s \r\n log:%s, err:%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc(),baseDoc,log,err))
            return False

    def WaitQueueEmpty(self, timeout=None):
        return self.__logJobQueue.wait_queue_empty(timeout)

class RoutineService(RoutineProc):
    def __init__(self, logger):
        super(RoutineService, self).__init__("LogDao", 1, func=self.Reload, logger=logger)
        self.target = None

    def Reload(self, bForce=True):
        for func in self.target:
            func()

    def register(self, func):
        if self.target is None:
            self.target = []
        self.target.append(func)