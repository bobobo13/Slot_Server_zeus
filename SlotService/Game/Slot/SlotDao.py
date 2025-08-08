#!/usr/bin/python
# -*- coding: utf-8 -*-

import traceback
import inspect
import time
from datetime import datetime, UTC
import pymongo
import pymongo.errors
from pymongo import WriteConcern
from ..Module.job_queue_service import JobQueueService
from .SlotDb import SlotDb

class SlotDao():
    GET_TIME = lambda: int(time.time() * 1000)
    GameSnStartValue = 0
    def __init__(self, Logger, SlotDataSource, HistoryDataSource, BackendLogDataSource, bInitDb=True, **kwargs):
        self.logger = Logger
        self.SlotDataSource = SlotDataSource
        self.HistoryDataSource = HistoryDataSource
        self.BackendLogDataSource = BackendLogDataSource

        self._GetTokenFunc = kwargs.get("GetTokenFunc")
        self._GetGameSnFunc = kwargs.get("GetGameSnFunc", None)
        self.LobbyStage = kwargs.get("LobbyStage")

        self.send_custom_event_func = kwargs.get('send_custom_event_func')
        if self.send_custom_event_func is None:
            self.logger.warn("[SlotService] send_custom_event_func is None")

        self._jobQueueServ = JobQueueService("SlotWriteHistory", Logger, workers=4)
        self.log_data = {}
        self.chunk_size = 5000

        self.create_index_collection = []
        self._collection_index = {
            # [(data, is_unique, ttl)]
            'SlotSetting': [([('GameName', pymongo.ASCENDING)], True, None)],
            'GameState': [([('ark_id', pymongo.ASCENDING), ('GameName', pymongo.ASCENDING)], True, None),
                          ([('GameName', pymongo.ASCENDING)], False, None)],
            'SlotMachine': [([('GameName', pymongo.ASCENDING)], True, None)],
            'DataGameReturn': [([('GameName', pymongo.ASCENDING), ('ark_id', pymongo.ASCENDING),
                                 ('GameNo', pymongo.ASCENDING), ('GameSn', pymongo.ASCENDING)], True, None),
                               ([('TTL', pymongo.ASCENDING)], False, 120)  # TTL 120秒
                               ],
            'LogGameReturn': [([('GameName', pymongo.ASCENDING), ('ark_id', pymongo.ASCENDING), ('GameNo', pymongo.ASCENDING), ('GameSn', pymongo.ASCENDING)], True, None)],
            'History': [([('WagersId', pymongo.DESCENDING)], False, None), ([('ark_id', pymongo.DESCENDING), ('CreateTimeTS', pymongo.DESCENDING)], False, None)],
        }

        if bInitDb:
            SlotDb.Initialize(logger=self.logger, DataSource=self.SlotDataSource)

    def load_slot_machine(self):
        collection_name = "SlotMachine"
        collection = self._get_collection(collection_name, data_source=self.SlotDataSource, create_index_key="SlotMachine")
        try:
            cursor = collection.find({})
        except Exception as e:
            self.logger.error("%s.%s\n%s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return {}
        if cursor is None:
            return {}
        result = {}
        for i in cursor:
            result[i['GameName']] = i
        return result

    def GetGameSn(self):
        return self._GetGameSnFunc()

    def _InsertManyLog(self, ds, col, log_list):
        if len(log_list) <= 0:
            return {}
        Coll = ds.get_collection(col, write_concern=WriteConcern(w=1))
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
                chunk = doc[i:i + self.chunk_size]  # 取出 50 筆資料
                self._jobQueueServ.push_job(self._InsertManyLog, ds, col, chunk)

    def async_bulk_insert_log(self, ds, collection_name, log, create_index_key=None):
        if (ds, collection_name) not in self.log_data:
            self.log_data[(ds, collection_name)] = []
        self.log_data[(ds, collection_name)].append(log)
        if self._check_index_create(collection_name, create_index_key):
            self._create_index(collection_name, ds, create_index_key)

    def WriteHistory(self, game_name, user_id, game_no, wagers_id, game_sn, Bet, Win, PlayerData, **kwargs):
        date_key = kwargs.get("DateKey", datetime.now().strftime("%Y%m%d"))
        if "DetailLog" in kwargs:
            doc = kwargs["DetailLog"]
            if "ValueAfter" not in doc:
                doc["ValueAfter"] = kwargs.get("Balance")
        else:
            doc = self.log_filter(game_name, user_id, Bet, Win, PlayerData, game_no=game_no, wagers_id=wagers_id, game_sn=game_sn **kwargs)
        ColName = "{}_{}".format(game_name, date_key)
        if self._jobQueueServ is not None:
            self.async_bulk_insert_log(self.HistoryDataSource, ColName, doc, create_index_key='History')
        else:
            try:
                CollHistory = self.HistoryDataSource[ColName]
                CollHistory.insert_one(doc)
            except:
                self.logger.error("[WriteHistory] GameName={}, UserId={}, GameNo={}, GameSn={}, Bet={}, Win={}, Ts={}, exception={}".format(game_name, user_id, game_no, game_sn, Bet, Win, time.time(), traceback.format_exc()))

    def log_filter(self, game_name, ark_id, bet, win, player_data, game_no=None, wagers_id=None, game_sn=None, chkType="normal", jp_type_code=0,
                   theme_id=1, theme_title=None, reason=0, effBet=None, effWin=None, originBet=None, accWin=0, user_ip='', user_agent=0, fGameRate=0, iRound=1, HistoryDetail=None, dJpContri=None, Balance=None, **kwargs):
        if HistoryDetail is None:
            HistoryDetail = dict()
        if dJpContri is None:
            dJpContri = dict()
        if kwargs is None:
            kwargs = dict()

        game_id = self.LobbyStage.GetGameId(game_name)
        currency = player_data["ThirdPartyCurrency"]
        merchant_id = player_data["MerchantId"]
        if game_no is None:
            game_no = wagers_id

        time_now = kwargs.get("TimeNow", datetime.now())
        time_utc = time_now.astimezone(UTC)
        timestamp = time.mktime(time_now.timetuple())
        timestamp_13 = int(timestamp * 1000)

        ori_bet_value = originBet if originBet is not None else bet
        total_win_value = win
        odd_value = float(accWin) // ori_bet_value

        log = {
                "UID": player_data.get("ThirdPartyId", ""),
                "ProjectID": "arkslot",
                "MerchantId": merchant_id,
                "ThemeID": game_id,
                "TableNO": 0,
                "GameName": game_name,
                "ark_id":ark_id,
                "AccountID": int(ark_id),
                "ThemeTitle": theme_title,
                "MerchantGameID": game_id,
                "GameID": game_id,
                "GameNO": game_no,
                "WagersId": wagers_id,
                "GameSerialID": game_sn,
                "Reason": reason,
                "JPType": jp_type_code,
                "SessionID": str(self._GetTokenFunc(ark_id)) or "",
                "AccountType": player_data.get("ThirdPartyAccountType", 1),
                "MerchantLoginName": player_data.get("ThirdPartyName", ""),
                "UserName": player_data.get("ThirdPartyName", ""),
                "NickName": player_data.get("ThirdPartyNick", ""),
                "ValueType": currency,
                "BetCoin": bet,
                "EffectBet": effBet if effBet is not None else bet,
                "WinAmount": total_win_value,
                "EffectWin": effWin if effWin is not None else total_win_value,
                "CreateTimeUTC": time_utc.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "CreateTimeTS": timestamp_13,
                "CreateTime": time_now.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "TempText":  str(HistoryDetail),
                "TempInt1": 0 if chkType.startswith("normal") else 1,  # NormalGame 0, FreeGame: 1
                "OriginBetValue": ori_bet_value,  # ori_bet(已乘1000倍)
                "WinOdds": odd_value,  # base on ori_bet的odd(已乘1000倍)

                "GrandJPContribution": dJpContri.get('grand', 0),
                "MajorJPContribution": dJpContri.get('major', 0),
                "MinorJPContribution": dJpContri.get('minor', 0),
                "MiniJPContribution": dJpContri.get('mini', 0),

                "BuyExchangeRate": 0,
                "SellExchangeRate": 0,

                "GameClientIP": user_ip,
                "DeviceInfo": user_agent, # 0:Other, 1:Windows, 2:Mac, 3:Linux, 11:iOS, 12:Android, 13:Windows Mobile
                "DisconnectType": 0,     # 僅iGaming使用，Ark一律填0
                "AwardType": 0,
            }
        if Balance is not None:
            log.update({"ValueAfter": Balance})
        return log

    def WriteDetailBetWinLog(self, game_name, user_id, game_no, wagers_id, game_sn, bet, Win, player_data, **kwargs):
        date_key = kwargs.get("DateKey", datetime.now().strftime("%Y%m%d"))
        strColName = "DetailBetWin"
        detailLog = kwargs["DetailLog"]
        detailVBetWinLog = kwargs["DetailBetWinLog"]
        platform_data = kwargs["PlatformData"]
        game_data = kwargs["GameData"]

        betType = detailVBetWinLog.get("BetType", 'NORMAL')
        specialGameId = detailVBetWinLog.get("SpecialGameId", -1)
        chanceRTP = detailVBetWinLog.get("ChanceRTP", 0)
        chanceVersion = detailVBetWinLog.get("ChanceVersion")
        originalBet = detailVBetWinLog.get("OriginalBet")
        extraBet = detailVBetWinLog.get("ExtraBet")
        probGroup = detailVBetWinLog.get("ProbGroup")

        extra_data = {
            "WagersID": wagers_id,
            "BetType":betType,
            "ChanceVersion": chanceVersion,
            "ChanceRTP": chanceRTP,
            "OriginalBet": originalBet,
            "ExtraBet": extraBet,
            "BetAmount": bet,
            "SpecialGameId": specialGameId,
            "ProbGroup": probGroup,
        }

        doc = self.detail_log_filter(detailLog, player_data, platform_data, extra_data=extra_data, **kwargs)
        ColName = "{}_{}".format(strColName, date_key)
        if self._jobQueueServ is not None:
            self.async_bulk_insert_log(self.BackendLogDataSource, ColName, doc)
            if self.send_custom_event_func is not None:
                self.send_custom_event_func("DetailBetWinLog", doc, doc['CreateTs'])
            return
        try:
            CollHistory = self.BackendLogDataSource[ColName]
            CollHistory.insert_one(doc)
        except:
            self.logger.error("[WriteDetailLog] GameName={}, UserId={}, GameNo={}, GameSn={}, exception={}".format(game_name, user_id, game_no, game_sn, traceback.format_exc()))

    def detail_log_filter(self, detailLog, player_data, bet_data, extra_data, **kwargs):
        log = {}

        create_time_ts = detailLog.get("CreateTimeTS")
        log["CreateTs"] = create_time_ts
        create_utc = datetime.fromtimestamp(create_time_ts / 1000.0, UTC).strftime('%Y-%m-%d %H:%M:%S')
        log["CreateUTC"] = create_utc
        log["WagersID"] = extra_data.get("WagersID")
        log["GameNO"] = detailLog.get("GameNO")
        log["MerchantGameID"] = detailLog.get("MerchantGameID")
        log["MerchantGameName"] = detailLog.get("GameName")
        log["GameID"] = detailLog.get("GameID")
        log["GameName"] = detailLog.get("GameName")
        log["UserID"] = detailLog.get("UID")
        log["MerchantID"] = detailLog.get("MerchantId")
        log["MerchantUserAccount"] = detailLog.get("UserName")
        log["AccountType"] = detailLog.get("AccountType")
        log["Nickname"] = detailLog.get("NickName")
        log["Logo"] = detailLog.get("Logo")
        log["LineCode"] = detailLog.get("LineCode")
        log["ArkID"] = detailLog.get("ark_id")
        log["GameClientIP"] = detailLog.get("GameClientIP")
        log["DeviceInfo"] = detailLog.get("DeviceInfo")
        log["SessionID"] = detailLog.get("SessionID")
        log["CreditType"] = player_data.get('ThirdPartyCurrency')
        log["Ratio"] = bet_data.get('GameRatio')
        log["BetType"] = extra_data.get("BetType")
        log["Reason"] = detailLog.get("Reason")
        log["ChanceVersion"] = extra_data.get("ChanceVersion")
        chance_rtp = extra_data.get("ChanceRTP", 0)
        log["ChanceRTP"] = chance_rtp/10000.0
        log["OriginalBet"] = extra_data.get("OriginalBet")
        extra_bet = extra_data.get("ExtraBet")
        log["ExtraBet"] = float(extra_bet) if extra_bet is not None else extra_bet
        bet_amount = extra_data.get("BetAmount")
        win_amount = detailLog.get("WinAmount")
        log["BetAmount"] = bet_amount
        log["WinAmount"] = win_amount
        log["Balance"] = detailLog.get("ValueAfter")
        log["Winnings"] = detailLog.get("Winnings")
        log["SpecialGameId"] = extra_data.get("SpecialGameId")
        log["ProbGroup"] = extra_data.get("ProbGroup")
        log["GameCategory"] = "SLOT"
        log["EffectBet"] = bet_amount
        log["EffectWin"] = win_amount
        return log

    def WriteAnalyticLog(self, ark_id, game_name, game_no, wagers_id, serial_no, player_data, oriBet, bet, extraBet, win, totalWin, result, **kwargs):
        date_key = kwargs.get("DateKey", datetime.now().strftime("%Y%m%d"))
        ColName = "{}_{}".format(game_name, date_key)
        doc = {
            'ark_id': ark_id,
            'Nickname': player_data.get('ThirdPartyName'),
            'Currency': player_data.get('LastCurrency'),
            'MerchantId': player_data.get('MerchantId'),
            'ThirdPartyId': player_data.get('ThirdPartyId'),
        }
        doc.update({'GameNo':game_no, "WagersId":wagers_id, 'SerialNo': serial_no, 'OriBet': oriBet, 'Bet': bet, 'ExtraBet':extraBet, 'Win':win, 'TotalWin': totalWin, 'ProdId':result.get("prodId", "")})
        doc.update(result)

        if self._jobQueueServ is not None:
            self.async_bulk_insert_log(self.BackendLogDataSource, ColName, doc)
            return
        try:
            CollGameLog = self.BackendLogDataSource[ColName]
            CollGameLog.insert_one(doc)
        except:
            self.logger.error("[WriteGameLog] GameName={}, UserId={}, GameNo={}, GameSn={}, exception={}".format(doc.get('GameName'), doc.get('UserId'), game_no, serial_no, traceback.format_exc()))

    def GetGameState(self, ark_id, game_name, bSecondary=False, bCheckLock=False):
        #  bCheckLock: [True:檢查沒上鎖, False:玩家結算]
        collection_name = "GameState"
        result, start = None, None
        query = {"ark_id": ark_id, "GameName": game_name}
        if bCheckLock:
            query["lock"] = 0
            bSecondary = False
        try:
            start = SlotDao.GET_TIME()
            collection = self._get_collection(collection_name, is_secondary_preferred=bSecondary)
            result = collection.find_one(query, self._Projection())
        except:
            self.logger.error("%s.%s %s query: %s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc(), query))
        self.logger.debug("[SlotService][SlotDao][GetGameState] elapsed:{}".format(SlotDao.GET_TIME() - start))
        return result

    def GetLockGameState(self, ark_id, game_name, bForceLock=False):
        #  bForceLock: [True:斷線結算, False:玩家結算]
        collection_name = "GameState"
        query = {"ark_id": ark_id, "GameName": game_name}
        if bForceLock:
            query["lock"] = {"$gte": 0}
            update = {"$set": {'lock': -1}}
        else:
            query["lock"] = 0
            update = {"$set": {'lock': 1}}
        result = None
        try:
            collection = self._get_collection(collection_name, is_secondary_preferred=False)
            result = collection.find_one_and_update(query, update, self._Projection(), upsert=False, return_document=pymongo.ReturnDocument.AFTER)
        except pymongo.errors.DuplicateKeyError:
            self.logger.warn("%s.%s %s query: %s" % (
                str(self.__class__.__name__), get_function_name(), traceback.format_exc(), query))
        except:
            self.logger.error("%s.%s %s query: %s" % (
                str(self.__class__.__name__), get_function_name(), traceback.format_exc(), query))
        return result

    def SetGameState(self, ark_id, game_name, field, upsert=True, new=True, lock=0):
        collection_name = "GameState"
        field["lock"] = 0
        query = {"ark_id": ark_id, "GameName": game_name, "lock": lock}
        update = {"$set": field}
        try:
            start = SlotDao.GET_TIME()
            return_document = pymongo.ReturnDocument.AFTER if new else pymongo.ReturnDocument.BEFORE
            result = self._get_collection(collection_name, is_secondary_preferred=False).update_one(query, update, upsert=upsert)
        except pymongo.errors.DuplicateKeyError:
            self.logger.warn("%s.%s %s query: %s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc(), query))
            return None
        except:
            self.logger.error("%s.%s %s user_id: %s, game_id: %s, field: %s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc(), ark_id, game_name, field))
            self.logger.error("query: %s, field: %s, upsert: %s, new: %s" % (query, field, upsert, new))
            return None
        if result is None and upsert and new:
            return None
        self.logger.debug("[SlotService][SlotDao][SetGameState] elapsed:{}".format(SlotDao.GET_TIME() - start))
        return result

    #  解鎖
    def UnlockGameState(self, ark_id, game_name):
        self.SetGameState(ark_id, game_name, field={}, lock={"$gte": 0})

    def data_game_return_processing (self, ark_id, game_name, game_no, wagers_id, game_return, bet_type="MAINGAME"):
        # bet_type MAINGAME FEVER
        game_return_processing = None
        common_data = {'ark_id': ark_id, 'GameName': game_name, 'GameNo': game_no}

        if game_return is not None:
            if not isinstance(game_return, list):
                game_return = [game_return]
            if len(game_return) <= 0:
                self.logger.error("fever_game_state is empty, GameNo:{}, bet_type:{}".format(game_no, bet_type))
                return None
            game_return_processing = [dict(rtn, WagersId=self.GetGameSn() if bet_type not in  ["MAINGAME", "BUYBONUS"] else wagers_id, BetType=bet_type, **common_data) for idx, rtn in enumerate(game_return)]
        return game_return_processing

    def save_data_game_return(self, game_return_processing):
        collection_name = "DataGameReturn"
        collection = self._get_collection(collection_name, is_secondary_preferred=False, data_source=self.SlotDataSource, create_index_key="DataGameReturn")
        doc = game_return_processing

        try:
            start = int(time.time() * 1000)
            ret = collection.insert_many(doc)
        except pymongo.errors.DuplicateKeyError:
            log_fail_collection = self._get_collection("data_game_return_fail", is_secondary_preferred=False, data_source=self.SlotDataSource, create_index_key="DataGameReturn")
            log_fail_collection.insert_many(doc)
            return False
        except Exception as e:
            self.logger.error("%s.%s\n%s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return False
        self.logger.debug("[SlotService][SlotServiceDao][data_game_return] elapsed:{}".format(SlotDao.GET_TIME() - start))
        if ret is None or not ret.inserted_ids:
            return False
        return True

    def get_data_game_return(self, ark_id, game_name, multi=False):
        # None: error, {}: not find
        collection_name = "DataGameReturn"
        collection = self._get_collection(collection_name, is_secondary_preferred=False, data_source=self.SlotDataSource, create_index_key="DataGameReturn")

        qry = {"ark_id": ark_id, "GameName": game_name}
        # upd={'$set': {"Return": True}}
        try:
            if multi:
                result = collection.find(qry, sort=[("GameSn", pymongo.ASCENDING)])
            else:
                result = collection.find_one(qry, sort=[("GameSn", pymongo.ASCENDING)])
        except:
            self.logger.error("%s.%s\n%s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return None
        return result if result is not None else {}

    def get_and_delete_data_game_return(self, ark_id, game_name):
        collection_name = "DataGameReturn"
        collection = self._get_collection(collection_name, is_secondary_preferred=False, data_source=self.SlotDataSource, create_index_key="DataGameReturn")

        qry = {"ark_id": ark_id, "GameName": game_name}
        # upd={'$set': {"Return": True}}
        try:
            # result = collection.find_one_and_update(qry, update=upd, sort=[("GameSn", pymongo.ASCENDING)], remove=True, return_document=pymongo.ReturnDocument.AFTER)
            result = collection.find_one_and_delete(qry, sort=[("GameSn", pymongo.ASCENDING)])
        except:
            self.logger.error("%s.%s\n%s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return None
        return result

    def delete_data_game_return(self, ark_id, game_name, multi=False):
        collection_name = "DataGameReturn"
        collection = self._get_collection(collection_name, is_secondary_preferred=False, data_source=self.SlotDataSource, create_index_key="DataGameReturn")

        qry = {"ark_id": ark_id, "GameName": game_name}
        upd = {'$set': {"TTL": datetime.now(UTC)}}
        try:
            if multi:
                collection.update_many(qry, update=upd)
                result = list(collection.find(qry).sort("GameSn", pymongo.ASCENDING))
            else:
                result = collection.find_one_and_update(qry, update=upd, sort=[("GameSn", pymongo.ASCENDING)], return_document=pymongo.ReturnDocument.AFTER)

            if not result:
                self.logger.warning("No matching record found for ark_id={}, GameName={}, GameNo={}".format(ark_id, game_name))
                return None
        except pymongo.errors.PyMongoError as e:
            self.logger.error("{}.{}\nMongoDB Error: {}\n{}".format(self.__class__.__name__, get_function_name(), str(e), traceback.format_exc()))
            return None
        except Exception as e:
            self.logger.error(
                "%s.%s\n%s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return None
        return result

    def log_game_return(self, date_key, game_return):
        collection_name = "{}_{}".format("LogGameReturn", date_key)
        collection = self._get_collection(collection_name, is_secondary_preferred=False, data_source=self.HistoryDataSource, create_index_key="LogGameReturn")
        if not isinstance(game_return, list):
            game_return = [game_return]
        try:
            start = int(time.time()*1000)
            ret = collection.insert_many(game_return)
        except pymongo.errors.DuplicateKeyError:
            log_fail_collection = self.HistoryDataSource["log_game_return_fail"]
            log_fail_collection.insert_many(game_return)
            return False
        except Exception as e:
            self.logger.error("%s.%s\n%s" % (str(self.__class__.__name__), get_function_name(), traceback.format_exc()))
            return False
        self.logger.debug("[SlotService][SlotServiceDao][log_game_return] elapsed:{}".format(SlotDao.GET_TIME() - start))
        if ret is None or not ret.inserted_ids:
            return False
        return True

    def _get_collection(self, collection_name, is_secondary_preferred=True, data_source=None, create_index_key=None):
        ds = self.SlotDataSource
        if data_source is not None:
            ds = data_source

        collection = ds[collection_name]
        if self._check_index_create(collection_name, create_index_key):
            self._create_index(collection_name, ds, create_index_key, collection=collection)

        if is_secondary_preferred:
            collection.with_options(read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
        return collection

    def _check_index_create(self, collection_name, create_index_key):
        should_create_index = (create_index_key is not None) and (collection_name not in self.create_index_collection) and (create_index_key in self._collection_index)
        return should_create_index

    def _create_index(self, collection_name, ds, create_index_key, collection=None):
        if collection is None:
            collection = ds[collection_name]
        index_data = self._collection_index[create_index_key]
        for data, is_unique, ttl in index_data:
            self.create_index_collection.append(collection_name)
            if ttl is None:
                collection.create_index(data, unique=is_unique)
            else:
                collection.create_index(data, unique=is_unique, expireAfterSeconds=ttl)

    def _Projection(self, Fields=[]):
        proj = {'_id': True}
        if Fields is not None:
            proj['_id'] = False
            for f in Fields:
                proj[f] = True
        return proj

def get_function_name():
    frame = inspect.currentframe()
    return frame.f_code.co_name if frame else "Unknown"




