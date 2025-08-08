#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "eitherwang"


from ..Util.MathTool import *
import time

"""
主要Public Method
updJpInfo(self, strInfoKey, strPoolGroup, jpInfo) -> None: 設定jpInfo給InfoKey
AddPool(self, strInfoKey, strPoolGroup, bet) -> None: 灌分
RevertPool(self, strInfoKey, strPoolGroup, bet) -> None: 扣除灌分(rollback)
GetJpStatus(self, strInfoKey, strPoolGroup) -> dict{jpLevel: AwardPoolValue}: 取得Jp累積數值
CheckJpBuffer(self, strInfoKey, strPoolGroup, line_bet) -> dict{jpLevel: bool}: 每一個JpLevel是否BufferPool已累積到允取出獎狀態
WinJp(self, strInfoKey, strPoolGroup, level, line_bet, ark_id=None, playerData=None, FloatPrecision=0) -> dict: 開獎，回傳開獎內容

主要參數:
strInfoKey: 要採用哪一組機率參數設定的Key
strPoolGroup: 要使用哪一個JP Pool，對應到DB的Pool Index欄位
jpInfo: JP機率設定表，結構詳見updJpInfo()的說明
bet: 是Total bet
line_bet: 這才是line_bet 
"""


class BaseIngameJp(object):
    POOL_REFRESH_INTERVAL = 3

    def __init__(self, game_id, gameSetting, logger, **kwargs):
        self.logger = logger
        self.game_id = str(game_id)
        # self.gameInfo = game_info   # 僅使用規格面資料(max_lines/max_ways, progressive_level_num, jpTypeLevelMap) 勿取用機率面資料
        self._gameSetting = gameSetting
        self.levelNum = gameSetting['progressive_level_num']
        self.jpInfo = dict()
        self._poolData = dict()
        self.bet_lines = gameSetting.get("max_lines", gameSetting.get("max_ways", 0))
        self.FloorFloat = kwargs.get("FloorFloatFunc", self._FloorFloat)
        self.getUserData = kwargs.get("getUserData")
        self._MongoDao = kwargs.get("MongoDao")
        self._PoolDao = kwargs.get("PoolDao")
        self._init_data()

        self._IsJpStatusWithBaseValue = self._gameSetting.get("JpStatusWithBaseValue", False)

    def _init_data(self):
        '''
        各遊戲JP需要特別的資料在這邊初始化
        '''
        pass

    def updJpInfoFromGameInfo(self, strInfoKey, strPoolGroup, gameInfo):

        JpInfo = gameInfo.get("jp_info", gameInfo.get("game_rate",{}).get("jp_info", gameInfo.get("game_rate", {}).get("extra_odds", {}).get("jp_info")))
        self.updJpInfo(strInfoKey, strPoolGroup, JpInfo)

    def updJpInfo(self, strInfoKey, strPoolGroup, jpInfo):
        """
        jpInfo: {
            jp_level(str of int): {
                init_value: (float) fixed init value
                base_value: (float, optional) init value for bet-related jp when max linebet
                top_value: (float, optional) max award for jp / bet-related jp
                max_bet_value: (float, optional) max linebet for bet-related jp
                acc: (float) acc from linebet * cost/lines
                bufferPoolAcc: (dict: {linebet:float}) acc for bufferpool
            }, {...},
        }
        """
        self.jpInfo[strInfoKey] = jpInfo


    def initPool(self, strInfoKey, strPoolGroup):
        aPool = {}
        bPool = {}
        for level in range(self.levelNum):
            aPool[str(level)] = self.jpInfo[strInfoKey][str(level)].get("init_value", 0)
            bPool[str(level)] = 0
        return self._PoolDao.InitPool(self.game_id, strPoolGroup, aPool, bPool)

    def updatePoolData(self, strInfoKey, strPoolGroup):
        aPool, bPool = self._PoolDao.GetBothPool(self.game_id, strPoolGroup)
        if aPool is None or len(aPool) <= 0:
            r = self.initPool(strInfoKey, strPoolGroup)
            if r is not None:  # FindAndModify by Mongo
                aPool, bPool = r
            else:   # Set by Redis
                aPool, bPool = self._PoolDao.GetBothPool(self.game_id, strPoolGroup)
        # else:
        #     bPool = self._PoolDao.GetBufferPool(self.game_id, strPoolGroup)
        poolData = {
            "UpdateTs": time.time(),
            "AwardPool": aPool,
            "BufferPool": bPool
        }
        self._poolData[strPoolGroup] = poolData

    # Add Pool
    def AddPool(self, strInfoKey, strPoolGroup, bet):
        jpStatus = self.GetJpStatus(strInfoKey, strPoolGroup)
        jpInfo = self.jpInfo[strInfoKey]
        contriDict = {}
        deltaAwardPool= {}
        deltaBufferPool = {}
        for level in range(self.levelNum):
            aContriCoin = self._awardPoolVal(jpInfo, level, bet)
            contriDict[self._get_jp_type_by_jp_level(str(level))] = aContriCoin
            aPoolCoin = floor_float(aContriCoin, 6)
            deltaAwardPool[level] = aPoolCoin
            deltaBufferPool[level] = floor_float(self._bufferPoolVal(jpInfo, level, bet, aPoolCoin), 6)

            # 新增檢查 AwardPool 上限, 由於上面 jpStatus 不考慮押注段 因此這裡 top 值也不需考慮
            top_value = self._getOddsTopVal(strInfoKey, level)
            if jpStatus[str(level)] >= top_value > 0:
                deltaAwardPool[level] = 0

        self._PoolDao.incPool(self.game_id, strPoolGroup, deltaAwardPool, deltaBufferPool)
        return contriDict

    def RevertPool(self, strInfoKey, strPoolGroup, bet):
        jpInfo = self.jpInfo[strInfoKey]
        deltaAwardPool = {}
        deltaBufferPool = {}
        for level in range(self.levelNum):
            aContriCoin = self._awardPoolVal(jpInfo, level, bet)
            aPoolCoin = floor_float(aContriCoin, 6)
            deltaAwardPool[level] = aPoolCoin
            deltaBufferPool[level] = floor_float(self._bufferPoolVal(jpInfo, level, bet, aPoolCoin), 6)
        self._PoolDao.revertPool(self.game_id, strPoolGroup, deltaAwardPool, deltaBufferPool)

    def _awardPoolVal(self, jpInfo, iLevel, bet):
        '''
        回傳本次total bet要灌入pool的金額
        '''
        acc = jpInfo[str(iLevel)].get("acc", 0)
        aVal = bet * acc
        aVal += self._addExtraValue(jpInfo, iLevel, bet)

        # print 'total_bet',total_bet,'acc',acc,'game_rate',game_rate,'=',(total_bet * acc * game_rate)
        return aVal

    def _addExtraValue(self, jpInfo, iLevel, bet):
        '''
        回傳本次額外要灌入的錢
        '''
        return 0

    # def _cal_baby_fund_add_coins(self, total_bet, jp_type, total_value):
    def _bufferPoolVal(self, jpInfo, iLevel, bet, aVal):
        bVal = aVal
        acc = None
        if "bufferPoolAcc" in jpInfo[str(iLevel)]:
            acc = jpInfo[str(iLevel)]["bufferPoolAcc"]
        elif "all" in jpInfo and "bufferPoolAcc" in jpInfo["all"]:
            acc = jpInfo["all"]["bufferPoolAcc"]
        if acc is None:
            return bVal
        if isinstance(acc, dict):
            betKey = float_to_string_without_tail_zero(bet, 3).replace(".", "_")
            acc = acc.get(betKey, acc.get("all", 1))
        bVal *= acc
        return bVal

    def GetJpStatus(self, strInfoKey, strPoolGroup, nFloatPrecision=None):
        if strPoolGroup not in self._poolData or \
                self._poolData[strPoolGroup]["UpdateTs"] < time.time() - self.POOL_REFRESH_INTERVAL:
            self.updatePoolData(strInfoKey, strPoolGroup)

        jp_status = {}
        for level, award_value in self._poolData[strPoolGroup]['AwardPool'].iteritems():
            base_value = self._getOddsInitVal(strInfoKey, level)
            top_value = self._getOddsTopVal(strInfoKey, level)
            top_aval = top_value - base_value
            jp_status[level] = top_aval if award_value >= top_aval > 0 else award_value
            if self._IsJpStatusWithBaseValue:
                jp_status[level] += base_value
            if nFloatPrecision is not None:
                jp_status[level] = self.FloorFloat(jp_status[level], nFloatPrecision)
        return jp_status

    def CheckJpBuffer(self, strInfoKey, strPoolGroup, line_bet):
        BufferAvail = {}
        if strPoolGroup not in self._poolData or \
                self._poolData[strPoolGroup]["UpdateTs"] < time.time() - self.POOL_REFRESH_INTERVAL:
            self.updatePoolData(strInfoKey, strPoolGroup)

        for level in range(self.levelNum):
            BufferAvail[level] = float(self._poolData[strPoolGroup]["BufferPool"][str(level)]) >= sum(self.getJpAward(strInfoKey, strPoolGroup, level, line_bet))
        return BufferAvail


    # WinJP
    def WinJp(self, strInfoKey, strPoolGroup, level, line_bet, ark_id=None, playerData=None, FloatPrecision=0):
        
        # currency = user_info["Currency"]
        # merchant_id = user_info["MerchantId"]

        # 取得redis鎖
        # 避免同時有多人同時中JP，發出重複的pool獎金
        # 太久都取得不到lock，跳出錯誤
        if not self._PoolDao.getLock(self.game_id, strPoolGroup):
            self.logger.error('[{}Jackpot] WinJp: cant get lock'.format(self.game_id))
            self._PoolDao.releaseLock(self.game_id, strPoolGroup)
            return None
        # 同步 DB
        self.updatePoolData(strInfoKey, strPoolGroup)
        pool_award, base_award = self.getJpAward(strInfoKey, strPoolGroup, level, line_bet)
        self.logger.info('[{}Jackpot] WinJp, ark_id:{}, poolAward:{}, baseAward:{}'.format(self.game_id, ark_id, pool_award, base_award))
        total_award = pool_award + base_award
        if total_award <= 0:
            self.logger.error('[{}] WinJp: cant get award'.format(self.game_id))
            self._PoolDao.releaseLock(self.game_id, strPoolGroup)
            return None
        self.resetPool(strInfoKey, strPoolGroup, level, pool_award, base_award)
        total_award = self.FloorFloat(total_award, FloatPrecision=FloatPrecision, ark_id=ark_id, playerData=playerData)
        # 回傳
        jp_type = self._get_jp_type_by_jp_level(str(level))
        ret_data = {
            'award': total_award,
            'pool': self._poolData[strPoolGroup]["AwardPool"][str(level)],
            'Perform_time': 20,  # self.jackpot_setting['client_perform_time'],
            'jp_type': jp_type
        }

        self.addWinnerList(level, total_award, ark_id, playerData)

        # 釋放redis鎖
        self._PoolDao.releaseLock(self.game_id, strPoolGroup)
        self.logger.info("WinJp, jp return data = {}".format(ret_data))

        return ret_data

    def getJpAward(self, strInfoKey, strPoolGroup, level, line_bet=None):
        if line_bet is None:
            line_bet = 0
            if "max_bet_value" in self.jpInfo[strInfoKey][str(level)]:
                line_bet = self.jpInfo[strInfoKey][str(level)]["max_bet_value"]
        # print 'get_jackpot_award_and_reset_pool  level: ', level, ', line_bet: ', line_bet
        pool_award = self.getPoolAward(strInfoKey, strPoolGroup, level, line_bet)
        base_award = self._getOddsInitVal(strInfoKey, level, line_bet)
        total_award = pool_award + base_award
        # print 'pool: ', pool_award, ', base_value: ', base_award

        # 新增檢查 TopValue 上限
        top_value = self._getOddsTopVal(strInfoKey, level, line_bet)
        if total_award >= top_value > 0:
            total_award = top_value
            pool_award = total_award - base_award

        return pool_award, base_award

    def resetPool(self, strInfoKey, strPoolGroup, level, pool_award, base_award):
        # pool_level_info = self._poolData[strPoolGroup]['AwardPool'][str(level)]
        # jp_level_info = self.jpInfo[strInfoKey][str(level)]
        total_award = pool_award + base_award
        # 重設pool
        reset_value = pool_award - self._getFixedInitVal(strInfoKey, level)
        self.logger.debug("InGameJackpot: total award= {}, reset value ={}, after reset pool value = {}".format(total_award, -reset_value, total_award - reset_value))
        self._PoolDao.DecBufferPool(self.game_id, strPoolGroup, level, total_award)
        self._PoolDao.DecAwardPool(self.game_id, strPoolGroup, level, reset_value)
        self.updatePoolData(strInfoKey, strPoolGroup)

    def getPoolAward(self, strInfoKey, strPoolGroup, level, line_bet):
        '''
        拉中JP從獎金池裡取出的金額
        若沒有特殊的規則，就是直接將pool的數值回傳
        :return:
        '''
        aPoolVal = self._poolData[strPoolGroup]['AwardPool'][str(level)]
        if 'max_bet_value' in self.jpInfo[strInfoKey][str(level)]:
            ratio = float(line_bet) / self.jpInfo[strInfoKey][str(level)]['max_bet_value']
            if ratio > 1.0:
                ratio = 1.0
            aPoolVal *= ratio
        return aPoolVal

    def getInitVal(self, strInfoKey, level, line_bet=None):
        return self._getFixedInitVal(strInfoKey, level) + self._getOddsInitVal(strInfoKey, level, line_bet)

    def _getFixedInitVal(self, strInfoKey, level):
        if str(level) not in self.jpInfo[strInfoKey]:
            return 0
        return self.jpInfo[strInfoKey][str(level)].get('init_value', 0)

    def _getOddsInitVal(self, strInfoKey, level, line_bet=None):
        if str(level) not in self.jpInfo[strInfoKey]:
            return 0
        if 'max_bet_value' not in self.jpInfo[strInfoKey][str(level)]:
            return 0
        if line_bet is None or line_bet > self.jpInfo[strInfoKey][str(level)]['max_bet_value']:
            line_bet = self.jpInfo[strInfoKey][str(level)]['max_bet_value']
        if 'base_odds' in self.jpInfo[strInfoKey][str(level)]:
            return self.jpInfo[strInfoKey][str(level)]['base_odds'] * self.bet_lines * line_bet
        elif 'base_value' in self.jpInfo[strInfoKey][str(level)]:
            ratio = float(line_bet) / self.jpInfo[strInfoKey][str(level)]['max_bet_value']
            return self.jpInfo[strInfoKey][str(level)]['base_value'] * ratio
        return 0

    def _getOddsTopVal(self, strInfoKey, level, line_bet=None):
        if str(level) not in self.jpInfo[strInfoKey]:
            return -1
        if 'top_value' not in self.jpInfo[strInfoKey][str(level)]:
            return -1
        if line_bet is not None and 'max_bet_value' in self.jpInfo[strInfoKey][str(level)]:
            ratio = float(line_bet) / self.jpInfo[strInfoKey][str(level)]['max_bet_value']
            if ratio > 1.0:
                ratio = 1.0
            return self.jpInfo[strInfoKey][str(level)]['top_value'] * ratio
        return self.jpInfo[strInfoKey][str(level)]['top_value']

    def addWinnerList(self, level, award, ark_id, playerData):
        if self._MongoDao is None or ark_id is None:
            return
        if playerData is None:
            fields = {"ArkId": True, "ThirdPartyId": True, "ThirdPartyName": True, "ThirdPartyNick": True,
                      "Currency": True, "MerchantId": True, "LineCode": True}
            playerData = self._MongoDao.getPlayerData(ark_id, fields)
            if playerData is None:
                return
        should_insert_winner, panel_type, jp_type = self.should_insert_winner_and_get_panel_type_and_jp_type(level)
        self.logger.info("should_insert_winner = {}, panel_type = {}, jp_type = {}".format(should_insert_winner, panel_type, jp_type))
        if should_insert_winner:
            # add to winner history
            self._MongoDao.AddWinnerList(
                self.game_id,
                playerData.get("Currency"),
                ark_id,
                playerData.get('ThirdPartyId'),
                playerData.get('ThirdPartyName'),
                jp_type,
                int(level),
                award
            )

    def should_insert_winner_and_get_panel_type_and_jp_type(self, level):
        jp_type = self._get_jp_type_by_jp_level(str(level))
        if jp_type == 'grand':
            return False, 'default', jp_type
        return False, 'default', jp_type


    def _get_jp_type_by_jp_level(self, jp_no):
        '''
        轉換jp_no -> jp_type
        '''
        jp_no_mapping = {
            "0": "grand",
            "1": "major",
            "2": "minor",
            "3": "mini",
            "4": "bonus"
        }
        return jp_no_mapping.get(jp_no, "")

    def _FloorFloat(self, val, FloatPrecision=0, **kwargs):
        return floor_float(val, FloatPrecision)