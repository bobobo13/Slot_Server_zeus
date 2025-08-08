# -*- coding: utf-8 -*-
__author__ = "eitherwang"

import time
from ...Wallet.iWallet import iWallet
from ...Wallet.WalletLock import WalletLock

class ApiWallet(iWallet):
    CREDIT_TYPE = "Coin"
    TIMESTAMP = "ts"

    def __init__(self, Logger, DataSource, **kwargs):
        self._Logger = Logger
        self.CommonLog = kwargs.get("CommonLog")
        self.ApiServer = kwargs["ApiServer"]
        self.GetPlayerDataFunc = kwargs.get("GetPlayerData")
        self.MerchantMgr = kwargs.get("MerchantMgr")
        self.ResendFunc = kwargs.get("ResendFunc")
        self.KickPlayer = kwargs.get("KickPlayer")
        self._bIsTestEnv = kwargs.get("IsTestEnv")

        WalletLock.initCls(Logger, **kwargs)

    def Update(self, passTime):
        pass

    def IsCredit(self, strType):
        return (strType == self.CREDIT_TYPE)

    def GetCredit(self, strArkId, typeList, **kwargs):
        playerData = kwargs.get("PlayerData")
        if playerData is None:
            playerData = self._GetPlayerData(strArkId)
        if playerData is None:
            return None
        if playerData["FromType"] == "webgl" and self._bIsTestEnv:
            return {self.CREDIT_TYPE: 100000, self.TIMESTAMP: time.time()}
        ret = self.ApiServer.get_user_balance(playerData['WalletType'], playerData['ThirdPartyId'], playerData['MerchantId'], playerData['ThirdPartyName'], from_type=playerData['FromType'], currency=playerData['ThirdPartyCurrency'])
        if ret is None:
            return None
        if 'balance' not in ret:
            return None
        balance = ret['balance']
        ts = ret.get('time')
        if self.MerchantMgr is not None:
            self.MerchantMgr.getFloorFloat(balance, playerData["ThirdPartyCurrency"], playerData['MerchantId'])
        return {self.CREDIT_TYPE: balance, self.TIMESTAMP: ts}


    def Transaction(self, ark_id, SubDict, AddDict, **kwargs):
        PlayerData = kwargs.get("PlayerData")
        DetailLog = kwargs.get("DetailLog")
        wagers_id = kwargs.get("WagersId")
        bCheckSameProcId = not kwargs.get("Reward", False)  # hardcode: Reward is not check procId
        CreditType = ApiWallet.CREDIT_TYPE
        bet = SubDict[CreditType]
        win = AddDict[CreditType]
        resp = dict({'status': 9999})

        if PlayerData is None:
            PlayerData = self._GetPlayerData(ark_id)
        if PlayerData is None:
            return None
        if PlayerData["FromType"] == "webgl":
            if self._bIsTestEnv:
                return {"Code": 0, self.CREDIT_TYPE: 100000 + win - bet, self.TIMESTAMP: time.time()}
            raise Exception("Not Test Env")
        # creditLimit = AddDict[CreditType]["Limit"]      # unused
        with WalletLock(ark_id, PlayerData=PlayerData, bCheckSameProcId=bCheckSameProcId) as success:
            if success or bet == 0:
                resp, cmd, DetailLog, wagers_id = self.ApiServer.AddBetWinLogEx(DetailLog, ark_id, wagers_id, bet, win, sWalletType=PlayerData.get("WalletType"), **kwargs)
            elif self.KickPlayer is not None:
                self.KickPlayer(ark_id)

        status = resp.get('status', 9999)
        if resp['status'] != 0:
            DetailLog["status"] = resp['status']
            if self.CommonLog is not None:
                self.CommonLog(DetailLog, 'ApiAddBetWinFail', bSendSplunk=True)
            if self.ResendFunc is not None:
                self.ResendFunc(status, cmd, DetailLog)
            return {"Code": status}
        balance = resp['balance']
        if self.MerchantMgr is not None:
            self.MerchantMgr.getFloorFloat(balance, PlayerData["ThirdPartyCurrency"], PlayerData['MerchantId'])
        DetailLog['ValueAfter'] = balance
        DetailLog['ValueBefore'] = balance if balance is None else balance - win + bet
        retCredit = {
            "Code": 0,
            CreditType: balance
        }
        return retCredit

    def AddCreditMulti(self, ark_id, AddDict, **kwargs):
        SubDict = {ApiWallet.CREDIT_TYPE:0}
        return self.Transaction(ark_id, SubDict, AddDict, **kwargs)

    def SubCreditMulti(self, ark_id, SubDict, **kwargs):
        AddDict = {ApiWallet.CREDIT_TYPE: 0}
        return self.Transaction(ark_id, SubDict, AddDict, **kwargs)

    def AcquireLock(self, ark_id, PlayerData=None, ttl=None):
        return WalletLock(ark_id, PlayerData).Acquire(ttl)

    def ReleaseLock(self, ark_id, PlayerData=None):
        return WalletLock(ark_id, PlayerData).Release()

    def CheckLock(self, ark_id, PlayerData=None):
        return WalletLock(ark_id, PlayerData).IsLocked()

    def RefreshLock(self, ark_id, PlayerData=None, ttl=None):
        return WalletLock(ark_id, PlayerData).Refresh(ttl)

    def _GetPlayerData(self, strArkId):
        if self.GetPlayerDataFunc is None:
            return None
        return self.GetPlayerDataFunc(strArkId)

    def KickPlayerCheckLock(self, ark_id):
        return {"status": -1} if self.CheckLock(ark_id) else {"status": 0}
