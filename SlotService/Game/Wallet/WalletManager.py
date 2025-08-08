# -*- coding: utf-8 -*-
from builtins import str
from builtins import object
__author__ = "Ellie"

import traceback, sys
import time
import requests
import traceback

class WalletManager(object):
    CREDIT_TYPE = ["Coin"]
    # GET_TIME = lambda: int(time.time() * 1000)
    def __init__(self, Wallet, Logger=None, **kwargs):
        self._Logger = Logger
        self.Wallet = Wallet
        self.GetPlayerDataFunc = kwargs.get('GetPlayerDataFunc')
        self.CommonLogFunc = kwargs.get("CommonLogFunc")
        self.AddSessionLogFunc = kwargs.get('AddSessionLogFunc')
        self._ErrorEventUrl = kwargs.get("ErrorEventUrl")

        # Note:以下只有信用網使用
        self.GtShareRedisDao = kwargs.get("GtShareRedisDao")
        self.GameInfoByName = kwargs.get("GameInfoByName")
        self.IgWalletStatus = kwargs.get('IgWalletStatus')
        self.GtAddCoin = kwargs.get("GtAddCoin")
        self.GtLogDao = kwargs.get("GtLogDao")

        # MongoWallet
        self._SecondWallet = kwargs.get("SecondWallet")
        self._SecondWalletFromType = kwargs.get("SecondWalletFromType", [])

    def IsCredit(self, strType):
        return self.Wallet.IsCredit(strType)

    def GetCredit(self, ark_id, typeList, **kwargs):
        if self._SecondWallet is not None and kwargs["PlayerData"]["FromType"] in self._SecondWalletFromType:
            return self._SecondWallet.GetCredit(ark_id, typeList, **kwargs)
        return self.Wallet.GetCredit(ark_id, typeList, **kwargs)

    def AddCredit(self, ark_id, strType, nAmount, **kwargs):
        raise NotImplementedError()

    def SubCredit(self, ark_id, strType, nAmount, **kwargs):
        raise NotImplementedError()

    def AddCreditMulti(self, ark_id, AddDict, **kwargs):
        SubDict = {WalletManager.CREDIT_TYPE[0]: 0}
        return self.Transaction(ark_id, SubDict, AddDict, **kwargs)

    def SubCreditMulti(self, ark_id, SubDict, **kwargs):
        AddDict = {WalletManager.CREDIT_TYPE[0]: 0}
        return self.Transaction(ark_id, SubDict, AddDict, **kwargs)

    def Transaction(self, ark_id, SubDict, AddDict, **kwargs):
        # For igLobby流程
        # start = WalletManager.GET_TIME()
        wagers_id = kwargs.get("WagersId")
        if self.GtShareRedisDao is not None:
            code, walletRet, sn = self._AddIgWallet(ark_id, SubDict, AddDict, **kwargs)
        elif self._SecondWallet is not None and kwargs["PlayerData"]["FromType"] in self._SecondWalletFromType:
            walletRet = self._SecondWallet.Transaction(ark_id, SubDict, AddDict, **kwargs)
            code = walletRet.get("Code") if walletRet is not None else -999
        else:
            walletRet = self.Wallet.Transaction(ark_id, SubDict, AddDict, **kwargs)
            # self._Logger.info("[Transaction1]{}".format(WalletManager.GET_TIME() - start))
            # start = WalletManager.GET_TIME()
            code = walletRet.get("Code") if walletRet is not None else -999
        if code != 0:
            self._SendBetFailEvent(ark_id, kwargs.get("PlayerData"), SubDict[WalletManager.CREDIT_TYPE[0]], wagers_id, code)
            return code, walletRet, wagers_id  # Todo 撞到errorcode
        DetailLog = kwargs.get("DetailLog")   # Note: 主要是Gt要回寫 GameSerialID
        if DetailLog is not None:
            wagers_id = wagers_id or DetailLog.get("WagersId")
            if self.AddSessionLogFunc is not None:
                self.AddSessionLogFunc(DetailLog)
        # self._Logger.info("[Transaction2]{}".format(WalletManager.GET_TIME() - start))
        return 0, walletRet, wagers_id

    def _AddIgWallet(self, ark_id, SubDict, AddDict, **kwargs):
        DetailLog = kwargs.get("DetailLog")
        wagers_id = kwargs.get("WagersId")
        bet, win = SubDict[WalletManager.CREDIT_TYPE[0]], AddDict[WalletManager.CREDIT_TYPE[0]]

        # For igLobby加扣款流程
        if self.GtShareRedisDao is None:
            self._Logger.error("[{}.{}] GtShareRedisDao is None".format(self.__class__.__name__, sys._getframe().f_code.co_name))
            return None, None, None
        PlayerData = kwargs.get("PlayerData")
        UserName, Logo = self._GetPlayerUserNameLogo(ark_id, PlayerData)
        isLocked = self.GtShareRedisDao.LockWallet(Logo, UserName)
        if not isLocked:
            self._Logger.warning("[{}.{}] Logo:{} {}'s Wallet Locked!".format(self.__class__.__name__, sys._getframe().f_code.co_name, Logo, UserName))
            return None, None, None
        ws = self.GtShareRedisDao.GetWalletStatus(Logo, UserName)
        # 處理 GtApi加款 (目前只有GameType為Lobby)

        if ws != self.IgWalletStatus.API:
            if isLocked:
                self.GtShareRedisDao.UnlockWallet(Logo, UserName)
            if self.GtAddCoin is None:
                self._Logger.warning("[{}.{}] Logo:{} {}'s GtAddCoin is None!".format(self.__class__.__name__, sys._getframe().f_code.co_name, Logo, UserName))
                return -21, None, None
            if ws != self.IgWalletStatus.GT:
                self._Logger.warning("[{}.{}] Logo:{} {}'s Wallet is in {}!".format(self.__class__.__name__, sys._getframe().f_code.co_name, Logo, UserName, ws ))
                return -22, None, None
            if (DetailLog is None) and ('GameName' not in DetailLog):
                return -23, None, None
            gameName = DetailLog['GameName']
            GameType = self.GameInfoByName(gameName)["Type"]
            if GameType != "Lobby":
                self._Logger.warning("[{}.{}] Logo:{} {}'s GameType is in {}!".format(self.__class__.__name__, sys._getframe().f_code.co_name, Logo, UserName, GameType))
                return -24, None, None
            # 走GtApi加款路線，不需要Lock
            res, resInfo = self.GtAddCoin(ark_id, gameName, 0, AddDict[WalletManager.CREDIT_TYPE[0]], Extra=DetailLog, strUserName=UserName, strLogo=Logo)
            return -20000+resInfo.get("Code") if not res else resInfo.get("Code", 0), None, None

        # 處理Api加扣款
        walletRet = self.Wallet.Transaction(ark_id, SubDict, AddDict, **kwargs)
        if isLocked:
            self.GtShareRedisDao.UnlockWallet(Logo, UserName)
        if (walletRet is None) or (len(walletRet)<= 0) or (WalletManager.CREDIT_TYPE[0] not in walletRet):
            self._Logger.warning("[{}.{}] Logo:{} {}'s Wallet.Transaction Fail! walletRet is {}".format(self.__class__.__name__, sys._getframe().f_code.co_name, Logo, UserName, walletRet))
            code = walletRet.get("Code", -999) if walletRet is not None else -999
            return code, None, None
        balance = walletRet[WalletManager.CREDIT_TYPE[0]]

        # 歷程送至Gt
        if self.GtLogDao is not None:
            DetailLog['ValueAfter'] = balance
            DetailLog['ValueBefore'] = balance if balance is None else balance - win + bet
            GtSn = self.GtLogDao.AddGtLog(DetailLog.get('Logo'), DetailLog)
            if GtSn is None:
                self._Logger.error("[{}.{}] {} Write GtLog Failed!".format(self.__class__.__name__, sys._getframe().f_code.co_name, wagers_id))
                self.CommonLogFunc(DetailLog, 'GtAddLogFail', bSendSplunk=True)
                return -25, None, None
            DetailLog['GameSerialID'] = GtSn
        return 0, walletRet, DetailLog['WagersId']

    def _GetPlayerUserNameLogo(self, ark_id, playerData=None):
        if (playerData is None) and (self.GetPlayerDataFunc is not None):
            playerData = self.GetPlayerDataFunc(ark_id)
        if playerData is None:
            self._Logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return None, None
        return playerData.get("UserName"), playerData.get("Logo")

    def _SendBetFailEvent(self, ark_id, PlayerData, Amount, WagersId, ErrorCode):
        if self._ErrorEventUrl is None:
            return
        sendData = {
            "UserId": PlayerData["ThirdPartyId"],
            "MerchantId": PlayerData["MerchantId"],
            "Currency": PlayerData["ThirdPartyCurrency"],
            "LoginName": PlayerData["ThirdPartyName"],
            "WalletType": PlayerData["WalletType"],
            "FromType": PlayerData["FromType"],
            "Amount": Amount,
            "WagersId": WagersId,
            "ErrorCode": ErrorCode,
            "Ts": int(time.time())
        }
        try:
            resp = requests.post(self._ErrorEventUrl+"/GameBetFail", json=sendData)
        except:
            self._Logger.error("SendErrorEvent Fail, Exception:{}".format(traceback.format_exc()))
            return
        if resp.status_code != 200:
            self._Logger.error("SendErrorEvent Fail, Status Code:{}".format(resp.status_code))
        try:
            respJson = resp.json()
        except:
            self._Logger.error("SendErrorEvent Fail, Response:{}".format(resp.text))
            return
        code = respJson.get("Code")
        if code != 0:
            self._Logger.error("SendErrorEvent Fail, Error Code:{}".format(code))
