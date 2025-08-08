# -*- coding: utf-8 -*-

import json
import logging
import sys, os
import six
if six.PY2:
    import ConfigParser
else:
    import configparser as ConfigParser
import time
import traceback
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from ...ArkEngine.ArkSDK.ArkUtilities import http_post
from .. import MathTool as MathTool
from ..RoutineProc import RoutineProc

class MacrossWalletType:
    H5Transfer        = 1
    H5Single          = 2
    APPTransfer       = 3
    APPSingle         = 4
    Multi_Transfer    = 5
    Multi_AppTransfer = 6

    @staticmethod
    def isSingleWallet(type_code):
        return type_code in {2, 4}


class MacrossPlatformAgent(RoutineProc):
    UPDATE_INTERVAL = 60
    def __init__(self, **kwargs):
        self.assetDao = None
        self.config_path = kwargs.get('config_path')
        self.logger = kwargs.get('logger')
        self._ApiDomain = ''
        self._tabApiRoute = {}
        super(MacrossPlatformAgent, self).__init__("MacrossPlatformAgent", MacrossPlatformAgent.UPDATE_INTERVAL, func=self.Reload, logger=self.logger)
        self._fNextReload = 0
        self._ApiSession = self.create_api_session()

        self._GetPlayerDataFunc = kwargs.get("GetPlayerDataFunc")
        self._SetMerchantFunc = kwargs.get("SetMerchantFunc")
        # self._ToCoin = lambda x, *args, **kwargs: x
        # self._ToCredit = lambda x, *args, **kwargs: x
        self._MerchantInfoManager = None

        if "MerchantInfoManager" in kwargs:
            self._MerchantInfoManager = kwargs["MerchantInfoManager"]
            # self._SetMerchantFunc = MerchantInfoManager.SetInfo
            self._SetMerchantBetListFunc = self._MerchantInfoManager.SetBetList
            # self._ToCoin = MerchantInfoManager.ToCoin
            # self._ToCredit = MerchantInfoManager.ToCredit
        if "CurrencyManager" in kwargs:
            CurrencyManager = kwargs["CurrencyManager"]
            self._GetCurrencyId = CurrencyManager.GetCurrencyId
            self._GetCurrencyNameById = CurrencyManager.GetCurrencyNameById

        self.read_config()

    def Reload(self, bForce=True):
        # 既不強制reload,且Reload時間還沒到
        if (not bForce) and (self._fNextReload > 0) and (self._fNextReload > time.time()):
            return
        self._fNextReload = time.time() + 600  # 10分鐘reload
        self._ApiSession = self.create_api_session()

    def create_api_session(self):
        sission = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=200, pool_block=True)
        sission.mount('http://', adapter)
        sission.mount('https://', adapter)
        return sission

    def _ToCoin(self, val, **kwargs):
        if kwargs.get("Ratio") is not None:
            return MathTool.ratio_shift(val, kwargs["Ratio"], precision=4)
        if self._MerchantInfoManager is not None and "MerchantId" in kwargs and "Currency" in kwargs:
            return self._MerchantInfoManager.ToCoin(val, kwargs["MerchantId"], kwargs["Currency"])
        return val

    def _ToCredit(self, val, **kwargs):
        if kwargs.get("Ratio") is not None:
            return MathTool.ratio_shift(val, 1.0/kwargs["Ratio"], precision=4)
        if self._MerchantInfoManager is not None and "MerchantId" in kwargs and "Currency" in kwargs:
            return self._MerchantInfoManager.ToCredit(val, kwargs["MerchantId"], kwargs["Currency"])
        return val


    def set_asset_dao(self, asset_dao):
        self.assetDao = asset_dao

    def verify_member(self, user_token, browser_info, os_info, from_id, extra_data, **kwargs):
        gameId = extra_data.get('game_id', '')
        data = dict({
            'gameId': gameId,
            'token': user_token
        })
        response = self.sendApi('TokenVerifyApi', data)
        if response is None or 'profile' not in response:
            self.logger.warn("[MacrossWrapper] verify, resp:{}".format(response))
            return {"status":-1, "msg": "verify fail"}
        self.logger.info("[MacrossWrapper] verify, resp:{}".format(response))
        profile = response['profile']
        uid = str(profile["aid"])
        siteId = profile["siteId"]
        apiId = profile["apiId"]
        apiName = profile.get("apiName")
        siteName = profile.get("siteName")
        gameSetting = response.get("gameSetting", {})
        betList = gameSetting.get("BetList", [])

        currency = self._GetCurrencyNameById(profile["wallets"][0]["CurrencyNumber"])

        self._SetMerchant(apiId, siteId, profile["wallets"][0], profile["isJPEnabled"], profile["walletType"], apiName, siteName, profile.get('lastCurrencyName'))
        self._SetMerchantBetList(siteId, gameId, betList)

        retdata = {
            "status": 0,
            'uid': uid,
            'trid': profile["siteId"],
            'type': 0,
            'trgameid': gameId,
            'lname': profile["account"],
            'currency': currency,
            'nick': profile["nickname"],
            'single': 1 if MacrossWalletType.isSingleWallet(profile["walletType"]) else 0,
            'linecode': "",
            "balance": profile["wallets"][0]["Coin"],
            "ts": response['response']['time'],
            "lastCurrency": profile.get('lastCurrencyName'),
            'isJPEnabled':profile["isJPEnabled"],
            'ConnectLine': profile.get("ConnectLine"),
            'TableId': profile.get("TableId"),
            'SeatNo': profile.get("SeatNo"),
            'apiId': apiId,
            "Currency": currency,
            "Ratio": profile["wallets"][0]["Ratio"],
            "GameRatio": self._MerchantInfoManager.getRatio(siteId, currency, isShow=True),
            "GameData": response["gameSetting"],
        }
        return retdata

    def _SetMerchant(self, apiId, siteid, currencyInfo, IsJPEnabled, walletType, apiName, siteName, lastCurrency=None):
        if self._SetMerchantFunc is None:
            return
        currency = self._GetCurrencyNameById(currencyInfo["CurrencyNumber"])
        realCurrency = currency if lastCurrency is None else lastCurrency
        wtMap = { 1: "Transfer", 2: "Single" }
        Data = {
            "Ratio": currencyInfo["Ratio"],
            "ShowCurrency": currencyInfo["CurrencyName"],
            "ShowRatio": currencyInfo["Ratio"],
            "JpGroup": "Merchant",
            "TableGroup": "Merchant",
            "ApiId": apiId,
            "ApiName": apiName,
            "MerchantName": siteName,
            "IsJPEnabled": IsJPEnabled,
            "WalletType": wtMap[walletType] if walletType in wtMap else walletType,
            "BuyExchangeRate": currencyInfo["Rate"],
            "SellExchangeRate":currencyInfo["Rate"],
            "LastCurrency": lastCurrency,
        }
        self._SetMerchantFunc(siteid, currency, realCurrency, isUpdate=False, **Data)

    def _SetMerchantBetList(self, siteid, gameId, betList):
        if self._SetMerchantBetListFunc is not None and len(betList) > 0:
            self._SetMerchantBetListFunc(siteid, gameId, betList)

    def get_user_balance(self, wallet_type, third_party_id, merchant_id, login_name, **kwargs):
        response = {'status': 0, 'balance': 0, 'time': 0}
        currency = kwargs.get('currency')
        user_change_coin_record = self.assetDao.GetUserChangeCoinRecord(third_party_id, currency)
        if user_change_coin_record:
            response.update({
                'balance': user_change_coin_record.get('balance'),
                'time': user_change_coin_record.get('time')
            })
        return response

    # 一次處理押注、贏分增減金額並紀錄
    def AddBetWinLogEx(self, logData, ArkId, wagers_id, bet, win, **kwargs):

        user_id = logData["UID"]
        timestamp = logData.get("CreateTimeTS") / 1000
        if timestamp is None:
            timestamp = time.time()

        player_data = self._GetPlayerDataFunc(ArkId)
        if kwargs.get('Reward', False):
            code_name = logData["GameName"]
            str_session = kwargs.get("strSession", "")
            reward_reason = kwargs.get("RewardReason", code_name)
            currency = player_data["ThirdPartyCurrency"]
            resp = self.CreateReward(ArkId, currency, user_id, code_name, str_session, reward_reason, wagers_id, win, timestamp)
            return resp, "", logData, wagers_id

        game_id = logData["GameID"]
        now = datetime.fromtimestamp(timestamp)

        statement = {
            "AccountId": user_id,
            "CurrencyNumber": self._GetCurrencyId(player_data.get('ThirdPartyCurrency', "")),
            "BetAmount": self._ToCoin(abs(bet), Ratio=player_data.get("Ratio"), MerchantId=player_data["MerchantId"], Currency=player_data["ThirdPartyCurrency"]) * -1,
            "WinloseAmount": self._ToCoin(win, Ratio=player_data.get("Ratio"), MerchantId=player_data["MerchantId"], Currency=player_data["ThirdPartyCurrency"])
        }

        record = {
            "WagersTime": now.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        }

        req_data = {
            "gameId": game_id,
            "wagersId": wagers_id,
            "statement": json.dumps([statement]),
            "record": json.dumps(record)
        }

        self.logger.info("[add_bet_win_log]req_data: %s", req_data)
        response = self.sendApi('StatementApi', req_data)
        self.logger.info("[add_bet_win_log]response: %s", response)
        if response is None:
            return {'status': -9999}, "", logData, wagers_id

        status = response.get("Response", {}).get("Error")
        resp_data = response.get("Data")
        if resp_data is None:
            return {'status': status}, "", logData, wagers_id
        if "wallets" in resp_data[0]:
            rbalance = resp_data[0]["wallets"][0]["Coin"]
        else:
            rbalance = resp_data[0]["coin"]
        balance = self._ToCredit(rbalance, Ratio=player_data.get("Ratio"), MerchantId=player_data["MerchantId"], Currency=player_data["ThirdPartyCurrency"]) if resp_data else 0

        resp = dict({
            'status': status,
            'balance': balance,
            'sn': wagers_id
        })

        return resp, "", logData, wagers_id

    def CreateReward(self, arkId, currency, userId, codeName, strSession, rewardReason, wagers_id, win, createTs):

        reqData = {
            "Session": strSession,
            "WagersId": wagers_id,
            "UserId": userId,
            "Reason": rewardReason,
            "CodeName": codeName,
            "CreditType": "Q",
            "CreditAmount": win,
            "CreateTs": createTs
        }

        headers = {'Content-Type': 'application/json'}

        self.logger.info("[CreateReward]req_data: %s", json.dumps(reqData))
        response = self.sendApi('CreateRewardApi', json.dumps(reqData), headers)
        self.logger.info("[CreateReward]response: %s", response)

        status = response.get("Code")
        balance = response.get("CreditBalance")

        if status != 0:
            return {'status': status}

        if status == 0:
            updateTs = response.get("UpdateTs")
            self.assetDao.UpdateUserChangeCoinRecord(arkId, userId, currency, balance, 0, updateTs)

        resp = dict({
            'status': status,
            'balance': balance,
            'sn': wagers_id
        })

        return resp

    def get_api_game_redirect(self, user_id, game_id, lang):
        data = {
            "UserId": user_id,
            "GameId": game_id,
            "Lang": lang,
        }

        self.logger.info("[get_api_game_redirect]data: %s", json.dumps(data))
        response = self.sendApi('GameRedirectApi', data)
        self.logger.info("[get_api_game_redirect]response: %s", response)

        result = dict({
            'status': response.get("Code"),
            'url': response.get("Url", "")
        })

        return result

    def read_config(self, section="MacrossProject"):
        if not os.path.exists(self.config_path):
            raise OSError(self.config_path+" not exist...")
        try:
            config = ConfigParser.RawConfigParser()
            config.read(self.config_path)
            self._ApiDomain = config.get(section, 'MacrossDomain')
            self._ApiRouteConfig = config
            self._ApiRouteConfigSection = section
            self.logger.info(str.format('[MacrossWrapper] self._ApiDomain: {0}', self._ApiDomain))

        except Exception as e:
            self.logger.error("[MacrossWrapper]%s.%s " % (self.__class__.__name__, sys._getframe().f_code.co_name))
            self.logger.error("[MacrossWrapper]read config error \n %s" % traceback.format_exc())

    def sendApi(self, api_name, data, headers=None):
        headers = headers or {'Content-Type': 'application/x-www-form-urlencoded'}
        headers.update({'User-Agent': 'My User Agent 1.0'})
        api_route = self._ApiRouteConfig.get(self._ApiRouteConfigSection, api_name)
        url = '{0}/{1}'.format(self._ApiDomain, api_route)
        start_ts = time.time()
        try:
            response = http_post(url, data, headers, session=self._ApiSession, timeout=15)
        except:
            self.logger.error('[http_post] api:{}, during:{}, req:{}, exc:{}'.format(api_route, time.time()-start_ts, data, traceback.format_exc()))
            return None
        if response is None:
            self.logger.error('[http_post] api:{}, during:{}, req:{}, resp is None'.format(api_route, time.time()-start_ts, data))
            return None
        rdata = json.loads(response)
        self.logger.info('[http_post] api:{}, during:{}, resp:{}, req:{}'.format(api_route, time.time()-start_ts, response, data))
        return rdata