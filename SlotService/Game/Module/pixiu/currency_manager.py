# -*- coding: utf-8 -*-
__author__ = "eitherwang"

"""
匯率換算器
主要功能：
_load_exchange_info(): 從API server獲取匯率資訊，只在service啟動時取一次
to_central(): 從玩家幣種轉為中心幣種，如：灌JP
to_local(): 從中心幣種轉為玩家幣種，如：顯示JP數值、拉JP

本程式引入即匯站RTER API即時取得全球匯率，自動檢查和修正匯率，以防市場波動大時導致虧損
"""

import requests
import traceback
from ..MathTool import *
from ..RoutineProc import RoutineProc

# buy=player paid, sell=player get



class CurrencyManager():
    RELOAD_TIME = 300
    AUTO_CHECK_EXCHANGE_RATE = False
    CENTRAL_CURRENCY = "CNY"
    SCORE_CURRENCY = "Q"

    def __init__(self, Logger=None, DataSource=None):
        self.exchange_info = dict()
        self._strCentralCurrency = self.CENTRAL_CURRENCY
        self._tabInfo = dict()              # (currency, RealCurrency): dict
        self.logger = Logger
        self.Dao = None
        if DataSource is not None:
            self.Dao = CurrencyDao(self.logger, DataSource)
            self.loadSetting()
            self._reload = RoutineProc("{}_Reload".format(self.__class__.__name__), self.RELOAD_TIME, self.loadSetting, logger=self.logger)


    def on_module_init(self, module_manager):
        self.module_manager = module_manager
        self.logger = module_manager.get_class_instance("log_manager").get_logger()
        self.Dao = CurrencyDao(self.logger, module_manager.get_class_instance("mongo_manager").get_wrap_database("MainGame"))

        self.loadSetting()

        timer_service = module_manager.get_class_instance('timer_service')
        if timer_service is not None:
            timer_service.register("{}_Reload".format(self.__class__.__name__), self.RELOAD_TIME, self.loadSetting)

    def loadSetting(self):
        self._tabInfo = self.Dao.loadInfoEx()
        self.ShowCurrencyList = self.Dao.loadShowCurrency()

    def getShowCurrency(self, env=None):
        return self.ShowCurrencyList

    def getAllCurrencyInfo(self):
        return self._tabInfo

    def getRealCurrency(self, env=None):
        ret = []
        for k in self._tabInfo.keys():
            if type(k) != tuple:
                continue
            ret.append(k[1])
        return ret

    def getCurrencyInfo(self, currency, RealCurrency):
        if currency == RealCurrency or RealCurrency is None:
            return self._tabInfo.get((currency, currency))
        elif currency is None:
            return self._tabInfo.get((RealCurrency, RealCurrency), self._tabInfo.get((self.SCORE_CURRENCY,RealCurrency)))
        return self._tabInfo.get((currency, RealCurrency))

    def to_central(self, currency, value, precision=-1, RealCurrency=None):
        info = self.getCurrencyInfo(currency, RealCurrency)
        return self.get_floor_float_by_currency(currency, value * info["BuyExchangeRate"], precision, RealCurrency)

    def to_local(self, currency, value, precision=-1, RealCurrency=None):
        info = self.getCurrencyInfo(currency, RealCurrency)
        rate = info["SellExchangeRate"]
        if rate <= 0:
            return 0
        return self.get_floor_float_by_currency(currency, value / rate, precision, RealCurrency)

    def get_floor_float_by_currency(self, currency, value, precision=-1, RealCurrency=None):

        if precision < 0:
            precision = self.getCurrencyInfo(currency, RealCurrency).get("FloatPrecision", 2)
        if precision == 0:
            return int(value)
        return floor_float(value, precision)

    def get_round_by_currency(self, currency, value, precision=-1, RealCurrency=None):
        if precision < 0:
            precision = self.getCurrencyInfo(currency, RealCurrency).get("FloatPrecision", 2)
        if precision == 0:
            return int(value)
        return round(value, precision)


    def get_ceil_float_by_currency(self, currency, value, precision=-1, RealCurrency=None):
        if precision < 0:
            precision = self.getCurrencyInfo(currency, RealCurrency).get("FloatPrecision", 2)
        if precision == 0:
            return int(value)
        return ceil_float(value, precision)

    @property
    def central_currency(self):
        return self.CENTRAL_CURRENCY

    def getSupportedBaseCurrency(self):
        return {key[0] for key in self._tabInfo.keys()}

    def getSupportedRealCurrency(self):
        return {key[1] for key in self._tabInfo.keys()}

    # deprecated: use 2 above method instead
    def get_supported_currency(self):
        return {key[1] for key in self._tabInfo.keys()}

    def _load_exchange_info(self, api_server):
        new_exchange_info = dict()
        resp = api_server.get_exchange_rate_info()
        if resp.get("status") != 0:
            self.logger.error("[CurrencyManager] _load_exchange_info, get exchange rate from API server failed!")
            new_exchange_info = self.Dao.loadExchangeInfo()

        else:
            new_exchange_info = resp["rate"]
        # if new_exchange_info != self.exchange_info:
        if self.AUTO_CHECK_EXCHANGE_RATE:
            self._check_exchange_rate(new_exchange_info)
        else:
            self._check_exchange_rate_without_rter_api(new_exchange_info)
        self.exchange_info = new_exchange_info
        # self.logger.info("[CurrencyManager] _load_exchange_info, exchange_info={}".format(self.exchange_info))

    def _check_exchange_rate(self, exchange_info):
        try:
            # 從即匯站API(https://tw.rter.info/capi.php)取得全球即時匯率
            r = requests.get('https://tw.rter.info/capi.php')
            data = r.json()
            for currency in self.get_supported_currency():
                if currency not in exchange_info:
                    self.logger.error("[CurrencyManager] _check_currency_info, currency:{} not in exchange info={}".format(currency, exchange_info))
                    continue
                if "USD"+currency not in data:
                    self.logger.error("[CurrencyManager] _check_currency_info, currency:{} not in RTER API".format(currency))
                    continue
                real_exchange_rate = data["USD"+self.central_currency]["Exrate"] / data["USD"+currency]["Exrate"]
                if exchange_info[currency]["SellExchangeRate"] < real_exchange_rate:
                    self.logger.error("[CurrencyManager] _check_currency_info, currency:{}, RTER API sell rate={}, our sell rate={}".format(
                            currency, real_exchange_rate, exchange_info[currency]["SellExchangeRate"]))
                    # exchange_info[currency]["SellExchangeRate"] = real_exchange_rate * 0.9
                else:
                    self.logger.debug("[CurrencyManager] _check_currency_info, currency:{}, RTER API sell rate={}, our sell rate={}".format(
                            currency, real_exchange_rate, exchange_info[currency]["SellExchangeRate"]))
                if exchange_info[currency]["BuyExchangeRate"] > real_exchange_rate:
                    self.logger.error("[CurrencyManager] _check_currency_info, currency:{}, RTER API buy rate={}, our buy rate={}".format(
                        currency, 1/real_exchange_rate, exchange_info[currency]["BuyExchangeRate"]))
                    # exchange_info[currency]["BuyExchangeRate"] = real_exchange_rate * 0.9
                else:
                    self.logger.debug("[CurrencyManager] _check_currency_info, currency:{}, RTER API buy rate={}, our buy rate={}".format(
                        currency, 1/real_exchange_rate, exchange_info[currency]["BuyExchangeRate"]))
        except:
            self.logger.error("[CurrencyManager] _check_currency_info with RETR API failed, Exception:{}".format(traceback.format_exc()))
            self._check_exchange_rate_without_rter_api(exchange_info)

    def _check_exchange_rate_without_rter_api(self, exchange_info):
        try:
            for currency in self.get_supported_currency():
                if currency not in exchange_info:
                    self.logger.error("[CurrencyManager] _check_currency_info. currency:{} not in exchange info={}".format(currency, exchange_info))
                    continue
                if exchange_info[currency]["SellExchangeRate"] <= 0 or exchange_info[currency]["BuyExchangeRate"] <= 0:
                    self.logger.warning(
                        "[CurrencyManager] _check_currency_info, currency:{}, sell:{}, buy:{}".format(
                            currency, exchange_info[currency]["SellExchangeRate"],
                            exchange_info[currency]["BuyExchangeRate"],))
                    continue

                if not 0.7 <= exchange_info[currency]["BuyExchangeRate"] / exchange_info[currency]["SellExchangeRate"]<= 1:
                    self.logger.warning("[CurrencyManager] _check_currency_info, currency:{}, sell:{}, buy:{}, buy/sell={}".format(
                        currency, exchange_info[currency]["SellExchangeRate"], exchange_info[currency]["BuyExchangeRate"], exchange_info[currency]["BuyExchangeRate"] / exchange_info[currency]["SellExchangeRate"]))
        except:
            self.logger.error("[CurrencyManager] _check_currency_info without RETR API failed, Exception:{}".format(traceback.format_exc()))

    # Macross
    def GetCurrencyId(self, CurrencyName):
        info = self.getCurrencyInfo(CurrencyName, CurrencyName)
        return info.get("CurrencyId", 0)

    # Macross
    def GetCurrencyNameById(self, CurrencyId):
        CurrencyId = int(CurrencyId)
        for _currency, RealCurrency in self._tabInfo:
            if CurrencyId == self._tabInfo[(_currency, RealCurrency)].get("CurrencyId"):
                return RealCurrency
        return None

class CurrencyDao:
    def __init__(self, logger=None, DataSource=None):
        self.logger = logger
        self._DataSource = DataSource

    def loadExchangeInfo(self):
        new_exchange_info = {}
        cursor = None
        try:
            cursor = self._DataSource["CurrencyExchangeInfo"].find()
        except:
            self.logger.error("[CurrencyDao] loadCurrencyExchangeInfo, e={}".format(traceback.format_exc()))
        if cursor is None:
            self.logger.error("[CurrencyManager] _load_exchange_info, load failed!")
            raise Exception("CurrencyManager _load_exchange_info failed")
        for doc in cursor:
            currency_id = doc["currency_id"]
            del doc["_id"]
            new_exchange_info.update({currency_id: doc})
        return new_exchange_info

    def loadInfoEx(self):
        new_info = {}
        cursor = None
        try:
            cursor = self._DataSource["CurrencyInfoEx"].find()
        except:
            self.logger.error("[CurrencyDao] loadCurrencyInfoEx, e={}".format(traceback.format_exc()))
        if cursor is None:
            self.logger.error("[CurrencyManager] _load_exchange_info, load failed!")
            raise Exception("CurrencyManager _load_exchange_info failed")
        for doc in cursor:
            key = (doc["Currency"], doc["RealCurrency"])
            del doc["_id"]
            new_info.update({key: doc})
        return new_info

    def loadShowCurrency(self):
        r = None
        try:
            r = self._DataSource["CurrencyInfoEx"].distinct("Currency")
        except:
            self.logger.error("[CurrencyDao] loadShowCurrency, e={}".format(traceback.format_exc()))
        return r


if __name__ == "__main__":

    def check_exchange_rate_from_rter(cm):
        r = requests.get('https://tw.rter.info/capi.php')
        data = r.json()
        for currency in cm.get_supported_currency():
            real_exchange_rate = data["USD" + currency]["Exrate"] / data["USD" + cm.central_currency]["Exrate"]
            print(currency, real_exchange_rate)

    def test_loop_exchange(cm, my_currency, init_balance):
        balance = init_balance
        bank = 0
        for i in xrange(10):
            bank = cm.to_central(my_currency, balance)
            balance = 0
            print ("Bank={}, Balance={}".format(bank, balance))

            balance = cm.to_local(my_currency, bank)
            bank = 0

            print ("Bank={}, Balance={}".format(bank, balance))

    EXCHANGE_INFO = {
            "CNY": {"BuyExchangeRate": 0.99,   "SellExchangeRate": 1},
            "MYR": {"BuyExchangeRate": 1.93,   "SellExchangeRate": 0.50},
            "THB": {"BuyExchangeRate": 0.22,   "SellExchangeRate": 4.1},
            "KVND": {"BuyExchangeRate": 0.0003, "SellExchangeRate": 3312.56153},
            "USD": {"BuyExchangeRate": 7,      "SellExchangeRate": 0.142724612859},
    }
    cm = CurrencyManager()
    cm.exchange_info = EXCHANGE_INFO

    my_currency = "CNY"

    test_loop_exchange(cm, my_currency, init_balance=10000)
    # check_exchange_rate_from_rter(cm)