# -*- coding: utf-8 -*-
__author__ = 'eitherwang'
from system.common.game_status_code import *

from InGameJpMgr import InGameJpMgr
from IngameJpDao import IngameJpRedisDao, IngameJpMongoDao
import traceback
from system.lib.env import Env
from system.common.game_info import *
from system.common.math_tool import *

class InGameJackpotManager(InGameJpMgr):
    CLINET_UPDATE_INTERVAL = 7  # 7秒Client上來要一次
    UPDATE_INTERVAL = 3  # 3秒跟redis同步一次
    SPLUNK_UPDATE_INTERVAL = 30 * 60  # 30分送splunk一次
    JP_MAX_WINNER_LIST_NUM = 11

    def __init__(self, **kwargs):
        tabGameSetting = kwargs.pop("GameSetting", None)
        if kwargs.get("module_manager") is not None:
            module_manager = kwargs.get("module_manager")
            self.env_obj = module_manager.get_class_instance("env")
            self.env = self.env_obj.get_env()
            self.user_manager = module_manager.get_class_instance('user_manager')
            self.splunk_sender = module_manager.get_class_instance('splunk_sender')
            self.MerchantInfoMgr = module_manager.get_class_instance('MerchantInfoManager')
            self.getMerchantInfo = self.MerchantInfoMgr.getMerchantInfo
            self.getAllMerchantInfo = self.MerchantInfoMgr.getAllMerchantInfo
            kwargs["DataSource"] = module_manager.get_class_instance('mongo_manager').get_wrap_database("InGameJackpot")
            kwargs["PoolDao"] = IngameJpRedisDao(logger=kwargs.get('logger'), DataSource=module_manager.get_class_instance('redis_manager').get_redis_client("Jackpot"))

        super(InGameJackpotManager, self).__init__(**kwargs)
        self.bet_times = 0

        self._calculators = dict()
        self.slot_system = kwargs.get("SlotSystem")
        if tabGameSetting is not None:
            self.load_jp_module(tabGameSetting)


    def _parseGroup(self, Group):
        currency = Group["Currency"]
        merchantId = Group["MerchantId"]
        lineCode = Group.get("LineCode", "")
        merchantInfo = self.getMerchantInfo(currency, merchantId)
        realCurrency = merchantInfo.get("RealCurrency", currency)
        jpGroupMode = merchantInfo.get('JpGroup')
        _tab = {
            'Global': realCurrency,
            'Currency': realCurrency,
            'Merchant': '_'.join([realCurrency, str(merchantId)]),
            'LineCode': '_'.join([realCurrency, str(merchantId), str(lineCode)]),
        }
        poolGroup = _tab.get(jpGroupMode, jpGroupMode)

        return currency, poolGroup


    def is_on_jackpot_server(self):
        if self.env in [Env.ENV_DEV, Env.ENV_TEST, Env.ENV_STAGE, Env.ENV_RELEASE]:
            if self.env_obj.get_admin_enable('jackpot_server'):
                return True
            else:
                return False
        return True

    def req_get_in_game_jp_status(self, user_id, game_id, Channel):
        if not self.is_ingame_jp_game(game_id):
            return
        currency = Channel["Currency"]
        merchantId = Channel["MerchantId"]
        floatPrecision = self.MerchantInfoMgr.getFloatPrecision(currency, merchantId)

        data_dic = self.GetJpStatus(game_id, Channel)
        for key in data_dic:
            if isinstance(data_dic[key], float):
                data_dic[key] = floor_float(data_dic[key], floatPrecision)
        data_dic['sent_time_gap'] = InGameJackpotManager.CLINET_UPDATE_INTERVAL

        return data_dic

    def on_command_get_in_game_jp_winner(self, ark_id, cmd_data):
        ##待修改##
        # 修改成取得各遊戲的jp winner
        '''
        if 'request' not in cmd_data or 'data' not in cmd_data['request'] or 'jp_type' not in cmd_data['request']['data'] or 'num' not in cmd_data['request']['data']:
            self.logger.error("[Jackpot] on_command_get_jp_winner - Command data format error. {}".format(cmd_data))
            return STATUS_CODE_ERR_DATA_FIELD, None
        '''

        ##jp_type = cmd_data['request']['data']['jp_type']
        ##num = cmd_data['request']['data']['num']
        num = self.JP_MAX_WINNER_LIST_NUM
        game_id = str(cmd_data['game_id'])
        result = list()
        try:
            result = self.jackpot_winner[:num]
            #result = self.req_get_jp_winner(num)
        except:
            self.logger.error('[jackpot.py][on_command_get_jp_winner] unexpect error occur')
            self.logger.error(traceback.format_exc())
            return STATUS_CODE_ERR_EXCEPTION, None

        return STATUS_CODE_OK, result


    def __sync_jackpot_value(self):
        if self.is_on_jackpot_server():
            self.__sync_jackpot_value_to_mongo()
            self.__sync_jackpot_value_to_splunk()
        # self.db_get_winner_list(JP_MAX_WINNER_LIST_NUM)

    def __sync_jackpot_value_to_mongo(self):
        merchant_dict = {}
        for merchant_key, merchant_setting in self.getAllMerchantInfo.iteritems():
            currency, merchant_id = merchant_key.split("_")
            if currency not in merchant_dict:
                merchant_dict[currency] = list()
            merchant_dict[currency].append((merchant_id, merchant_setting))

        for kiosk_key in self._calculators:
            game_id, currency = kiosk_key.split("_")
            if currency not in merchant_dict:
                continue
            for merchant_id, merchant_setting in merchant_dict[currency]:
                game_info, _ = self.slot_system.get_game_info(game_id, currency, merchant_id, merchant_setting)
                max_bet = game_info["bet_info_list"][-1]["line_bet"]
                jp_max_value = self._calculators[kiosk_key].get_jp_max_value(max_bet)
                self.col_jackpot_data.find_and_modify(
                    {
                        'currency': currency,
                        'merchant_id': int(merchant_id),
                        'jp_id': game_id,
                        'jp_type': 'ingame'
                     },
                    {'$set': {'jp_value': jp_max_value,
                              'games': [GAME_ID_TO_API_GAME_ID_MAP[game_id]]}
                     },
                    upsert=True)

    def __sync_jackpot_value_to_splunk(self):
        if not self.is_on_jackpot_server():
            return
        if self.env not in [Env.ENV_TEST, Env.ENV_RELEASE]:
            return

        for kiosk_key in self._calculators:
            data = dict()
            # jp_pool = self._calculators[game_id].db_get_in_game_jp_pool()
            self._calculators[kiosk_key].update_pool_info()
            game_id, currency = kiosk_key.split("_")
            for level in self._calculators[kiosk_key].pool_info:
                jp_type = self._calculators[kiosk_key]._get_jp_type_by_jp_level(str(level))
                face_jp_pool = self._calculators[kiosk_key].pool_info[level]["pool"] + self._calculators[kiosk_key]._get_bet_related_baby_fund(level)
                real_jp_pool = self._calculators[kiosk_key].pool_info[level]["pool"] - self._calculators[kiosk_key]._get_fixed_baby_fund(level)
                baby_fund_pool = self._calculators[kiosk_key].db_get_baby_fund_pool(level)
                data[jp_type+"Value"] = floor_float(face_jp_pool,4)
                data[jp_type+"RealValue"] = floor_float(real_jp_pool,4)
                data[jp_type+"Init"] = floor_float(baby_fund_pool,4)
            self.splunk_sender.send_jp_pool_value('ingame', game_id, currency, data)