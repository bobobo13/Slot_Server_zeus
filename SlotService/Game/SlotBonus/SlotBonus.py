# -*- coding: utf-8 -*-
__author__ = 'JhengSianChen'
import time
import gevent
import datetime
from .SlotBonusDao import SlotBonusDao
from ..Module.RoutineProc import RoutineProc

class SlotBonus(RoutineProc):
    CONFIG = "game.cfg"
    UPDATE_INTERVAL = 60
    def __init__(self, logger, DataSource=None, **kwargs):
        self.name = kwargs.get('Name', "SlotBonus")
        self.name_slot_config = "{}_{}".format(self.name, "SlotConfig")
        self.logger = logger
        # configuration for database
        self.SBDao = SlotBonusDao(DataSource, self.logger, ConfigFile=kwargs['ConfigPath'] + SlotBonus.CONFIG, **kwargs)  # Init Database

        super(SlotBonus, self).__init__(self.name, SlotBonus.UPDATE_INTERVAL, func=self.Reload)
        self._Enable = True
        self.GetPlayerDataFunc = kwargs.get('GetPlayerDataFunc')
        self.get_info_func = kwargs.get('GetInfoFunc')
        info_register_func = kwargs.get('InfoRegisterFunc')
        if info_register_func is not None:
            info_register_func(self.name, self.SBDao.load_info)
            info_register_func(self.name_slot_config, self.SBDao.load_slot_config)

        self._tab_setting, self._tab_info, self._tab_model, self._tab_game_info = None, None, None, None

        self._fNextReload = 0
        while (self._tab_setting is None) or (self._tab_info is None) or (self._tab_model is None):
            self.Reload()
            for i in [self._tab_setting, self._tab_info, self._tab_model]:
                if i is None:
                    self.logger.error('[{}] init fail.'.format(self.name))
                    break
            gevent.sleep(1)
        self.logger.info('[{}] init ok.'.format(self.name))

    def Reload(self, bForce=True):
        if (not bForce) and (self._fNextReload > 0) and (self._fNextReload > time.time()):
            return
        self._fNextReload = time.time() + 180
        self._tab_setting = self.SBDao.load_setting()
        self._tab_info = self.SBDao.load_info()
        self._tab_model = self.SBDao.load_model()
        # self._tab_game_info = self.SBDao.load_game_info()

    def is_enable(self):
        return self._Enable

    def _basic_check(self, ark_id):
        if not self.is_enable():
            return -1, None  # Code:-1 功能未開啟
        if not isinstance(ark_id, str):
            return -2, None  # Code:-2 ark_id 錯誤
        info = self.get_info_func(ark_id, self.name)
        if info is None or not info.get('Enable', False):
            return -3, None  # Code:-3 無支援的info
        return 0, info

    def check_game_valid(self, ark_id, game_name, special_game, name, extra_bet):
        code, info = self._basic_check(ark_id)
        if code < 0:
            # return self._Result(code)
            return False
        if game_name not in self._tab_model:
            # return self._Result(-999)
            return False
        for model in self._tab_model[game_name]:
            if model['SpecialGame'] != special_game:
                continue
            if name != model['Name'] or name not in info['Name']:
                continue
            if not model['BetMode'].get('ExtraBet' if extra_bet else 'Bet', False):
                continue
            if not self.check_activity_time(model):
                continue
            return model['Name'] in info['Name']
        return False

    RATE_UNIT = 10000
    def get_dec_game_rate(self, ark_id, game_name, player_data):
        player_data = dict(self.GetPlayerDataFunc(ark_id), GameName=game_name)
        slot_config = self.get_info_func(None, self.name_slot_config, userData=player_data)
        return slot_config.get('DecGameRate', 0) * self.RATE_UNIT

    def get_model_info(self, game_name, name):
        for model in self._tab_model[game_name]:
            if model['Name'] == name:
                return model
        return None

    def check_activity_time(self, model):
        now = datetime.datetime.now()
        # 檢查活動時間
        if model is not None:
            if ('StartTime' in model) and (now < model['StartTime']):
                return False
            if ('EndTime' in model) and (now > model['EndTime']):
                return False
        return True

    def get_info(self, ark_id, game_name, group_bet_list):
        code, info = self._basic_check(ark_id)
        if code < 0:
            return self._Result(code)

        if game_name not in self._tab_model:
            return self._Result(-999)

        game_model = []
        for model in self._tab_model[game_name]:
            if model['Name'] not in info['Name']:
                continue
            if not self.check_activity_time(model):
                continue
            game_model.append(model)

        result = []
        for model in game_model:
            bonus_info = {}
            bet_mode = model['BetMode']
            bet_mode = [bet_mode.get(i, False) for i in ['Bet', 'ExtraBet']]
            bet_list = self._modify_bet_list(self.get_player_valid_bet(model, group_bet_list), bet_mode[0], bet_mode[1])
            if len(bet_list) <=0:
                continue

            bonus_info['BetMode'] = bet_mode
            bonus_info['BetList'] = bet_list
            bonus_info['Name'] =model['Name']
            bonus_info['GameName'] = model['GameName']
            bonus_info['SpecialGame'] = model['SpecialGame']
            bonus_info['SpecialGameType'] = model['SpecialGameType']
            bonus_info['BetLines'] = model['BetLines']

            if 'EndTime' in model:
                end_time = model['EndTime']
                bonus_info['EndTimeTs'] = int(time.mktime(end_time.timetuple()))
                bonus_info['CountdownTs'] = int(time.mktime(end_time.timetuple())) - int(time.mktime(datetime.datetime.now().timetuple()))
            result.append(bonus_info)

        self.logger.debug('[SlotBonus] get_info ark_id: {}, result: {}'.format(ark_id, result))
        return self._Result(0, DataList=result)

    def get_bet_info(self, model_info, bet_value, line_bet_list=None, game_setting=None):
        if line_bet_list is None and (game_setting is None or 'line_bet_list' not in game_setting):
            return None
        if line_bet_list is None:
            line_bet_list = game_setting['line_bet_list']
        bet_list = self.get_player_valid_bet(model_info, line_bet_list)
        if not isinstance(bet_value, (int, float)):
            return None
        for item in bet_list:
            if bet_value in [item['LineBet']]:
                return item
        return None

    def get_player_valid_bet(self, bet_model, group_bet_list):
        model_bet_info = bet_model.get('BetList')
        group_bet_info = [{'LineBet':i} for i in group_bet_list]
        bet_info = group_bet_info
        if model_bet_info is not None:
            model_bet_list = [v for i in model_bet_info for k, v in i.items() if k in ['LineBet']]
            # 押注段交集
            inter_bet_list =list(set(group_bet_list).intersection(set(model_bet_list)))
            bet_info = [i for i in model_bet_info if i['LineBet'] in inter_bet_list]

        result = [dict(bet_model['DefBetList'], **b) for b in bet_info]
        return result

    def IsBetActivityEffective(self):
        return True

    def WinAmountComfirm(self, win, bet, model_info):
        return True

    def _modify_bet_list(self, bet_list, bet_enable, extra_bet_enable):
        result = []
        field = ['Currency', 'LineBet']
        field = field + ['CostMulti'] if bet_enable else field
        field = field + ['ExtraCostMulti'] if extra_bet_enable else field
        for b in bet_list:
            result.append({k: v for k, v in b.items() if k in field})
        return result

    # def get_bonus_game_Info(self, game_id, assignProbId=None):
    #     return self._tab_game_info[game_id]

    def _Result(self, nResult=0, Src=None, *args, **kwargs):
        r = {} if 'OutParam' not in kwargs else kwargs['OutParam']
        r['Code'] = nResult
        # 透過args挑出想留下的欄位
        if Src is not None:
            if len(args) <= 0:
                r.update(Src)
            else:
                for k in args:
                    r[k] = Src[k]
        # 透過kwargs合併欄位
        r.update(kwargs)
        r.pop('OutParam', None)
        '''
        for k in kwargs:
            if k != 'OutParam':
                r[k] = kwargs[k]
        '''
        return r

    def DbLog(self, strArkId, name, type, gameNo, serialNo, bet, extrabet, cost, probId, **kwargs):
        return self.SBDao.DbLog(strArkId, name, type, gameNo, serialNo, bet, extrabet, cost, probId, **kwargs)

    # def get_gate_and_max_win(self, version, group, game_name, ark_id, nTotalBet):
    #     if self.BonusBuffer is None:
    #         self.logger.error("[SlotBonus] {} Buffer is None ".format(self.name))
    #         return None, None
    #     no_win_gate, max_win = self.BonusBuffer.get_gate_and_max_win(version, group, game_name, ark_id, nTotalBet)
    #     return no_win_gate, max_win
    #
    # def incr_buffer(self, version, group, game_name, bet, win, jp_bet):
    #     if self.BonusBuffer is None:
    #         self.logger.error("[SlotBonus] {} Buffer is None ".format(self.name))
    #         return None, None
    #     self.BonusBuffer.incr_buffer(version, group, game_name, bet, win, jp_bet)



if __name__ == "__main__":
    import logging
    import os
    # from GuaiGuaiLib.Common.Dimension import Dimension
    from .FakerLoader import FakerLoader

    log_level = 'test'
    os_path = 'D:\\SVN_FILE\\iGaming\\trunk\\Server\\H5\\pixiu\\Game'
    ConfigPath = 'D:\\SVN_FILE\\iGaming\\trunk\\Server\\H5\\pixiu\\Game/config/local/'
    module_list = ['mongo_manager', 'config_manager', 'log_manager', 'timer_service', 'user_manager']

    os.chdir(os_path)
    print(os.getcwd())

    logging.basicConfig()
    logger = logging.getLogger(log_level)
    loader = FakerLoader(os_path, ConfigPath, module_list).get_loader()


    # Dimension = Dimension(ConfigFile=ConfigPath, Logger=logger, GetUserDataFunc=loader.get_class_instance("user_manager").get_user_data)
    # slotBonus = SlotBonus(logger, Name='BuyBonus', ConfigPath=ConfigPath, GetInfoFunc=Dimension.GetInfo, InfoRegisterFunc=Dimension.InfoRegister)
    slotBonus = SlotBonus(logger, Name='BuyBonus', ConfigPath=ConfigPath)
    # _tabSetting = iGamingLoginDao.LoadSetting()
    # group = 'gw99'
    # version = platformUtil.GetVersion(group.lower())
    # setting = _tabSetting.get(version.lower(), _tabSetting.get('default'))
    ark_id = '10000021'
    gameName = 'LegendOfTheWhiteSnake'

    result = slotBonus.get_info(ark_id, gameName)
    print(result)
