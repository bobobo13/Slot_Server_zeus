#!/usr/bin/env python
# -*- coding: utf-8 -*-


import inspect
from .slot_status_code import *
from .main_game_result import *
from .main_game_play_info import *
import importlib, os, sys
from .IgsRandomer import IgsRandomer
from .Util.Util import Copy as copy


class FeverLevelResult:
    def __init__(self, sg_id):
        self.is_gameover = False
        self.sg_id = int(sg_id)
        self.win_amount = 0
        self.fever_map = None
        self.error = None
        self.last_reel = {}
        self.show_reel = list()
        self.extra_reel_info = list()  # 儲存GameLog壓文字在symbol上的位置，格式和show_reel一致
        self.wheel_block_result_index = '3'
        self.in_game_jp_win_list = list()
        self.this_win = 0
        self.win_fever_times = 0  # 增加這場fever game手數
        self.win_retrigger_fever_times = 0  # 增加一場fever game(不含JP game)次數
        self.log_custom = dict()  # 紀錄客製化遊戲log
        self.hit_max_win = False

    @property
    def has_error(self):
        return (self.error != None)

    @property
    def is_win_fever_times(self):
        return self.win_fever_times > 0 or self.win_retrigger_fever_times > 0

    @property
    def current_level(self):
        if self.fever_map is None:
            raise Exception("Trying to get current level with fever_map is None.")
        return self.fever_map._id

    def __str__(self):
        return str("{{ sg_id: {}, win_amount: {}, this_win: {}, is_gameover: {}, show_reel: {}, last_reel: {}, extra_reel_info: {}, wheel_block_result_index={}, win_fever_times: {}, win_retrigger_fever_times: {}, in_game_jp_win_list: {}, log_custom: {} }}".format(
            self.sg_id, self.win_amount, self.this_win, self.is_gameover, self.show_reel, self.last_reel, self.extra_reel_info, self.wheel_block_result_index, self.win_fever_times, self.win_retrigger_fever_times, self.in_game_jp_win_list, self.log_custom))

class DoubleGameResult:
    def __init__(self):
        self.game_data_dic = {}
        self.is_gameover = False
        self.total_win = 0
        self.double_game_info = None
        self.total_bet = 0
        self.error = None
        self.has_result = False

    @property
    def has_error(self):
        return (self.error != None)


class FeverMap:
    def __init__(self, fever_id):
        self._id = fever_id
        self._map = {}

    def append(self, key, value):
        self._map[str(key)] = value

    def update(self, key, value=None):
        if str(key) in self._map:
            self._map[str(key)] = value

    def get(self, key, default=None):
        ret = default
        if str(key) in self._map:
            ret = self._map[str(key)]
        return ret

    def export(self):
        return {'sg_state': self._id,
                'sg_map': self._map}


class DefaultSlotCalculator(object):

    def _init_weight_info(self):
        # self.weight_info = {}
        # pattern = re.compile("\w+weight")
        # 轉存成weight需要的格式(還沒想好)
        pass

    def _init_consts(self):
        pass

    def get_probability_ver(self):
        return self._gameSetting.get('probability_ver', '0.0.1')

    def __init__(self, logger, gameSetting, **kwargs):
        self.logger = logger
        self._gameSetting = gameSetting
        self._game_id = gameSetting['game_id']
        # self._game_info = gameSetting
        self.reel_amount = gameSetting['reel_amount']
        self.reel_length = gameSetting['reel_length']

        self.line_data = gameSetting.get('line_data')
        self.check_reel_amount = gameSetting['check_reel_amount']
        self.check_reel_length = gameSetting['check_reel_length']
        self.invisible_symbols = self.reel_length - self.check_reel_length

        self._reel_data = {}  # _reel_data[str(block_index)][str(reel_index)]
        self._odds = gameSetting['odds']  # _odds[str(symbol_id)][int(symbol count)]
        self._special_odds = gameSetting['special_odds']  # _odds[str(symbol_id)][int(symbol count)]
        self._extra_info = self._gameSetting.get("extra_info", {})

        self._init_weight_info()
        self._init_consts()
        self._DelegateFuncs = kwargs

        self.randomer = kwargs.get('randomer')
        if self.randomer is None:
            self.randomer = IgsRandomer()
        self.get_result_by_weight = self.randomer.get_result_by_weight
        self.get_result_by_gate = self.randomer.get_result_by_gate

        self._initChanceCheck()

    def _initChanceCheck(self):
        chanceMod = importlib.import_module("{}Chance".format(self.__class__.__module__))
        checkMod = importlib.import_module("{}Check".format(self.__class__.__module__))
        self._chance = getattr(chanceMod, "{}Chance".format(self._game_id))(randomer=self.randomer)
        self._check = getattr(checkMod, "{}Check".format(self._game_id))(randomer=self.randomer)
        self._chance.init_game_reel_info(self.reel_amount, self.reel_length,
                                         self.check_reel_amount, self.check_reel_length)

    def SetRandomer(self, randomer):
        self.randomer = randomer
        self._chance.randomer = randomer
        self._check.randomer = randomer
        self.get_result_by_weight = self.randomer.get_result_by_weight
        self.get_result_by_gate = self.randomer.get_result_by_gate

    def get_init_reel_info(self, game_state):
        """
        START_GAME的時候需要取得初始牌面
        :return:
        """
        view_reels = game_state.last_main_reels.get('0', {})
        if len(view_reels) <= 0:
            view_reels = self._chance.get_init_reel(self._gameSetting['fake_main_reels'], self.reel_length,
                                                    self.reel_amount)
        fake_reels = self._gameSetting['fake_main_reels']['0']
        special_game_state = game_state.current_special_game_data
        special_game_id = game_state.current_sg_id
        block_data = dict()
        if special_game_state is not None:
            special_game_view_reels = game_state.get_special_game_last_reels(special_game_id)
            if special_game_view_reels is not None:
                view_reels = special_game_view_reels['0']
            fake_reels = self._gameSetting['fake_fever_reels']['0']
            if special_game_state['current_level'] == 1:
                win_special_symbols = game_state.win_special_symbols
                if len(win_special_symbols):
                    block_data['win_special_symbols'] = win_special_symbols
        ret = list()
        block_data.update({
            'id': 0,
            'init_wheels': view_reels,
            'fake_wheels': fake_reels,
        })
        ret.append(block_data)
        return ret

    def get_extra_info(self, game_state):
        # FOR START_GAME
        return {}

    def get_custom_info(self):
        """
        SS 在 start_game 封包 需要額外附加的平台相關資訊 來源從 game_setting 取得
        """
        custom_info = {
            "jp_bet": self._gameSetting.get("jp_bet"),
            "recovery_need_start": True,
        }
        return custom_info

    def get_bet_level(self, bet_value, gameInfo, bet_lines=None):
        return 0

    def get_extra_bet_info(self):
        if "extra_bet" in self._gameSetting:
            return self._gameSetting["extra_bet"]
        return None

    def get_spin_reels_id(self, bet_value, bet_lines, gameInfo):
        extraOdds = gameInfo.get('extra_odds', {})
        spin_reels_id = self.get_bet_level(bet_value, gameInfo, bet_lines)
        if "spin_reels_id_weight" not in extraOdds:
            return spin_reels_id

        total_bet = bet_value * bet_lines
        for spin_reels in extraOdds["spin_reels_id_weight"]:
            if total_bet < spin_reels['total_bet']:
                break
            _index, spin_reels_id = self.randomer.get_result_by_weight(spin_reels["spin_reels_id"], spin_reels["weight"])
        return spin_reels_id

    def get_fever_reels_id(self, game_state, gameInfo):
        sg_id = game_state.current_sg_id
        special_game_state = game_state.current_special_game_data
        bet_value = special_game_state['current_bet']
        bet_lines = special_game_state['current_line']  # 押注線數
        spin_reels_id = self.get_bet_level(bet_value, gameInfo, bet_lines)
        return spin_reels_id

    def custom_last_main_reels(self, spin_result):
        return spin_result.spin_reels

    def custom_last_fever_reels(self, fever_result):
        return fever_result.last_reel, False

    def spin_flow(self, bet_value, bet_lines, game_state, gameInfo, dev_mode=DevMode.NONE, **kwargs):
        extra_info = None
        oriExtraInfo = copy.deepcopy(game_state.extra_info)
        extra_bet = kwargs.pop('extra_bet', False)
        spin_result = self.spin(bet_value, bet_lines, game_state, gameInfo, dev_mode, extra_info=oriExtraInfo,
                                extra_bet=extra_bet, **kwargs)
        if isinstance(spin_result, tuple):
            spin_result, extra_info = spin_result

        # 檢查贏分是否有超過倍數上限
        if 'MaxOdds' in gameInfo:
            total_bet = bet_value * bet_lines  # total_bet 不算extra_bet
            win_limit = gameInfo['MaxOdds'] * total_bet
            if spin_result.this_win > win_limit:
                spin_result.this_win = win_limit
                spin_result.hit_max_win = True
                # 如果有進特殊遊戲，也要一起中止
                spin_result.reset_win_special_game()

        return spin_result, spin_result.this_win, extra_info

    def bonus_spin_flow(self, bet_value, bet_lines, game_state, game_info, special_game, extra_bet=False, is_ingame_jp=False, enable_jp=False, **kwargs):
        spin_result, extra_info = None, {}
        extra_bet_info = self.get_extra_bet_info()
        extra_bet = kwargs.pop('extra_bet', False)
        if extra_bet and ("Ratio" not in extra_bet_info or extra_bet_info["Ratio"] is None):
            return spin_result, 0, extra_info

        temp_spin_result, temp_extra_info = self.gen_bonus_result(bet_value, bet_lines, game_state, game_info, special_game, extra_bet, is_ingame_jp, enable_jp, **kwargs)
        if temp_spin_result is None:
            return spin_result, 0, extra_info

        spin_result, extra_info = temp_spin_result, temp_extra_info

        # 檢查贏分是否有超過倍數上限
        if 'MaxOdds' in game_info:
            total_bet = bet_value * bet_lines  # total_bet 不算extra_bet
            win_limit = game_info['MaxOdds'] * total_bet
            if spin_result.this_win > win_limit:
                spin_result.this_win = win_limit
                spin_result.hit_max_win = True
                # 如果有進特殊遊戲，也要一起中止
                spin_result.reset_win_special_game()

        return spin_result, spin_result.this_win, extra_info

    def gen_bonus_result(self, bet_value, bet_lines, game_state, gameInfo, special_game, extra_bet=False, is_ingame_jp=False, enable_jp=False, dev_mode=DevMode.NONE, **kwargs):
        extra_info = None
        oriExtraInfo = copy.deepcopy(game_state.extra_info)
        spin_result = self.bonus_spin(bet_value, bet_lines, game_state, gameInfo, special_game, dev_mode, extra_info=oriExtraInfo, extra_bet=extra_bet)
        if spin_result is None:
            return None, None
        if isinstance(spin_result, tuple):
            spin_result, extra_info = spin_result

        if not spin_result.is_win_special_game:
            self.logger.warn("[slot_calculator] [gen_bonus_result] is_win_special_game false 1 ")
            return None, None

        return spin_result, extra_info

    def bonus_spin(self, bet_value, bet_lines, game_state, gameInfo, special_game, dev_mode=DevMode.NONE, **kwargs):
        return self.spin(bet_value, bet_lines, game_state, gameInfo, dev_mode, **kwargs)

    def get_spin_reel_data(self, gameInfo, is_fever=False):
        if is_fever:
            return gameInfo['fever_reels']
        else:
            return gameInfo['main_reels']

    def spin(self, bet_value, bet_lines, game_state, gameInfo, dev_mode=DevMode.NONE, **kwargs):
        play_info = MainGamePlayInfo()
        play_info.set_is_fever_game(False)
        play_info.set_bet_info(bet_value, bet_lines)

        block_id = 0
        result = MainGameResult([block_id])
        spin_reel_data = self.get_spin_reel_data(gameInfo)[str(block_id)]
        self._chance.get_spin_result(result, block_id, spin_reel_data, self.reel_length, self.reel_amount, self.check_reel_length, self.check_reel_amount, dev_mode)
        self._check.game_check(result, block_id, play_info, self._odds, self._special_odds, self._extra_odds, self.reel_length, self.reel_amount, self.check_reel_length, self.check_reel_amount)

        return result

    def next_fever_flow(self, client_action, game_state, gameInfo, dev_mode=DevMode.NONE, **kwargs):
        game_state = copy.deepcopy(game_state)  # 不覆蓋原本GameState

        fever_result = self.next_fever(client_action, game_state, gameInfo, dev_mode)

        # 檢查總贏分是否有超過倍數上限
        if 'MaxOdds' in gameInfo:
            total_bet = game_state.current_special_game_data['current_bet'] * game_state.current_special_game_data['current_line']
            win_limit = gameInfo['MaxOdds'] * total_bet

            # 累積贏分+本次贏分是否有超過上限
            if game_state.one_play_win_amount + fever_result.this_win > win_limit:
                fever_result = self.abort_fever_by_limit(fever_result, game_state, gameInfo, win_limit)

        return fever_result, game_state

    def next_fever(self, client_action, game_state, gameInfo, dev_mode=DevMode.NONE, simulation_mode=False, resume_randomer=None):
        fever_result = FeverLevelResult(0)
        if client_action['client_sg_id'] != game_state.current_sg_id:
            fever_result.error = True
            return fever_result
        return fever_result

    def abort_fever_by_limit(self, fever_result: FeverLevelResult, game_state, gameInfo, win_limit):
        """
        當達到贏分上限時，會呼叫此Function中止Fever Game
        由各遊戲繼承後各自實做
        :return: FeverLevelResult
        """
        raise NotImplementedError
        fever_result.is_gameover = True
        fever_result.hit_max_win = True
        ctl_win = win_limit - game_state.one_play_win_amount
        # ctl_win = fever_result.this_win + game_state.one_play_win_amount - win_limit
        fever_result.this_win = ctl_win
        fever_result.win_amount = ctl_win
        return fever_result

    def get_free_recovery(self, game_state):
        return {}

    def get_fever_recovery(self, game_state):
        return {}

    def after_spin_current_script(self, game_state):
        return {}

    # JHW
    def fever_after_action(self, game_state):
        pass

    # update fever result
    def update_fever_result(self, fever_result, fever_update_data):
        pass

    def after_hit_jackpot_doing(self, fever_result, fever_update_data, game_state, user_id=None, jackpot_manager=None):
        return fever_result, game_state

    def get_special_game_data(self, bet_value, bet_lines, **kwargs):
        return {}

    # update spin result
    def update_spin_result(self, spin_result, spin_update_data):
        return spin_result

    # update fever last reels
    def get_updated_fever_last_reels(self, fever_result):
        # print 'get_updated_fever_last_reels ', fever_result.last_reel
        return fever_result.last_reel

    def is_in_game_jackpot_exist(self):
        return 'progressive_level_num' in self._gameSetting

    def is_double_game_enable(self):
        if 'double_game_info' in self._gameSetting:
            if 'double_enable' in self._gameSetting['double_game_info']:
                return self._gameSetting['double_game_info']['double_enable']
        return False

    def get_reel_info_log(self, spin_result):
        return spin_result.get_show_reel_and_clean()

    def get_extra_reel_info_log(self, spin_result):
        return spin_result.extra_reel_info

    def get_feature_win_log(self, spin_result):
        return []

    def get_feature_type_dic(self, spin_result):
        return None

    def get_fever_reel_info_log(self, fever_result):
        reel_info = fever_result.show_reel
        return [reel_info[0]] if len(reel_info) > 0 else [reel_info]

    def get_fever_extra_reel_info_log(self, fever_result):
        return fever_result.extra_reel_info

    def get_fever_feature_win_log(self, fever_result):
        return [], None

    def modify_bingo_log(self, spin_result, bingo_log):
        return bingo_log

    def modify_fever_bingo_log(self, fever_result, bingo_log):
        return bingo_log

    def get_fever_additional_jp_ann_win(self, total_bet, fever_result):
        return 0

    def get_jp_pick_log(self, game_state):
        """
        生成jp pick game的client game log
                    未開   Grand   Major   Minor   Mini  Bonus
        server      -1        0          1         2         3        4
        client      60       61        62        63       64       65
        """
        return []

    def show_reel_big_symbol_split(self, show_reel, big_symbol_list, left_reels, big_symbol_width, big_symbol_length, check_reel_length):
        """
        用在GameLog切割大symbol
        :param show_reel: 就是check_reel，不要像main_result, fever_result多包一層的
        :param big_symbol_list: 大symbol ID列表
        :param left_reels: 大symbol出現的最左側輪數
        :param big_symbol_width: 大symbol寬度
        :param big_symbol_length: 大symbol長度，如果跟check_reel_length不一樣長要留意
        :param check_reel_length:
        :return: show_reel, 要存入fever_result.show_reel時請用append()
        """
        for col in left_reels:
            for row in range(check_reel_length):
                if show_reel[col][row] not in big_symbol_list:
                    continue
                if row == 0:
                    show_reel_big_symbol_length = 1
                    while show_reel_big_symbol_length < big_symbol_length and row + show_reel_big_symbol_length < check_reel_length and show_reel[col][row + show_reel_big_symbol_length] == show_reel[col][row]:
                        show_reel_big_symbol_length += 1
                    shift = show_reel_big_symbol_length - big_symbol_length
                else:
                    shift = row
                self._split_shift(show_reel, show_reel[col][row], col, shift, big_symbol_width, big_symbol_length, check_reel_length)
        return show_reel

    def _split_shift(self, show_reel, symbol, left_reel, shift, big_symbol_width, big_symbol_length, check_reel_length):
        # considered in case of big_symbol_length == check_reel_length, NEED TO THINK ABOUT NOT EQUAL CASES
        # if symbol size is larger than 10, change "symbol * 10" to larger number
        if shift > 0:
            modify_rows = range(shift, check_reel_length)
        else:
            modify_rows = range(big_symbol_length + shift)
        for row in modify_rows:
            for col in range(big_symbol_width):
                show_reel[left_reel + col][row] = symbol * 10 + check_reel_length * col + row - shift

    # def add_marquee(self, AddMarqueeFunc, ret=None, extra_bet=None, chkType=None):
    def add_marquee(self, strLogo=None, nKioskId=None, strArkId=None, strNickName=None, ret=None, AddMarqueeFunc=None, extra_bet=None, chkType=None):
        winMap = {4: "BigWin", 5: "MegaWin", 6: "SuperWin"}
        gameType = None
        if (AddMarqueeFunc is None) or (ret is None) or (strLogo is None) or (nKioskId is None) or (strArkId is None) or (extra_bet is None) or (chkType is None):
            return None
        sgState = ret.get('sg_state', -1)

        # 檢查FreeGame最後一手、MainGame每一手
        if chkType == 'fever' and sgState >= 4:
            gameType = 'FreeGame'
        elif chkType == 'normal':
            winType = ret.get('win_type', -1)
            gameType = winMap.get(winType)
        if gameType is None:
            return None

        gameType = 'Extra' + gameType if extra_bet else gameType
        totalWin = ret.get('total_win_amount', 0)
        AddMarqueeFunc("GAME", strArkId, strNickName, self._game_id, totalWin, Logo=strLogo, KioskId=nKioskId, GameType=gameType)

    def get_win_type(self, bet, win, gameState, result, winTypeInfo):
        type = 0
        if bet == 0 or win == 0 or bet is None or win is None:
            return 0
        win_bet_multiple = float(win) / bet
        for t, g in enumerate(winTypeInfo):
            if win_bet_multiple >= g:
                type = t
            else:
                break
        # "NO_WIN": 0, "NORMAL_WIN": 1, "LIGHT_WIN": 2, "SMALL_WIN": 3, "BIG_WIN": 4,"MEGA_WIN": 5, "SUPER_WIN": 6
        return type

    def get_fever_win_type(self, bet, win, game_state, result, winTypeInfo):
        return self.get_win_type(bet, win, game_state, result, winTypeInfo)

    def SerialFeverGameCnt(self, current_script, CurrType=1):
        ret = {"CurrType": CurrType}
        t = {
            "Total": "total_times",
            "Current": "current_times"
        }
        for key in t:
            if t[key] in current_script:
                ret[key] = current_script[t[key]]
        return ret

    def GetWinJpList(self, game_state, result):
        if not self.is_in_game_jackpot_exist():
            return None

        sg_id = game_state.current_sg_id
        if sg_id < 0:
            return self.SpinWinJpList(game_state, result)
        else:
            return self.FeverWinJpList(game_state, result)

    def SpinWinJpList(self, game_state, spin_result):
        r = []
        win_jp_level = spin_result.jackpot_info.get('WinJP', -1)
        if win_jp_level >= 0:
            r.append(win_jp_level)
        return r

    def FeverWinJpList(self, game_state, fever_result):
        r = []
        current_script = game_state.current_special_game_data['current_script']
        if 'WinJP' not in current_script:
            return []
        win_jp_level = current_script['WinJP']
        if win_jp_level >= 0:
            r.append(win_jp_level)
        return r
        # line_bet = game_state.current_special_game_data['current_bet']
        # ingame_jp_bonus_win = current_script['WinBonusWin'] if 'WinBonusWin' in current_script else 0

    def JackpotFlow(self, gameInfo, JpMgr, ark_id, bet_value, bet_lines, result, game_state, playerData=None, isJpWinIndependent=False, extra_bet=False, ratio=1):
        winJpList = []
        total_jackpot_win = 0
        if (JpMgr is None) or (not JpMgr.is_ingame_jp_game(self._game_id)):
            return result, game_state, total_jackpot_win, winJpList, "", {}
        jp_system = "ingame"
        group = ""
        jp_contribution = {}
        if "packGroup" in self._DelegateFuncs:
            group = self._DelegateFuncs["packGroup"](ark_id, playerData)
        JpMgr.updJpInfo(self._game_id, group, self._gameSetting['jp_info'])
        sg_id = game_state.current_sg_id
        if sg_id < 0:
            jp_bet = bet_value * bet_lines
            if extra_bet:
                extraBetInfo = self._gameSetting.get("ExtraBet", self._gameSetting.get("extra_bet"))
                if extraBetInfo is None:
                    self.logger.error("[{}] {} Get ExtraBetInfo failed!".format(self._game_id, sys._getframe().f_code.co_name))
                else:
                    jp_bet *= extraBetInfo.get("JpBetRatio", 1)
            jp_bet *= ratio
            jp_contribution = JpMgr.AddPool(self._game_id, group, jp_bet)

        winJpList = self.GetWinJpList(game_state, result)
        if len(winJpList) <= 0:
            return result, game_state, total_jackpot_win, winJpList, jp_system, jp_contribution

        for JpLevel in winJpList:
            winJpResult = JpMgr.WinJp(self._game_id, group, JpLevel, bet_value, ark_id)
            if winJpResult is None:
                self.logger.error("[{}] {}, Win ingame JP failed!  win_jp_level={}".format(self._game_id, sys._getframe().f_code.co_name, JpLevel))
                break
            JpData = {}
            JpData['win_jp_level'] = JpLevel
            JpData['win_jp_result'] = winJpResult
            jackpot_win = winJpResult['award']
            total_jackpot_win += jackpot_win

            if sg_id < 0:
                result = self.update_spin_result(result, JpData)
                if not isJpWinIndependent:
                    result.this_win += jackpot_win
            else:
                self.update_fever_result(result, JpData)
                result, game_state = self.after_hit_jackpot_doing(result, JpData, game_state)
                if not isJpWinIndependent:
                    result.win_amount += jackpot_win

        return result, game_state, total_jackpot_win, winJpList, jp_system, jp_contribution

    def build_custom_log(self, total_bet, total_win, jackpot_win, winJpList, chkType=None, spin_result=None, fever_result=None, win_type_info=None, **kwargs):
        """
        SS 用來產生 SessionGameLog特規欄位 / Line大獎通報 所需要資料
        custom_log = {
          "GameSerialID" : 標記這是特殊遊戲的第幾手 各遊戲評估是否需複寫去對應資料
          "Reason" : 標記這把牌面屬於什麼獎 各遊戲評估是否需複寫去對應資料
          "Device" : 標記這把玩家是用何種裝置遊玩
          "PrizeType" : 標記這把牌面觸發哪種報獎 討論廢棄 目前先不帶
          "BigWinMsg" : 串接大獎LINE通報的獎項字串
          "TempIntX" : 各遊戲依需求自行規劃 可不帶
          "TempStrX" : 各遊戲依需求自行規劃 可不帶
          "TempText" : 各遊戲依需求自行規劃 可不帶
        }
        """

        extra_bet = kwargs.get("extra_bet", False)
        client_data = kwargs.get("cmd_data", {})
        device = client_data.get("device", 0)
        serial_id = 0 if (fever_result is None or fever_result.fever_map is None) else fever_result.fever_map.get("0", 0)
        sg_id = spin_result.first_special_id if chkType == "normal" else fever_result.sg_id

        sg_mapping = self._gameSetting.get("sg_mapping", {})
        now_game = sg_mapping.get(str(sg_id), "Unknown")

        reason = 0
        if win_type_info is not None and total_win > 0:
            win_odds = float(total_win) / total_bet
            for idx, value in enumerate(win_type_info):
                if win_odds >= value:
                    reason = idx
                else:
                    break

        big_win_msg = None
        if chkType == "normal" or (chkType == "fever" and fever_result.is_gameover):
            big_win_msg = now_game
        if jackpot_win > 0:
            big_win_msg = "InGameJP"

        if isinstance(big_win_msg, str):
            big_win_msg = 'Extra' + big_win_msg if extra_bet else big_win_msg

        custom_log = {
            "GameSerialID": serial_id,
            "Reason": reason,
            "Device": device,
            "BigWinMsg": big_win_msg
        }
        return custom_log

    def build_result_log(self, ret_data, spin_result, jp_win, extra_bet=False):
        # calculator = self.dispatcher._calculators[game_id]
        result_log = {'reel_info': [], 'extra_reel_info': [], 'bingo_line': [], 'feature_win': []}
        # result_log['extra_bet'] = extra_bet
        # bingo_info = ret_data['wheel_blocks'][0]['bingo'] if 'bingo' in ret_data['wheel_blocks'][0] else []
        bingo_log = []
        if len(spin_result.reel_block_data) > 0:
            for wb in spin_result.reel_block_data.values():
                bingo_log.extend(copy.deepcopy(wb.bingo))
        for item in bingo_log:
            item.pop('pos', None)
        result_log['reel_info'] = self.get_reel_info_log(spin_result)
        result_log['extra_reel_info'] = self.get_extra_reel_info_log(spin_result)
        result_log['bingo_line'] = self.modify_bingo_log(spin_result, bingo_log)
        result_log['feature_win'] = self.get_feature_win_log(spin_result)
        type_dic = self.get_feature_type_dic(spin_result)
        if type(type_dic) is dict:
            result_log.update(type_dic)
        if jp_win > 0:
            result_log['jp_win'] = jp_win
        # print '------------result_log: ' + ppretty(result_log, seq_length=1000)
        # print '------------totoal win='+str(ret_data['total_win_amount'])
        # print '------------ret_data='+ppretty(ret_data, seq_length=1000)
        return result_log

    def build_fever_result_log(self, ret_data, fever_result, game_state):
        result_log = {'reel_info': [], 'extra_reel_info': [], 'bingo_line': [], 'feature_win': []}
        # result_log['extra_bet'] = game_state.extra_bet
        # if '3' in ret_data['sg_map'] and 'bingo' in ret_data['sg_map']['3']:
        #    print '----fever win result='+ppretty(ret_data['sg_map']['3'], seq_length=1000)
        rpos = fever_result.wheel_block_result_index
        # print 'rpos=', rpos
        # print '---fever sgmap='+ppretty(ret_data['sg_map'][str(rpos)], seq_length=1000)
        # print '---fever sgmap='+ppretty(ret_data['sg_map'], seq_length=1000)
        bingo_info = fever_result.fever_map.get(str(rpos))['bingo'] if (type(
            fever_result.fever_map.get(str(rpos))) is dict and 'bingo' in fever_result.fever_map.get(str(rpos))) else []
        bingo_log = copy.deepcopy(bingo_info)
        for item in bingo_log:
            item.pop('pos', None)
        result_log['reel_info'] = self.get_fever_reel_info_log(fever_result)

        if (len(result_log['reel_info']) == 0 or len(result_log['reel_info'][0]) == 0) and \
                'WinJP' in game_state.current_special_game_data['current_script'] and \
                game_state.current_special_game_data['current_script']['WinJP'] >= 0:
            result_log['reel_info'] = self.get_jp_pick_log(game_state)
        result_log['extra_reel_info'] = self.get_fever_extra_reel_info_log(fever_result)
        result_log['bingo_line'] = self.modify_fever_bingo_log(fever_result, bingo_log)
        result_log['feature_win'], type_dic = self.get_fever_feature_win_log(fever_result)
        if type(type_dic) is dict:
            result_log.update(type_dic)
        # print '------------fever_result_log: ' + ppretty(result_log, seq_length=1000)
        # print '------------totoal win='+str(ret_data['this_win_amount'])
        return result_log

    def ModifySettledResult(self, sg_id, sg_state, sg_map, client_action):
        return sg_map

    def SettleFeverClientAction(self, sg_id):
        gameSetting = self._gameSetting
        client_action_data = {}
        if "FeverDefaultAction" in gameSetting and str(sg_id) in gameSetting["FeverDefaultAction"]:
            client_action_data = copy.copy(gameSetting["FeverDefaultAction"][str(sg_id)])
            for key, val in client_action_data.iteritems():
                if type(val) == dict and "RAND_PICK" in val:
                    # client_action_data[key] = i % (val["RAND_PICK"][1]-val["RAND_PICK"][0]) + val["RAND_PICK"][0]
                    client_action_data[key] = self.randomer.choice(val["RAND_PICK"])
        return client_action_data

    def GetBetType(self, gameInfo, bet_value, extra_bet):
        bet_lines = self.GetBetLines()
        betType = str(self.get_bet_level(bet_value, gameInfo, bet_lines))
        if extra_bet:
            betType = "EX" + betType
        return betType

    def GetBetLines(self):
        gameSetting = self._gameSetting
        if 'max_ways' in gameSetting:
            bet_lines = gameSetting['max_ways']
        elif 'max_costs' in gameSetting:
            bet_lines = gameSetting['max_costs']
        else:
            bet_lines = gameSetting['max_lines']
        return bet_lines

    def GetExtraBetRatio(self):
        gameSetting = self._gameSetting
        if "extra_bet" not in gameSetting:
            return 1
        return gameSetting["extra_bet"]["Ratio"]

    def _build_client_action(self, game_state, client_action=None, assign_selection=-1):
        if client_action is None or game_state.current_special_game_level == 1:
            client_action = {}
        for i in range(5):
            if assign_selection >= 0:
                client_action[str(i)] = assign_selection
            elif str(i) not in client_action:
                client_action[str(i)] = 0
            else:
                client_action[str(i)] += 1

        special_game_id = game_state.current_sg_id
        client_action.update({'client_sg_id': special_game_id})
        return client_action
