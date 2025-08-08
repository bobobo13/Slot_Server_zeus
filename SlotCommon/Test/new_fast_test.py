#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import sys
import importlib
import collections
import multiprocessing
import traceback

import SlotCommon.slot_calculator
import SlotCommon.player_game_state
from SlotCommon.player_game_state import *
from SlotCommon.Test.record_data import *
from SlotCommon.Test.double_output import *
from SlotCommon.IgsRandomer import IgsRandomer
from SlotCommon.Util.Util import Copy as copy
from SlotCommon.Util.MathTool import floor_float

LOG_LEVEL = 3
PLAYER_SELECTION = 1
CURRENCY = ""
PROB_ID = 1
ProbId = ""
TEST_MERCHANT_ID = 0
TEST_LINECODE = ""
TEST_PROCESS_NUM = 1

class FakeDispatcher(object):
    def __init__(self, logger):
        self.env = "local"
        self.splunk_sender = FakeSplunkSender(logger)
        self.in_game_jackpot_manager = None
        self.logger = logger

class FastTest(object):
    def __init__(self, logger, game_id, bet_value, extra_bet=False, setting=None, calculator=None, probId=None):
        self.logger = logger
        # self.PKGPath = PKGPath
        self.calculator = calculator
        self.game_id = game_id
        self.bet_value = bet_value
        self.extraBet = extra_bet
        self.probId = probId
        self.randomer = IgsRandomer()

        self.extra_bet_ratio = 1
        # ExtraBet Ratio
        if extra_bet and 'extra_bet' in setting:
            self.extra_bet_ratio = setting['extra_bet']['Ratio']

        if 'max_ways' in setting:
            self.bet_lines = setting['max_ways']
        elif 'max_costs' in setting:
            self.bet_lines = setting['max_costs']
        else:
            self.bet_lines = setting['max_lines']

        self.currency = CURRENCY
        self.merchant_id = TEST_MERCHANT_ID
        self.line_code = TEST_LINECODE
        self._print_init_config()

    def _print_init_config(self):
        # 紀錄參數
        self.logger.out("================================")
        self.logger.out("PLAYER_SELECTION={}".format(PLAYER_SELECTION))
        self.logger.out(
            "Currency={}, ProbId={}, TotalBet={}, ExtraBet={}, PrefixWord={}".format(self.currency, self.probId,
                                                                                     self.bet_value * self.bet_lines,
                                                                                     self.extraBet, None))

    def _print_final_result(self):
        self.logger.out("\n\n\n===========================================")
        self.logger.out("==========     Final Result     ===========")

    def main(self, calculator: SlotCommon.slot_calculator.DefaultSlotCalculator,
             game_id: str, test_spin_time: int, record_time: int, process_num: int,
             extra_bet=False, info=None, **kwargs):
        if test_spin_time % process_num != 0:
            raise Exception(
                f"test_spin_time % process_num != 0, test_spin_time={test_spin_time}, process_num={process_num}")

        if process_num == 1:
            calculator = _CreateCalculator(None, game_id, setting, game_state=PlayerGameState(None), calMod=calMod, randomer=IgsRandomer())
            record = RecordData()
            record.init_info(self.game_id, self.bet_lines, self.bet_value, self.extra_bet_ratio)
            record = spin_until_reach_times(calculator, game_id, self.bet_value, self.bet_lines, PlayerGameState(None),
                                            record, test_spin_time, record_time, extra_bet, info, self.logger)
            record.print_record_data(self.logger)
            self._print_init_config()
            record.print_line_count(self.logger)
            # 檢查是否需要寫入csv
            if kwargs.get('wirteCsv', False):
                self.write_csv(record)
            return record
        else:
            # 切成10個去跑
            with multiprocessing.Pool(process_num) as p:
                total_record = RecordData()
                total_record.init_info(self.game_id, self.bet_lines, self.bet_value, self.extra_bet_ratio)
                target_times = test_spin_time // process_num
                arg_list = [(_CreateCalculator(None, game_id, setting, game_state=PlayerGameState(None), calMod=calMod,
                                               randomer=IgsRandomer()), game_id, self.bet_value, self.bet_lines,
                             PlayerGameState(None), RecordData(),
                             target_times, record_time, extra_bet, info, self.logger) for _ in range(process_num)]
                for i in range(process_num):
                    arg_list[i][5].init_info(self.game_id, self.bet_lines, self.bet_value, self.extra_bet_ratio)
                record_list = p.starmap(spin_until_reach_times, arg_list)

                # 統計結果
                for record in record_list:
                    total_record = total_record + record
                self._print_final_result()
                total_record.print_record_data(self.logger)
                self._print_init_config()
                total_record.print_line_count(self.logger)
                # 檢查是否需要寫入csv
                if kwargs.get('wirteCsv', False):
                    self.write_csv(total_record)
                return total_record

    def write_csv(self, record: RecordData):
        # csv路徑
        file_path = './Log/{}_{}_{}_{}_{}.csv'.format(game_id, self.bet_value,
                                                      'ExtraBet' if self.extraBet else 'NormalBet'
                                                      , self.probId, int(time.time()))
        with open(file_path, "a") as f:
            # 分成10000行寫入(參照線上)
            max_line_items = len(record.win_list) // 10000
            count = 0
            temp_str = ""
            for i in range(len(record.win_list)):
                count += 1  # 紀錄次數
                # csv用,分隔
                temp_str += str(floor_float(record.win_list[i], 3)) + ","
                if count == max_line_items:
                    f.write(temp_str + "\n")
                    count = 0
                    temp_str = ""


def spin_until_reach_times(calculator: SlotCommon.slot_calculator.DefaultSlotCalculator,
                           game_id: str, bet_value: float, bet_lines: int,
                           game_state: SlotCommon.player_game_state.PlayerGameState,
                           record: RecordData, target_times: int, record_time: int, extra_bet=False, info=None,
                           logger=None):
    while True:
        # 沒有特殊遊戲，進行一般spin
        if not game_state.is_scatter_game:
            spin(calculator, game_id, bet_value, bet_lines, game_state, record, extra_bet, info=info)
        elif game_state.is_special_game:
            game_state = next_fever(calculator, game_id, game_state, record, info=info)

        if record.total_times % record_time == 0 and not game_state.is_special_game:
            if logger is not None:
                record.print_record_data(logger, id(record))

        if record.total_times == target_times and not game_state.is_scatter_game:
            return record


def spin(calculator: SlotCommon.slot_calculator.DefaultSlotCalculator,
         game_id: str, bet_value: float, bet_lines: int,
         game_state: SlotCommon.player_game_state.PlayerGameState,
         record: RecordData, extra_bet=False, info=None):
    extra_info = None
    cp_extra_info = copy.deepcopy(game_state.extra_info)
    result = calculator.spin(bet_value=bet_value, bet_lines=bet_lines, game_state=game_state, game_info=info, gameInfo=info, dev_mode=0,
                             extra_info=cp_extra_info, extra_bet=extra_bet)
    if isinstance(result, tuple):
        result, extra_info = result

    """:type(result): MainGameResult"""
    if extra_info is not None:
        game_state.update_extra_info(extra_info)
    game_state.update_by_spin_result(result, bet_value, bet_lines)

    if result.get_extra_data().get("feature_wheels"):
        record.feature_wild_hit['main'][sum(result.get_extra_data()["feature_wheels"])] += 1

    # ===================
    if game_id == 'RomaPlus':
        record.mgCombolCnt += sum(map(int, result.reel_block_data['0'].end_feature.keys()))
    # ===================

    if result.win_special_game:
        if len(record.main_win_times_list) <= 0:
            record.main_win_times_list[0] = 1
        else:
            record.main_win_times_list[0] += 1
    record.main_spin_record(result, game_state)


def next_fever(calculator: SlotCommon.slot_calculator.DefaultSlotCalculator,
               game_id: str,
               game_state: SlotCommon.player_game_state.PlayerGameState,
               record: RecordData, dev_mode=0, info=None):
    client_action = calculator._build_client_action(game_state, None, PLAYER_SELECTION)

    special_game_id = game_state.current_sg_id
    current_level = game_state.current_special_game_level
    special_game_data = None
    if calculator.is_in_game_jackpot_exist():
        try:
            for i in range(20):
                result = calculator.next_fever(client_action, game_state, game_info=info, gameInfo=info, dev_mode=dev_mode)
                if result.has_error:
                    client_action = calculator._build_client_action(game_state, client_action)
                    continue

                break
        except:
            print(traceback.format_exc())
            print(game_state)
            print(client_action)

        if len(result.in_game_jp_win_list) > 0:
            for _val in result.in_game_jp_win_list:
                if game_id == "FuXingGaoZhaoArk":
                    val = special_game_id * 10 + int(_val)
                elif len(game_state.all_sg_id) > 1:
                    val = 10 + _val
                else:
                    val = _val
                if str(val) not in record.in_game_jp_dic:
                    record.in_game_jp_dic[str(val)] = 1
                else:
                    record.in_game_jp_dic[str(val)] += 1

        if game_state.current_special_game_data['current_script'].get('WinJP', -1) >= 0:
            val = special_game_id * 10 + game_state.current_special_game_data['current_script'].get('WinJP', -1)
            if str(val) not in record.in_game_jp_dic:
                record.in_game_jp_dic[str(val)] = 1
            else:
                record.in_game_jp_dic[str(val)] += 1
    else:
        for i in range(5):
            try:
                result = calculator.next_fever(client_action, game_state, game_info=info, gameInfo=info, dev_mode=dev_mode)
                if result.has_error:
                    calculator._build_client_action(game_state, client_action)
                    continue
                break
            except:
                logger.out("exception={}".format(traceback.format_exc()))

    current_level_win = result.win_amount
    if current_level != 1:
        if result.has_error:
            logger.out("Fever Error:{}".format(result.error))
            raise Exception

    total_win = game_state.update_by_fever_result(result)
    game_state.update_last_fever_reels(special_game_id, *calculator.custom_last_fever_reels(result))  # 更新 recovery 盤面
    record_special_game_id = special_game_id
    if game_id == "FlyToTheMoon" and special_game_id == 1 and game_state.current_special_game_data[
        'current_script'].get('is_fever_trigger'):
        record_special_game_id = 3
    elif game_id == "HotShot":
        record_special_game_id = result.fever_map.get(7)
    elif game_id == 'RomaPlus':
        combol = 0
        if special_game_id == 0 and current_level != 1 and 'feature_wheels' in result.fever_map._map['SpinResult']:
            if 'End' in result.fever_map._map['SpinResult']['feature_wheels']:
                combol = len(result.fever_map._map['SpinResult']['feature_wheels']['End'][0]['Combo'])
        CombolCnt = record.sgCombolCnt.get(str(special_game_id))
        CombolCnt += combol
        record.sgCombolCnt.update({str(special_game_id): CombolCnt})

    if game_id == "SpinOfFate":
        if result.sg_id == 1 and game_state.current_special_game_data['current_script']['trigger_type'] == "Free":
            record_special_game_id = 2
        if 'feature_win_data' in result.record:
            key_list = ['is_post_wild_symbol', 'is_keep_wild', 'SmallWin', 'BigWin', 'JP3', 'JP2', 'JP1',
                        'SmallWin_win', 'BigWin_win', 'JP3_win', 'JP2_win', 'JP1_win', 'feature_upgrade_chance',
                        'feature_upgrade_time_chance']
            special_game_data = result.record["feature_win_data"]
            special_game_data = collections.OrderedDict(
                sorted(special_game_data.items(), key=lambda x: key_list.index(x[0])))
    if result.is_gameover:
        game_state.end_special_game(special_game_id)

        player_selection = None
    bingo_data = None
    record.special_game(record_special_game_id, current_level_win, current_level, bingo_data,
                        special_game_data=special_game_data)
    return game_state


def check_reel_symbol_legal(game_rate, logger):
    logger.out(str(game_rate))
    logger.out("================================")
    action = input("Need to check reel?(y/n)\ndefault: y\nplease input choice:")
    if action == 'n':
        return
    main_reels = game_rate.get("main_reels")
    fever_reels = game_rate.get('fever_reels')
    if len(main_reels) <= 0 or len(fever_reels) <= 0:
        action = input("main_reel or fever_reel is none still continue?(y)")
        if action != 'y':
            sys.exit(1)
    # check_main_reels
    main_strage_symbol = list()
    for reel_info_index in range(len(main_reels)):
        reel_info = main_reels[str(reel_info_index)]
        for reel_index in range(len(reel_info)):
            reel = reel_info[str(reel_index)]
            for symbol_index in range(len(reel)):
                symbol = reel[symbol_index]
                if str(symbol) not in game_rate['odds']:
                    main_strage_symbol.append((symbol, reel_info_index, reel_index, symbol_index))
    # check_fever_reels
    fever_strage_symbol = list()
    for reel_info_index in range(len(fever_reels)):
        reel_info = fever_reels[str(reel_info_index)]
        for reel_index in range(len(reel_info)):
            reel = reel_info[str(reel_index)]
            for symbol_index in range(len(reel)):
                symbol = reel[symbol_index]
                if str(symbol) not in game_rate['odds']:
                    fever_strage_symbol.append((symbol, reel_info_index, reel_index, symbol_index))
    for strage_data in main_strage_symbol:
        logger.out("!!!!!!!!!!!!!!Warning!!!!!!!!!!!!!!!!")
        logger.out(
            "main_reel: symbol={}, reel={}, col={}, pos={}".format(strage_data[0], strage_data[1], strage_data[2],
                                                                   strage_data[3]))
    for strage_data in fever_strage_symbol:
        logger.out("!!!!!!!!!!!!!!Warning!!!!!!!!!!!!!!!!")
        logger.out(
            "fever reel: symbol={}, reel={}, col={}, pos={}".format(strage_data[0], strage_data[1], strage_data[2],
                                                                    strage_data[3]))
    if len(fever_strage_symbol) <= 0 and len(main_strage_symbol) <= 0:
        logger.out("pass")
    logger.out("================================")


def _LoadGameList():
    game_list_setting_file = 'new_fast_test.json'
    game_list_setting_path = os.path.join(os.path.dirname(__file__), game_list_setting_file)
    if not os.path.isfile(game_list_setting_path):
        return '', ['Razor'], ['Razor.Game.Slot']
    with open(game_list_setting_path, 'r') as f:
        game_list_setting = json.load(f)
        PKGPath = game_list_setting['PKGPath']
        GAME_LIST = game_list_setting['GAME_LIST']
        PATH_LIST = game_list_setting['PATH_LIST']
        return PKGPath, GAME_LIST, PATH_LIST


def _CreateCalculator(logger, game_id, setting, game_state=None, calMod=None, randomer=None):
    calculator = getattr(calMod, game_id)(logger, setting)
    calculator.get_init_reel_info(game_state)
    calculator.SetRandomer(randomer)
    return calculator


def _CreateLogger(game_id, bet_value, spin_times, probability_ver=''):
    import time
    ts = str(int(time.time()))
    path = './Log'
    if not os.path.isdir(path):
        print(f"{os.path.abspath(path)} is not exist, create it")
        os.mkdir(path)
    logger = DoubleOutput(
        path + "_".join(["/fast_test", game_id, probability_ver, str(bet_value), str(spin_times), ts]) + ".txt",
        path + "_".join([path, "/fast_test", game_id, probability_ver, str(bet_value), str(spin_times), ts, "ERR.txt"])
    )
    logger.log_level = LOG_LEVEL
    logger.out(probability_ver)
    return logger


def trans_input(title, option_list):
    msg = ""
    for k, v in enumerate(option_list):
        msg += "[{}]{} ".format(k, v)

    i = input(title + msg)
    if not i.isdigit() or int(i) >= len(option_list):
        print("option is not exist !")
        return None
    return option_list[int(i)]


if __name__ == "__main__":
    # for game_id in GAME_ID_TO_API_GAME_ID_MAP:
    PKGPath, GAME_LIST, PATH_LIST = _LoadGameList()
    sys.path.extend([os.path.join(PKGPath, 'SlotCommon')])
    # marketList = ['MY', 'TH', 'MM', 'VN', 'DEFAULT']
    # market = trans_input("MARKET:", marketList)
    # if market == 'DEFAULT':
    #     market = ''

    gameIdx = 0
    game_id = GAME_LIST[gameIdx]
    game_path = PATH_LIST[gameIdx]
    print(["{}:{}".format(i, name) for i, name in enumerate(GAME_LIST)])
    # gameIdx = input("Game Name:")
    gameIdx = 0
    if type(gameIdx) == int or gameIdx.isdigit():
        gameIdx = int(gameIdx)
        if 0 <= gameIdx < len(GAME_LIST):
            game_id = GAME_LIST[gameIdx]
            game_path = PATH_LIST[gameIdx]

    print('GameName:', game_id)

    bet = float(input("BetValue:"))
    # extra_bet = input("Activity ExtraBet?(Y/N) Default:N")
    # extra_bet = True if extra_bet in ["Y", "y"] else False
    extra_bet = False

    probId = input("ProbId[A]:")
    probId = 'A' if probId == '' else probId

    # wirteCsv = input("WriteCsv?(Y/N) Default:N")
    # wirteCsv = True if wirteCsv in ["Y", "y"] else False
    wirteCsv = True

    # process_num = input("ProcessNum[10]:")
    # process_num = 10 if process_num == '' else int(process_num)
    process_num = TEST_PROCESS_NUM

    spin_times = input("SpinTimes[100,000,000]:")
    spin_times = 100000000 if spin_times == '' else int(spin_times)
    record_times = 1000000

    import_path = f'{game_path}.{game_id}.{game_id}'
    # info = importlib.import_module(f'{import_path}Info{market}').GameInfo
    info = importlib.import_module(f'{import_path}Info').GameInfo
    info = {i["ProbId"]: i for i in info}
    info = info[probId]
    # setting = importlib.import_module(f'{import_path}Setting{market}').Setting
    setting = importlib.import_module(f'{import_path}Setting').Setting
    info.update(setting)
    logger = _CreateLogger(game_id, bet, spin_times, probId)

    calMod = importlib.import_module(f'{import_path}')
    # game_calculator = _CreateCalculator(logger, game_id, setting, game_state=PlayerGameState(None), calMod=calMod, randomer=IgsRandomer())
    fast_test = FastTest(logger, game_id, bet, extra_bet, setting, None, probId)
    # game_calculator.SetRandomer(fast_test.randomer)

    for bet in [bet]:  # 0.01, 0.02, 0.05, 0.1, 0.2, 0.25, 0.5, 1, 1.5, 2, 2.5, 5
        # for _game_id in [game_id]:
        try:
            fast_test.main(None, game_id,
                           test_spin_time=spin_times,
                           record_time=record_times,
                           process_num=process_num,
                           extra_bet=extra_bet,
                           info=info,
                           wirteCsv=wirteCsv)
        except:
            print(traceback.format_exc())
