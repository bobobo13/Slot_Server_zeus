#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
# import random
import copy

from .game_check import MainGameCheck
from .slot_status_code import *
from .Util.MathTool import *

# 做為基本規則讓其他function繼承
class MainGameCheckCount(MainGameCheck):

    def _init_const(self):
        # id對照表
        self.EmptySymbolID = 0
        self.FeverSymbolID = 2
        # 判斷pre_win及是否觸發special game用
        self.FeverLimitCount = 3
        # special game id
        self.FeverID = 1

    def main_win_check(self, main_result, block_id, play_info, odds, check_reel, show_reel, check_reel_length, check_reel_amount, base_bet=1, multipler=1):
        """
        檢查圖騰對獎類型贏分；scatter的贏分在scatter的check給
       :type main_result: MainGameResultEX
        """
        total_win = 0
        reel_info = main_result.get_reel_block_data(block_id)
        reel_info.set_bingo_type(SLOT_TYPE.COUNTS)

        check_symbol_list = list()

        for reel in check_reel:
            for symbol in reel:
                if symbol not in check_symbol_list:
                    check_symbol_list.append(symbol)

        # symbol count bingo 資訊的line id沒有意義
        for check_symbol_id in check_symbol_list:
            total_win += self.check_count_win(reel_info, play_info, odds, check_reel, show_reel, check_symbol_id, multipler=multipler, base_bet=base_bet)

        main_result.this_win += total_win


    def check_count_win(self, reel_info, play_info, odds, check_reel, show_reel, check_symbol_id, multipler=1, base_bet=1):
        empty_symbol_id = self.EmptySymbolID
        fever_symbol_id = self.FeverSymbolID
        special_symbol_id_list = [empty_symbol_id, fever_symbol_id]

        if check_symbol_id in special_symbol_id_list:
            return 0

        check_reel_list = range(0, len(check_reel))

        check_symbol_count = 0  # 幾顆symbol
        win_symbol_pos = [[-1 for row in range(len(check_reel[col]))] for col in range(len(check_reel))]
        for col in check_reel_list:
            for row in range(len(check_reel[col])):
                current_symbol = check_reel[col][row]
                if current_symbol in [check_symbol_id]:
                    check_symbol_count += 1
                    win_symbol_pos[col][row] = 1

        # 單顆對獎式有設基準押注賠率表，以基準押注的比值計算贏分
        odds_multipler = float(play_info.line_bet) / float(base_bet)

        # 如果有重複圖騰累進賠率，計算對應賠率表顆數，超過賠率表最大顆數以最大顆數計
        odds_symbol_count = check_symbol_count if check_symbol_count < len(odds[str(check_symbol_id)]) else len(odds[str(check_symbol_id)])-1
        symbol_win = odds[str(check_symbol_id)][odds_symbol_count] * odds_multipler
        symbol_win *= multipler

        if symbol_win > 0:
            symbol_win = floor_float(symbol_win, 3)
            reel_info.set_bingo_count_info(check_symbol_id, win_symbol_pos, check_symbol_count, symbol_win, multiplier=multipler)

        return symbol_win

    # =======================================檢查特殊symbol============================================
    # 要將free game和 special game的拆成兩個function
    def special_symbol_check(self, main_result, block_id, play_info, odds, special_odds, check_symbol_id, check_symbol_limit, special_game_id,
                             check_reel, show_reel, check_reel_length, check_reel_amount, is_pass_line=False):
        """
        檢查特殊symbol的中獎，包括贏得的次數和倍數
        次數設定在special odd中，倍數設定在odds中
        """
        reel_info = main_result.get_reel_block_data(block_id)
        symbol_count = 0
        symbol_line_count = 0
        symbol_pos = [[-1 for row in range(len(check_reel[col]))] for col in range(len(check_reel))]
        for col in range(len(check_reel)):
            current_col_symbol_count = 0
            for row in range(len(check_reel[col])):
                current_symbol = check_reel[col][row]
                if current_symbol == check_symbol_id:
                    current_col_symbol_count += 1
                    symbol_pos[col][row] = 1
            if current_col_symbol_count > 0:
                symbol_count += current_col_symbol_count
                symbol_line_count += 1
            # 如果scatter一定要連著出現才算，只要檢查到有一輪沒有就不檢查後面了
            elif is_pass_line:
                break
        # TODO 修改成符合SpecialGame架構
        # FreeSpin已被移除，現在只有用到fever game
        if symbol_count >= check_symbol_limit:
            result_win_times = 0
            key = "fever" if not play_info.is_special_game else "fever_again"
            win_times = special_odds[key][symbol_count-1]
            # win_odds = odds[str(check_symbol_id)][0]
            # win_odds *= play_info.total_bet
            # win_odds = floor_float(win_odds, 3)
            # self.scatter_win = win_odds
            #print '-------game check scatter_win='+str(self.scatter_win)
            reel_info.set_special_symbol_win_pos(special_game_id, symbol_pos)
            main_result.set_win_special_game(special_game_id, win_times)
            if not play_info.is_special_game:
                current_script = {
                }
                self.set_win_special_symbol_info(main_result, block_id, special_game_id, check_symbol_id, symbol_pos, win_times, current_script)
            # if win_odds > 0:
            #     main_result.this_win += win_odds