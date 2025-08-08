#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
# import random
import copy

from .game_check import MainGameCheck
from .slot_status_code import *

# 做為基本規則讓其他function繼承
class MainGameCheckWays(MainGameCheck):
    def main_win_check(self, main_result, block_id, play_info, odds, check_reel, show_reel, check_reel_length, check_reel_amount, is_two_way=False):
        """
        檢查AllWays贏分；scatter的贏分在scatter的check給
       :type main_result: MainGameResultEX
        :param is_two_way: 是不是雙邊對獎，預設單邊，通常AllWays遊戲不會雙邊對獎
        """
        total_win = 0
        reel_info = main_result.get_reel_block_data(block_id)
        reel_info.set_bingo_type(SLOT_TYPE.WAYS)
        # 將第一輪和最後一輪的symbol抓出來，要從左到右或從右到左對獎使用
        from_left_check_symbol_list = list()
        from_right_check_symbol_list = list()
        for symbol in check_reel[0]:
            if symbol not in from_left_check_symbol_list:
                from_left_check_symbol_list.append(symbol)
        for symbol in check_reel[len(check_reel)-1]:
            if symbol not in from_left_check_symbol_list:
                from_right_check_symbol_list.append(symbol)

        # all-ways bingo 資訊的line id沒有意義，就是從0開始增加
        fake_line_id = 0
        for check_symbol_id in from_left_check_symbol_list:
            total_win += self.check_way_win(reel_info, play_info, odds, check_reel, show_reel, fake_line_id,
                                            check_symbol_id, is_from_left=True, skip_five_symbol=False)
            fake_line_id += 1
        if is_two_way:
            for check_symbol_id in from_right_check_symbol_list:
                total_win += self.check_way_win(reel_info, play_info, odds, check_reel, show_reel, fake_line_id,
                                                check_symbol_id, is_from_left=False, skip_five_symbol=True)
                fake_line_id += 1

        main_result.this_win += total_win

    # 計算贏倍 (skip_five_symbol=True代表要略過5連線的贏分)
    # All-ways對獎先不考慮wild帶頭的狀況，基本上不會有這種規格
    # 計獎部分包括連續出現的scatter的贏分
    def check_way_win(self, reel_info, play_info, odds, check_reel, show_reel, line_id, check_symbol_id, is_from_left=True, skip_five_symbol=False):
        wild_symbol_id = self.WildSymbolID
        fever_symbol_id = self.FeverSymbolID
        special_symbol_id_list = [fever_symbol_id]

        if check_symbol_id in special_symbol_id_list:
            return 0

        if is_from_left:
            check_reel_list = range(0, len(check_reel))
        else:
            check_reel_list = range(len(check_reel) - 1, -1, -1)

        line_symbol_count = 0  # 幾連線
        ways_count = 0  # 有多少ways
        win_symbol_pos = [[-1 for row in range(len(check_reel[col]))] for col in range(len(check_reel))]
        for col in check_reel_list:
            current_col_check_symbol_count = 0  # 當前這輪和check symbol一樣的symbol的數量
            for row in range(len(check_reel[col])):
                current_symbol = check_reel[col][row]
                if current_symbol in [check_symbol_id, wild_symbol_id]:
                    current_col_check_symbol_count += 1
                    win_symbol_pos[col][row] = 1
            # 這輪有check symbol
            if current_col_check_symbol_count > 0:
                line_symbol_count += 1
                # ways_count = 0 表示這是第一個輪檢查的，要直接set
                if ways_count == 0:
                    ways_count = current_col_check_symbol_count
                else:
                    ways_count *= current_col_check_symbol_count
            # 沒有找到check symbol，就不用繼續對下一輪
            else:
                break

        symbol_win = odds[str(check_symbol_id)][line_symbol_count-1]
        symbol_win *= ways_count
        symbol_win *= play_info.line_bet

        if symbol_win > 0:
            reel_info.set_bingo_info(check_symbol_id, win_symbol_pos, line_symbol_count, ways_count, symbol_win)

        return symbol_win

