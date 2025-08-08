#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
# import random
import copy

from .game_check import MainGameCheck
from .slot_status_code import *
from .main_line_data import *


# 做為基本線獎規則讓各遊戲繼承
class MainGameCheckLine(MainGameCheck):
    def main_win_check(self, main_result, block_id, play_info, odds, check_reel, show_reel, check_reel_length,
                       check_reel_amount, is_two_way=False, wild_multiplier=(), all_multiplier=1, line_type="Common", line_data=None):
        """
        檢查線獎的贏分；scatter的贏分在scatter的check給
        :type main_result: MainGameResultEX
        :param is_two_way: 是不是雙邊對獎，預設單邊
        """
        check_line_data = self.get_check_line_data(check_reel_length, check_reel_amount, line_type,line_data)

        total_win = 0
        reel_info = main_result.get_reel_block_data(block_id)
        for line_id in range(0, play_info.winnable_lines):
            total_win += self.check_line_win(reel_info, play_info, odds, check_reel, show_reel, line_id,
                                             check_line_data[line_id], check_reel_length, check_reel_amount,
                                             is_from_left=True, skip_five_symbol=False, wild_multiplier=wild_multiplier, all_multiplier=all_multiplier)
            if is_two_way:
                total_win += self.check_line_win(reel_info, play_info, odds, check_reel, show_reel, line_id,
                                                 check_line_data[line_id], check_reel_length, check_reel_amount,
                                                 is_from_left=False, skip_five_symbol=True, wild_multiplier=wild_multiplier, all_multiplier=all_multiplier)

        main_result.this_win += total_win


    # =======================================Win Check============================================
    def get_check_line_data(self, check_reel_length, check_reel_amount, line_type="Common", line_data=None):
        if line_data is None:
            return LineDataEX[line_type][str(check_reel_amount)][str(check_reel_length)]
        return line_data

    # 計算贏倍 (skip_five_symbol=True代表要略過5連線的贏分)
    def check_line_win(self, reel_info, play_info, odds, check_reel, show_reel, line_id, line_data, check_reel_length,
                       check_reel_amount, is_from_left=True, skip_five_symbol=False, wild_multiplier=(), all_multiplier=1):
        """
        計算線獎贏分
        :type reel_info: MainReelInfo
        """
        WildSymbolIdList = self.WildSymbolIdList
        fever_symbol_id = self.FeverSymbolID
        special_symbol_id_list = [fever_symbol_id]

        if is_from_left:
            check_reel_list = range(0, check_reel_amount)
        else:
            check_reel_list = range(check_reel_amount - 1, -1, -1)

        check_symbol = check_reel[check_reel_list[0]][line_data[check_reel_list[0]]]
        line_symbol_count = 0
        line_wild_count = 0
        for col in check_reel_list:
            current_symbol_id = check_reel[col][line_data[col]]
            if current_symbol_id in special_symbol_id_list:
                break
            # wild帶頭，且還沒有被其他symbol中斷的時候
            if check_symbol in WildSymbolIdList:
                if current_symbol_id in WildSymbolIdList:
                    line_wild_count += 1
                    line_symbol_count += 1
                else:
                    line_symbol_count += 1
                    check_symbol = current_symbol_id
            elif current_symbol_id == check_symbol or current_symbol_id in WildSymbolIdList:
                line_symbol_count += 1
            else:
                break

        wild_win = 0  # wild連線的贏分
        symbol_win = 0  # 一般symbol連線的贏分
        if line_wild_count > 0:
            wild_win = odds[str(self.WildSymbolID)][line_wild_count-1]
            wild_win *= play_info.line_bet
        if line_symbol_count > 0:
            symbol_win = odds[str(check_symbol)][line_symbol_count-1]
            symbol_win *= play_info.line_bet

        extra_multiplier = 1
        if symbol_win>0 and len(wild_multiplier)>0:
            for reel_idx in range(check_reel_amount):
                extra_multiplier *= wild_multiplier[reel_idx][line_data[reel_idx]]
            symbol_win *= extra_multiplier

        current_win = 0  # 最終的贏分
        current_win_symbol = 0  # 最終的連線的symbol
        current_line_symbol_count = 0  # 最終的連線數
        if symbol_win >= wild_win:
            current_win = symbol_win
            current_win_symbol = check_symbol
            current_line_symbol_count = line_symbol_count
        else:
            current_win = wild_win
            current_win_symbol = self.WildSymbolID
            current_line_symbol_count = line_wild_count

        if current_win > 0:
            if all_multiplier > 1:
                current_win *= all_multiplier
                extra_multiplier *= all_multiplier

            # ====用來處理雙邊對獎從右邊對過來5連線不重覆計算贏分
            if skip_five_symbol and current_line_symbol_count == len(line_data):
                current_win = 0
            #===================================================
            else:
                reel_info.set_bingo_type(SLOT_TYPE.LINES)
                reel_info.set_bingo_line(line_id, current_win_symbol, line_data, current_line_symbol_count,
                                         check_reel_length, check_reel_amount, current_win, is_from_left=is_from_left, multiplier=extra_multiplier)

        return current_win
