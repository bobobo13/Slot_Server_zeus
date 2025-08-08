#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .slot_status_code import *
from .main_game_result import *


class MainGameChance(object):
    def __init__(self, **kwargs):
        self.randomer = kwargs.get('randomer')

    def init_game_reel_info(self, reel_amount, reel_length, check_reel_amount, check_reel_length):
        self.reel_amount = reel_amount
        self.reel_length = reel_length

        self.check_reel_amount = check_reel_amount
        self.check_reel_length = check_reel_length
        self.invisible_symbols = self.reel_length - self.check_reel_length

    def get_init_reel(self, reel_data, reel_length, reel_amount):
        main_result = MainGameResult([0])
        reel_info = main_result.get_reel_block_data(0)
        self._get_spin_result_from_reel(reel_info, reel_data[str(0)], reel_length, reel_amount)
        return reel_info.reel_data

    def get_spin_result(self, main_result, block_id, reel_data, reel_length, reel_amount, check_reel_length, check_reel_amount, dev_mode=DevMode.NONE, is_weight=False, spin_mode="normal", **kwargs):
        """
        取得牌面
        :param main_result: 所有chance、check產出的結果
        :type main_result: MainGameResultEX
        :param reel_data: 轉輪帶or權重的資訊
        :param reel_length: 包含不可視範圍的轉輪長度
        :param reel_amount: 包含不可視範圍的轉輪個數
        :param check_reel_length: 不包含不可視範圍的轉輪長度
        :param check_reel_amount: 不包含不可視範圍的轉輪個數
        :param dev_mode: 開發模式，指定牌面用
        :param is_weight: 是否為權重取牌方式
        :param spin_mode: 用來標記是在 spin 或 next_fever 呼叫
        :return:
        """
        reel_info = main_result.get_reel_block_data(block_id)
        if dev_mode == DevMode.ITEM_TRIGGER_SPECIAL_GAME:
            self._get_item_trigger_spin_special_game_result(reel_info, reel_data)
        elif isinstance(dev_mode, dict):
            self._get_pre_spin_result(reel_info, reel_amount, dev_mode)
        elif isinstance(dev_mode, str):
            self._get_custom_dev_spin_result(reel_info, dev_mode)
        elif dev_mode != DevMode.NONE and spin_mode == "normal":
            # 取測試盤面
            # print 'dev mode='+str(dev_mode)
            self._get_dev_spin_result(reel_info, dev_mode)
        elif dev_mode != DevMode.NONE and spin_mode == "fever":
            self._get_dev_fever_result(reel_info, reel_data, reel_length, reel_amount, dev_mode, **kwargs)
        elif "PreCreate" in reel_data:
            data = reel_data["PreCreate"].pop(0)
        elif is_weight:
            # 權重取牌面的邏輯
            self._get_spin_result_from_weight(reel_info, reel_data, reel_length, reel_amount, **kwargs)
        else:
            # 轉輪帶取牌面的邏輯
            self._get_spin_result_from_reel(reel_info, reel_data, reel_length, reel_amount)

    def set_reel_info_from_look(self, reel_info, look_list, default_symbol,
                                check_reel_length=0, check_reel_amount=0, reel_length=0, reel_amount=0, transform=False):
        if check_reel_length == 0:
            check_reel_length = self.check_reel_length
        if check_reel_amount == 0:
            check_reel_amount = self.check_reel_amount
        if reel_length == 0:
            reel_length = self.reel_length
        if reel_amount == 0:
            reel_amount = self.reel_amount

        for idx in range(check_reel_length):
            look_str = ""
            for idx2 in range(check_reel_amount):
                look_str += str(look_list[idx * check_reel_amount + idx2]) + '\t'

        show_reel = [[0 for elt in range(check_reel_length)] for col in range(check_reel_amount)]
        index = 0
        for symbol in look_list:
            x = index % check_reel_amount
            y = index // check_reel_amount
            show_reel[x][y] = symbol
            index += 1

        if transform:
            show_elt_index_from_reel_length = reel_length / 2
            for reel_index in range(reel_amount):
                reel_data = []
                for elt_index in range(reel_length):
                    if elt_index == show_elt_index_from_reel_length:
                        show_reel_index = reel_index / check_reel_amount
                        show_elt_index = reel_index % check_reel_amount
                        reel_data.append(show_reel[show_reel_index][show_elt_index])
                    else:
                        reel_data.append(default_symbol)
                reel_info.set_one_reel_data(reel_index, reel_data)
        else:
            show_elt_start = (reel_length - check_reel_length) // 2
            show_elt_end = reel_length - show_elt_start - 1
            for reel_index in range(reel_amount):
                reel_data = []
                for elt_index in range(reel_length):
                    if show_elt_start <= elt_index <= show_elt_end:
                        reel_data.append(show_reel[reel_index][elt_index - show_elt_start])
                    else:
                        reel_data.append(default_symbol)
                reel_info.set_one_reel_data(reel_index, reel_data)

    def set_all_target_symbol(self, init_reel_length, max_symbol_id, reel_amount, reel_info, target_symbol):
        if target_symbol > max_symbol_id:
            target_symbol = max_symbol_id
        for reel_index in range(0, reel_amount):
            reel_data = []
            for slot_index in range(0, init_reel_length):
                reel_data.append(target_symbol)
            reel_info.set_one_reel_data(reel_index, reel_data)

    def get_double_game_result(self, game_info, special_game_state, client_action, dev_mode=DevMode.NONE):
        return {}

    def _get_spin_result_from_reel(self, reel_info, all_reel_data, reel_length, reel_amount):
        """
        透過轉輪帶取得spin牌面
        :param reel_info: 一個轉輪區中的資訊
        :type reel_info: MainReelInfo
        :param all_reel_data: 轉輪帶的資料
        :type all_reel_data: dict[str, list[int]]
        """
        for col in range(reel_amount):
            temp_show_reel = list()
            now_reel_data = all_reel_data[str(col)]
            now_reel_data_max_index = len(now_reel_data) - 1
            random_index = self.randomer.randint(0, now_reel_data_max_index)
            for index in range(random_index, random_index + reel_length):
                while index > now_reel_data_max_index:
                    index -= (now_reel_data_max_index + 1)
                temp_show_reel.append(now_reel_data[index])
            reel_info.set_one_reel_data(col, temp_show_reel)

    def _get_spin_result_from_weight(self, reel_info, weight_data, reel_length, reel_amount, **kwargs):
        """
        透過權重取得盤面
        如果是15輪的slot，reel_length=3, reel_amount=15
        :param reel_info:
        :type reel_info: MainReelInfo
        :param weight_data:
        """
        for reel in range(reel_amount):
            temp_show_reel = list()
            total_weight = 0
            for info in weight_data[str(reel)]:
                total_weight += info[1]
            for symbol in range(0, reel_length):
                rand_num = self.randomer.randint(1, total_weight)
                for info in weight_data[str(reel)]:
                    rand_num -= info[1]
                    if rand_num <= 0:
                        temp_show_reel.append(info[0])
                        break
            reel_info.set_one_reel_data(reel, temp_show_reel)

    def _set_reel_info_from_look(self, reel_info, look_list):
        """
        :param reel_info:
        :param look_list: symbol由左至右 由上而下的排列法
        ex. [ 1, 2, 3, 4, 5
               6, 7, 8, 9,10
              11,12,13,14,15 ]
        會轉變成
        reel[0] = [1,6,11]
        reel[1] = [2,7,12]
        reel[2] = [3,8,13]
        reel[3] = [4,9,14]
        reel[4] = [5,10,15]
        """
        reel_amount = self.reel_amount
        reel_length = self.reel_length
        check_reel_amount = self.check_reel_amount
        check_reel_length = self.check_reel_length
        invisible_symbols = self.invisible_symbols

        # for idx in range(check_reel_length):
        #     print look_list[idx*check_reel_amount+0], '\t', look_list[idx*check_reel_amount+1], '\t', look_list[idx*check_reel_amount+2], '\t', look_list[idx*check_reel_amount+3], '\t', look_list[idx*check_reel_amount+4]
        show_reel = [[0 for elt in range(check_reel_length)] for col in range(check_reel_amount)]
        index = 0
        for symbol in look_list:
            x = index % check_reel_amount
            y = index / check_reel_amount
            show_reel[x][y] = symbol
            index += 1
        # print show_reel

        for reel_index in range(reel_amount):
            reel_data = []
            for elt_index in range(reel_length):
                if invisible_symbols / 2 <= elt_index < reel_length - invisible_symbols / 2:
                    reel_data.append(show_reel[reel_index][elt_index - invisible_symbols / 2])
                else:
                    reel_data.append(10)
            reel_info.set_one_reel_data(reel_index, reel_data)

    def _get_dev_spin_result(self, reel_info, dev_mode):
        """
        測試用直接指定牌面
        :param reel_info:
        :return:
        """
        if dev_mode == DevMode.SUPER_WIN:
            reel_info.set_one_reel_data(0, [10, 10, 10, 10, 10])
            reel_info.set_one_reel_data(1, [10, 10, 10, 10, 10])
            reel_info.set_one_reel_data(2, [10, 10, 10, 10, 10])
            reel_info.set_one_reel_data(3, [10, 10, 10, 10, 10])
            reel_info.set_one_reel_data(4, [10, 10, 10, 10, 10])
        elif dev_mode == DevMode.FREE_SPIN:
            reel_info.set_one_reel_data(0, [10, 10, 2, 10, 10])
            reel_info.set_one_reel_data(1, [10, 10, 2, 10, 10])
            reel_info.set_one_reel_data(2, [10, 10, 2, 10, 10])
            reel_info.set_one_reel_data(3, [10, 10, 10, 10, 10])
            reel_info.set_one_reel_data(4, [10, 10, 10, 10, 10])
        elif dev_mode == DevMode.FEVER:
            reel_info.set_one_reel_data(0, [10, 10, 10, 10, 10])
            reel_info.set_one_reel_data(1, [10, 10, 1, 10, 10])
            reel_info.set_one_reel_data(2, [10, 10, 1, 10, 10])
            reel_info.set_one_reel_data(3, [10, 10, 10, 10, 10])
            reel_info.set_one_reel_data(4, [10, 10, 10, 10, 10])

    def _get_dev_fever_result(self, reel_info, reel_data, reel_length, reel_amount, dev_mode=DevMode.NONE, **kwargs):
        """
        測試用直接指定牌面
        :param reel_info:
        :return:
        """
        # 預設轉輪帶取牌面的邏輯
        self._get_spin_result_from_reel(reel_info, reel_data, reel_length, reel_amount)

    def _get_pre_spin_result(self, reel_info, reel_amount, dev_mode):
        """
        使用已預先計算之spin結果
        :param reel_info:
        :param dev_mode:
        :return:
        """
        for reel_index in range(reel_amount):
            reel_info.set_one_reel_data(reel_index, dev_mode[str(reel_index)])

    def _get_custom_dev_spin_result(self, reel_info, dev_mode):
        """
        測試用直接指定牌面
        :param reel_info:
        :return:
        """
        if len(dev_mode) == self.check_reel_amount * self.check_reel_length:
            show_elt_start = (self.reel_length - self.check_reel_length) / 2
            show_elt_end = self.reel_length - show_elt_start - 1
            for reel_index in range(0, self.reel_amount):
                reel_data = []
                for slot_index in range(0, self.reel_length):
                    if show_elt_start <= slot_index <= show_elt_end:
                        symbol = dev_mode[reel_index * self.check_reel_length + slot_index - show_elt_start]
                        if symbol.isalpha():
                            reel_data.append(ord(symbol.lower()) - ord("a") + 10)
                        elif symbol.isdigit():
                            reel_data.append(ord(symbol) - ord("0"))
                        else:
                            reel_data.append(self.randomer.randint(10, 12))
                    else:
                        reel_data.append(self.randomer.randint(10, 12))
                reel_info.set_one_reel_data(reel_index, reel_data)
        else:
            for reel_index in range(0, self.reel_amount):
                reel_data = []
                for slot_index in range(0, self.reel_length):
                    reel_data.append(self.randomer.randint(10, 11))
                reel_info.set_one_reel_data(reel_index, reel_data)

    def _get_item_trigger_spin_special_game_result(self, reel_info, reel_data):
        """
        (道具卡)產生一個必中fever game的spin結果，依遊戲不同需複寫
        :param reel_info:
        :param reel_data:
        :return:
        """
        scatter_count = 3
        wild_id = 1
        scatter_id = 2
        special_symbols = {wild_id, scatter_id}
        symbol_set = set(reel_data[str(0)])
        symbol_set = symbol_set - special_symbols
        # 部分輪數不可能出現scatter的遊戲，或是需連續出現scatter的遊戲，override時寫死target_reels
        # target_reels = self.randomer.sample([0, 1, 2, 3, 4], scatter_count)
        target_reels = [0, 1, 2]

        symbol_begin_pos = (self.reel_length - self.check_reel_length) / 2
        symbol_end_pos = symbol_begin_pos + self.check_reel_length - 1
        both_first_two_reels_symbol_set = set()

        for reel_index in range(0, self.reel_amount):
            reel_data = []
            if reel_index == 2:
                # 確保第三輪不會有和前兩輪連線
                both_first_two_reels_symbol_set = set(reel_info.reel_data["0"][symbol_begin_pos:symbol_end_pos + 1]) & set(reel_info.reel_data["1"][symbol_begin_pos:symbol_end_pos + 1])
            for row in range(0, self.reel_length):
                show_reel_data = set(reel_data[symbol_begin_pos:])
                avail_symbol_set = symbol_set if not symbol_begin_pos <= row <= symbol_end_pos else \
                    symbol_set - set(show_reel_data) if reel_index != 2 else \
                        symbol_set - (set(show_reel_data) | both_first_two_reels_symbol_set)

                symbol_id = self.randomer.choice(list(avail_symbol_set))
                reel_data.append(symbol_id)
            reel_info.set_one_reel_data(reel_index, reel_data)
        for reel_index in target_reels:
            slot_index = self.randomer.randint(symbol_begin_pos, symbol_end_pos)
            reel_info.reel_data[str(reel_index)][slot_index] = scatter_id  # scatter symbol

    def get_bonus_result(self, main_result, block_id, reel_data, reel_length, reel_amount, check_reel_length, check_reel_amount, is_weight=False, spin_mode="normal", **kwargs):
        reel_info = main_result.get_reel_block_data(block_id)
        self._get_buy_bonus_spin_result(reel_info, reel_data, reel_length, reel_amount, dev_mode=DevMode.NONE, **kwargs)
        return main_result

    def _get_buy_bonus_spin_result(self, reel_info, reel_data, reel_length, reel_amount, dev_mode, **kwargs):
        """
        (道具卡)產生一個必中fever game的spin結果，依遊戲不同需複寫
        :param reel_info:
        :param reel_data:
        :return:
        """
        scatter_count = 3
        wild_id = 1
        scatter_id = 2
        special_symbols = {wild_id, scatter_id}
        symbol_set = set(reel_data[str(0)])
        symbol_set = symbol_set - special_symbols
        # 部分輪數不可能出現scatter的遊戲，或是需連續出現scatter的遊戲，override時寫死target_reels
        # target_reels = self.randomer.sample([0, 1, 2, 3, 4], scatter_count)
        target_reels = [0, 1, 2]

        symbol_begin_pos = (self.reel_length - self.check_reel_length) / 2
        symbol_end_pos = symbol_begin_pos + self.check_reel_length - 1
        both_first_two_reels_symbol_set = set()

        for reel_index in range(0, self.reel_amount):
            reel_data = []
            if reel_index == 2:
                # 確保第三輪不會有和前兩輪連線
                both_first_two_reels_symbol_set = set(reel_info.reel_data["0"][symbol_begin_pos:symbol_end_pos + 1]) & set(reel_info.reel_data["1"][symbol_begin_pos:symbol_end_pos + 1])
            for row in range(0, self.reel_length):
                show_reel_data = set(reel_data[symbol_begin_pos:])
                avail_symbol_set = symbol_set if not symbol_begin_pos <= row <= symbol_end_pos else \
                    symbol_set - set(show_reel_data) if reel_index != 2 else \
                        symbol_set - (set(show_reel_data) | both_first_two_reels_symbol_set)

                symbol_id = self.randomer.sample(avail_symbol_set, 1)[0]
                reel_data.append(symbol_id)
            reel_info.set_one_reel_data(reel_index, reel_data)
        for reel_index in target_reels:
            slot_index = self.randomer.randint(symbol_begin_pos, symbol_end_pos)
            reel_info.reel_data[str(reel_index)][slot_index] = scatter_id  # scatter symbol

    def test_get_spin_result_from_reel_assigned_index(self, index_data, reel_data, main_result, block_id, reel_length, reel_amount, check_reel_length, check_reel_amount, dev_mode=DevMode.NONE):
        reel_info = main_result.get_reel_block_data(block_id)
        for col in range(reel_amount):
            temp_show_reel = list()
            now_reel_data = reel_data[str(col)]
            now_reel_data_max_index = len(now_reel_data) - 1
            random_index = index_data[col]
            for index in range(random_index, random_index + reel_length):
                while index > now_reel_data_max_index:
                    index -= (now_reel_data_max_index + 1)
                temp_show_reel.append(now_reel_data[index])
            reel_info.set_one_reel_data(col, temp_show_reel)