#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'shihpochien'

from .Util import MathTool as MathTool
from .JsonSerializable import JsonSerializable


class MainReelInfo(JsonSerializable):
    def __init__(self, *args, **kwargs):
        self.reel_data = kwargs.get("reel_data", dict())  # reel_data[str(reel_index)][list(int(reel_symbols))]，包含不可視的symbols
        self.special_symbol_pos = kwargs.get("special_symbol_pos", dict())  # special_symbol_pos[str(symbol_id)][str(reel_index)][list(int(reel_symbols))]，只有看的到的symbols
        self.end_feature = kwargs.get("end_feature", dict())  # end_feature[str(key)][any_type(value)]
        self.rot_feature = kwargs.get("rot_feature", dict())  # rot_feature[str(key)][any_type(value)]
        self.pre_win = kwargs.get("pre_win", list())  # 每一輪是否要聽牌 0:不聽牌 1:聽牌
        self.bingo = kwargs.get("bingo", list())  # 中獎線資料{"line_id":int(id), "symbol_id":int(id), "pos":2d-list(只有看的到的symbols)}
        self.bingo_type = kwargs.get("bingo_type", 0)  # (int) 0:lines 1:all-ways 2:count

        self.win_amount = kwargs.get("win_amount", 0)  # (int)這組reel的贏分

    @property
    def has_rot_feature(self):
        return len(self.rot_feature) > 0

    @property
    def has_end_feature(self):
        return len(self.end_feature) > 0

    @property
    def has_pre_win(self):
        return len(self.pre_win) > 0

    @property
    def has_bingo(self):
        return len(self.bingo) > 0

    def replace_symbol_id(self, ori_symbol_id, new_symbol_id):
        for key in self.reel_data.keys():
            for i in range(len(self.reel_data[key])):
                if self.reel_data[key][i] == ori_symbol_id:
                    self.reel_data[key][i] = new_symbol_id
        for bingo_data in self.bingo:
            if bingo_data['symbol_id'] == ori_symbol_id:
                bingo_data['symbol_id'] = new_symbol_id
        # print 'self.bingo: ', self.bingo

    def get_rot_feature_in_list(self):
        ret = list()
        for key, value in self.rot_feature.items():
            ret.append({"key": key, "value": value})
        return ret

    def get_rot_feature_in_list_extra(self):
        ret = list()
        for key, value in self.rot_feature.items():
            ret.append({key: value})
        return ret

    def get_end_feature_in_list(self):
        ret = list()
        for key, value in self.end_feature.items():
            ret.append({"key": key,"value":value})
        return sorted(ret, key=lambda x: x['key'])

    def get_end_feature_in_list_extra(self):
        ret = list()
        for key, value in self.end_feature.items():
            ret.append({key: value})
        return ret

    def set_one_reel_data(self, reel_index, one_reel_data):
        self.reel_data[str(reel_index)] = one_reel_data
        
    def get_one_reel_data(self, reel_index):
        return self.reel_data[str(reel_index)]
    
    def set_special_symbol_win_pos(self, special_game_id, special_symbol_win_pos):
        if type(special_symbol_win_pos) == dict:
            self.special_symbol_pos[str(special_game_id)] = special_symbol_win_pos
        elif type(special_symbol_win_pos) == list:
            self.special_symbol_pos[str(special_game_id)] = dict()
            reel_index = 0
            for col_info in special_symbol_win_pos:
                self.special_symbol_pos[str(special_game_id)][str(reel_index)] = col_info
                reel_index += 1

    def set_end_feature(self, key, value):
        self.end_feature[str(key)] = value

    def set_rot_feature(self, key, value):
        self.rot_feature[str(key)] = value

    def set_pre_win_info(self, pre_win_info):
        self.pre_win = pre_win_info

    def set_bingo_type(self, bingo_type):
        self.bingo_type = bingo_type

    def set_bingo_line(self, line_id, symbol_id, line_info, symbol_count, check_reel_length, check_reel_amount,
                       symbol_win, is_from_left=True, multiplier=1, transform = True):
        """
        設定中獎線資訊
        :param line_id: (int)中獎線的編號
        :param symbol_id: (int)中獎的symbol
        :param line_info: 中獎線的線型
        :param symbol_count: 中獎symbol的數量
        :param symbol_win: 贏分
        :param is_from_left: 是否從左計分
        :return:
        """
        bingo_line = {
            "line_id": int(line_id),
            "symbol_id": int(symbol_id),
            "symbol_count": int(symbol_count),
            "win": MathTool.floor_float(symbol_win, 3),
            "multiplier": multiplier,
        }

        check_reel_length_list = check_reel_length
        if type(check_reel_length) is int:
            check_reel_length_list = [check_reel_length for col in range(check_reel_amount)]
        reel_direct = range(check_reel_amount) if is_from_left else range(check_reel_amount-1, -1, -1)
        if transform:
            bingo_symbol_pos = list()
            current_col = 0
            for line_count in reel_direct:
                tmp_reel_data = list()
                for row in range(check_reel_length_list[current_col]):
                    if line_count < symbol_count and row == line_info[current_col]:
                        tmp_reel_data.append(1)
                    else:
                        tmp_reel_data.append(0)
                current_col += 1
                bingo_symbol_pos.append(tmp_reel_data)
            bingo_line["pos"] = bingo_symbol_pos
        else:
            bingo_line["pos"] = line_info
        self.bingo.append(bingo_line)


    def set_15_reels_bingo_line(self, line_id, symbol_id, line_info, symbol_count, check_reel_length, check_reel_amount,
                       symbol_win, is_from_left=True, multiplier=1):
        """
        設定15輪專用中獎線資訊
        :param line_id: (int)中獎線的編號
        :param symbol_id: (int)中獎的symbol
        :param line_info: 中獎線的線型
        :param symbol_count: 中獎symbol的數量
        :param symbol_win: 贏分
        :param is_from_left: 是否從左計分
        :return:
        """
        bingo_line = {
            "line_id": int(line_id),
            "symbol_id": int(symbol_id),
            "symbol_count": int(symbol_count),
            "win": MathTool.floor_float(symbol_win, 3),
            "multiplier": multiplier,
        }

        check_reel_length_list = check_reel_length
        if type(check_reel_length) is int:
            check_reel_length_list = [check_reel_length for col in range(check_reel_amount)]
        reel_direct = range(check_reel_amount) if is_from_left else range(check_reel_amount-1, -1, -1)
        bingo_symbol_pos = list()
        current_col = 0
        for line_count in reel_direct:
            for row in range(check_reel_length_list[current_col]):
                if line_count < symbol_count and row == line_info[current_col]:
                    bingo_symbol_pos.append([1])
                else:
                    bingo_symbol_pos.append([0])
            current_col += 1
        bingo_line["pos"] = bingo_symbol_pos
        self.bingo.append(bingo_line)

    def set_bingo_info(self, symbol_id, pos_info, symbol_count, ways_count, symbol_win, multiplier=1):
        """
        設定中獎資訊，symbol pos直接chance計算好設定
        :param symbol_id: (int)中獎的symbol
        :param pos_info: 中獎symbol的位置
        :type pos_info: list[list[int]]
        :param symbol_count: 中獎symbol的數量
        :param ways_count: (int)該Symbol中了多少ways
        :param symbol_win: 贏分
        :return:
        """

        bingo_line = {
                    "symbol_id": int(symbol_id),
                    "symbol_count": int(symbol_count),
                    "ways_count": int(ways_count),
                    "win": MathTool.floor_float(symbol_win, 3),
                    "multiplier": multiplier,
                    "pos": pos_info}
        # all ways 的遊戲不一定是NxN，可能會變形，所以這檢查可能會有問題
        # if len(pos_info) != check_reel_amount or len(pos_info[0]) != check_reel_length:
        #     raise Exception("set_bingo_way: pos_info size error, len(pos_info)={}, len(pos_info[0])={}, check_reel_amount={}, check_reel_length={}".format(len(pos_info), len(pos_info[0]), check_reel_amount, check_reel_length))

        self.bingo.append(bingo_line)

    def set_bingo_count_info(self, symbol_id, pos_info, symbol_count, symbol_win, multiplier=1):
        """
        設定中獎資訊，symbol pos直接chance計算好設定
        :param symbol_id: (int)中獎的symbol
        :param pos_info: 中獎symbol的位置
        :type pos_info: list[list[int]]
        :param symbol_count: 中獎symbol的數量
        :param symbol_win: 贏分
        :return:
        """

        bingo_line = {
                    "symbol_id": int(symbol_id),
                    "symbol_count": int(symbol_count),
                    "win": MathTool.floor_float(symbol_win, 3),
                    "multiplier": multiplier,
                    "pos": pos_info}
        # all ways 的遊戲不一定是NxN，可能會變形，所以這檢查可能會有問題
        # if len(pos_info) != check_reel_amount or len(pos_info[0]) != check_reel_length:
        #     raise Exception("set_bingo_way: pos_info size error, len(pos_info)={}, len(pos_info[0])={}, check_reel_amount={}, check_reel_length={}".format(len(pos_info), len(pos_info[0]), check_reel_amount, check_reel_length))

        self.bingo.append(bingo_line)

    def update_bingo_multiplier(self, multiplier):
        for item in self.bingo:
            item['multiplier'] = multiplier

    def clear_bingo(self):
        self.bingo = []

class MainGameResult(JsonSerializable):
    def __init__(self, block_id_list, *args, **kwargs):
        """
        初始化該有的資料
        """
        if type(block_id_list) is int:
            block_id_list = [block_id_list]
        self.block_id_list = block_id_list

        self.reel_block_data = kwargs.get("reel_block_data", dict())  # 一個 block 可能有多組 reel
        self.this_win = kwargs.get("this_win", 0)  # 這次動作結束獲得的贏分
        self.scatter_win_odds = kwargs.get("scatter_win_odds", 0)
        self.win_special_game = kwargs.get("win_special_game", 0)  # 贏得fever game次數，[sg_id] = times
        self.win_special_game_id_list = kwargs.get("win_special_game_id_list", list())
        self.win_special_game_id_times = kwargs.get("win_special_game_id_times", dict())
        self.special_game_current_script = kwargs.get("special_game_current_script", dict())
        self.temp_special_game_data = kwargs.get("temp_special_game_data", dict())
        self.init_reel_block_data()

        self.show_reel = kwargs.get("show_reel", list())
        self.extra_reel_info = kwargs.get("extra_reel_info", list())       # 儲存GameLog壓文字在symbol上的位置，格式和show_reel一致
        self.log_custom = kwargs.get("log_custom", dict())  # 客製化遊戲Log

        # new
        self.jackpot_info = kwargs.get("jackpot_info", dict())
        self.extra_data = kwargs.get("extra_data", dict())
        self.has_game_buffer = kwargs.get("has_game_buffer", False)
        self.hit_max_win = False

    def init_reel_block_data(self):
        for block_id in self.block_id_list:
            data = self.reel_block_data.get(str(block_id))
            self.reel_block_data[str(block_id)] = MainReelInfo() if data is None else MainReelInfo.from_json(data)

    def append_show_reel(self, show_reel):
        self.show_reel.append(show_reel)

    def get_show_reel_and_clean(self):
        temp = self.show_reel
        self.show_reel = list()
        return temp

    def set_win_special_game(self, sg_id, win_times):
        self.win_special_game_id_list.append(sg_id)
        self.win_special_game = win_times
        self.win_special_game_id_times.update({str(sg_id): win_times})

    def reset_win_special_game(self):
        self.win_special_game = 0
        self.win_special_game_id_list = list()
        self.win_special_game_id_times = dict()

    def get_special_game_times(self, sg_id):
        return self.win_special_game_id_times[str(sg_id)]

    def get_temp_special_game_data(self, key, default=None):
        return self.temp_special_game_data.get(key, default)

    def export_jackpot_info(self):
        return {"jackpot_info": self.jackpot_info}

    def set_win_jackpot(self, win_jp_level):
        self.jackpot_info['WinJP'] = win_jp_level

    def set_jackpot_info(self, key, value):
        self.jackpot_info.update({key: value})
        
    def set_temp_special_game_data(self, key, value):
        self.temp_special_game_data.update({key: value})

    def set_extra_data(self, key, value):
        self.extra_data[str(key)] = value

    def get_extra_data(self):
        return self.extra_data

    def set_log_custom(self, key, value):
        self.log_custom.update({key: value})

    def get_log_custom(self):
        return self.log_custom
        
    def update_special_game_current_script(self, sg_id, current_script):
        if str(sg_id) not in self.special_game_current_script:
            self.special_game_current_script[str(sg_id)] = dict()
        self.special_game_current_script[str(sg_id)].update(current_script)

    def get_reel_block_data(self, block_id):
        return self.reel_block_data[str(block_id)]

    def replace_reel_block_data(self, block_id, ori_symbol_id, new_symbol_id):
        for reel in self.show_reel:
            for col in reel:
                for elt_index in range(len(col)):
                    if col[elt_index] == ori_symbol_id:
                        col[elt_index] = new_symbol_id
        self.reel_block_data[str(block_id)].replace_symbol_id(ori_symbol_id, new_symbol_id)


    def update_game_buffer(self, has_game_buffer):
        self.has_game_buffer = has_game_buffer

    def export_ex_wheel_block_result(self):
        """
        將物件內的資料轉成EX模組的格式
        回傳給spin manager的資料，spin manager會再跟其他平台資料合併
        """
        return_data = list()

        # 所有轉輪區的資料
        for block_index in self.block_id_list:
            reel_data = self.reel_block_data[str(block_index)]
            """:type:MainReelInfo"""
            trans_reel_data = dict()
            trans_reel_data['id'] = block_index
            trans_reel_data['result_wheels'] = reel_data.reel_data
            for sg_id in self.win_special_game_id_list:
                trans_reel_data['win_special_symbols'] = reel_data.special_symbol_pos.get(str(sg_id), dict())
            if reel_data.has_rot_feature or reel_data.has_end_feature:
                trans_reel_data['feature_wheels'] = dict()
                if reel_data.has_rot_feature:
                    trans_reel_data['feature_wheels']['rot_feature_map'] = reel_data.get_rot_feature_in_list()
                    trans_reel_data['feature_wheels']['Rot'] = reel_data.get_rot_feature_in_list_extra()
                if reel_data.has_end_feature:
                    # trans_reel_data['feature_wheels']['end_feature_map'] = reel_data.get_end_feature_in_list()
                    trans_reel_data['feature_wheels']['End'] = reel_data.get_end_feature_in_list_extra()
            if reel_data.has_pre_win:
                trans_reel_data['pre_win_wheels'] = reel_data.pre_win
            if reel_data.has_bingo:
                trans_reel_data['bingo'] = reel_data.bingo
            trans_reel_data['bingo_type'] = reel_data.bingo_type
            return_data.append(trans_reel_data)

        return return_data

    @property
    def spin_reels(self):
        # data[block_id][reel_data]
        return_data = dict()
        for block_index in self.block_id_list:
            return_data.update({str(block_index):self.reel_block_data[str(block_index)].reel_data})
            return_data.update({"0":self.reel_block_data[str(block_index)].reel_data})
        return return_data

    @property
    def is_win_special_game(self):
        return len(self.win_special_game_id_list) > 0

    @property
    def is_win_scatter(self):
        return self.is_win_special_game

    @property
    def first_special_id(self):
        if len(self.win_special_game_id_list) > 0:
            return self.win_special_game_id_list[0]
        return -1

    @property
    def special_game_times(self):
        return self.win_special_game

    def special_game_id_times(self, sg_id):
        return self.win_special_game_id_times[str(sg_id)]

    @property
    def have_extra_data(self):
        return (len(self.extra_data) > 0)

    def __str__(self):
        return "MainGameResult:\n- this_win={},\n- scatter_win_odds={},\n- win_special_game={},\n- win_special_game_id_list={},\n- special_game_current_script={},\n- temp_special_game_data={},\n- extra_data={}".format(
            self.this_win, self.scatter_win_odds, self.win_special_game, self.win_special_game_id_list, self.special_game_current_script, self.temp_special_game_data, self.extra_data)
