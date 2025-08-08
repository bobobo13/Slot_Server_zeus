#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'shihpochien'
import time

import functools


class RecordData(object):
    _feature_record_func = dict()

    def init_info(self, game_id, bet_lines, bet_value, cost_multi, extra_bet_ratio=1):
        # 紀錄資料初始化
        self.start_timestamp = int(time.time())

        self.game_id = game_id
        self.bet_lines = bet_lines
        self.bet_value = bet_value
        self.total_bet_value = self.bet_lines * self.bet_value * extra_bet_ratio

        self.cost_multi = cost_multi
        self.total_cost_value = self.bet_lines * self.bet_value * cost_multi

        self.total_times = 0
        self.total_bet = 0
        self.total_win = 0
        self.feature_times = dict()
        self.total_cost = 0
        self.win_list = []

        self.main_win_times = 0
        self.main_win_times_list = dict()
        self.free_win_times = 0
        self.special_game_win_times = dict()

        self.main_win = 0
        self.free_win = 0
        self.special_game_win = dict()  # int(sg_id)[int(win)]
        self.feature_win = dict()

        self.bingo_count = dict()

        self.enter_free_times = 0  # 進入次數
        self.enter_special_game_times = dict()  # int(sg_id)[int(times)] ， 進入次數
        self.free_times = 0  # 特殊遊戲內旋轉次數
        self.special_game_times = dict()  # int(sg_id)[int(times)] ， 特殊遊戲內旋轉次數
        self.feature_wild_hit = {'main': [0, 0, 0, 0, 0, 0], 'free': [0, 0, 0, 0, 0, 0]}
        self.special_game_record = {}

        self.in_game_jp_dic = {}

        self.mgCombolCnt = 0
        self.sgCombolCnt = {"0": 0, "1": 0}

    def __add__(self, other):
        self.total_times += other.total_times
        self.total_bet += other.total_bet
        self.total_win += other.total_win
        self.total_cost += other.total_cost
        self.win_list = self.win_list + other.win_list

        self.main_win += other.main_win
        self.free_win += other.free_win
        self.free_times += other.free_times

        self.main_win_times += other.main_win_times
        self.free_win_times += other.free_win_times

        self.enter_free_times += other.enter_free_times
        self.mgCombolCnt += other.mgCombolCnt

        toadd_dict_list = [[self.feature_times, other.feature_times], [self.feature_win, other.feature_win],
                           [self.in_game_jp_dic, other.in_game_jp_dic], [self.sgCombolCnt, other.sgCombolCnt],
                           [self.special_game_times, other.special_game_times], [self.special_game_win, other.special_game_win],
                           [self.enter_special_game_times, other.enter_special_game_times], [self.special_game_win_times, other.special_game_win_times]]

        for dict_list in toadd_dict_list:
            origin_dict = dict_list[0]
            add_dict = dict_list[1]
            for key in add_dict:
                if key not in origin_dict:
                    origin_dict[key] = add_dict[key]
                else:
                    origin_dict[key] += add_dict[key]

        for key in other.bingo_count:
            if key not in self.bingo_count:
                self.bingo_count[key] = other.bingo_count[key]
            else:
                for i in range(len(other.bingo_count[key])):
                    self.bingo_count[key][i] += other.bingo_count[key][i]

        for key in other.special_game_record:
            if key not in self.special_game_record:
                self.special_game_record[key] = other.special_game_record[key]
            else:
                for field in other.special_game_record[key]:
                    if field not in self.special_game_record[key]:
                        self.special_game_record[key][field] = other.special_game_record[key][field]
                    else:
                        self.special_game_record[key][field] += other.special_game_record[key][field]

        if len(other.main_win_times_list) > 0:
            if len(self.main_win_times_list) == 0:
                self.main_win_times_list = other.main_win_times_list
            else:
                for i in range(len(other.main_win_times_list)):
                    self.main_win_times_list[i] += other.main_win_times_list[i]

        for key in other.feature_wild_hit:
            # value是list，需要逐一相加
            if key not in self.feature_wild_hit:
                self.feature_wild_hit[key] = other.feature_wild_hit[key]
            else:
                for i in range(len(other.feature_wild_hit[key])):
                    self.feature_wild_hit[key][i] += other.feature_wild_hit[key][i]

        return self

    def main_spin_record(self, result, game_state, bingo_data=None):
        if self.game_id not in self._feature_record_func:
            self.spin(result.this_win, game_state.is_special_game, bingo_data)
        else:
            self._feature_record_func[self.game_id](self, result, game_state)

    def spin(self, win, win_special_game=False, bingo_data=None):
        self.total_times += 1
        self.total_bet += self.total_bet_value
        self.total_win += win
        self.win_list.append(float(win / self.total_bet_value))
        self.main_win += win
        self.total_cost += self.total_cost_value

        if win > 0 or win_special_game:
            self.main_win_times += 1
        if bingo_data is not None:
            for bingo in bingo_data:
                temp_liner = -1
                ways_count = 1
                for col_data in bingo['pos']:
                    if 1 in col_data:
                        temp_liner += 1
                    temp_symbol_count = 0
                    for is_hit in col_data:
                        if is_hit == 1:
                            temp_symbol_count += 1
                    if temp_symbol_count > 0:
                        ways_count *= temp_symbol_count
                if bingo['symbol_id'] not in self.bingo_count:
                    self.bingo_count[bingo['symbol_id']] = [0, 0, 0, 0, 0]
                self.bingo_count[bingo['symbol_id']][temp_liner] += ways_count

    def feature(self, win, id, is_extra=False, win_special_game=False, bingo_data=None):
        if not is_extra:
            self.total_times += 1
            self.total_bet += self.total_bet_value
            self.total_cost += self.total_cost_value
        self.total_win += win
        self.win_list.append(float(win / self.total_bet_value))
        if id not in self.feature_times:
            self.feature_win[id] = 0
            self.feature_times[id] = 0

        self.feature_win[id] += win
        self.feature_times[id] += 1
        if (win > 0 or win_special_game) and not is_extra:
            self.main_win_times += 1

        if bingo_data is not None:
            for bingo in bingo_data:
                temp_liner = -1
                ways_count = 1
                for col_data in bingo['pos']:
                    if 1 in col_data:
                        temp_liner += 1
                    temp_symbol_count = 0
                    for is_hit in col_data:
                        if is_hit == 1:
                            temp_symbol_count += 1
                    if temp_symbol_count > 0:
                        ways_count *= temp_symbol_count
                if bingo['symbol_id'] not in self.bingo_count:
                    self.bingo_count[bingo['symbol_id']] = [0, 0, 0, 0, 0]
                self.bingo_count[bingo['symbol_id']][temp_liner] += ways_count

    def special_game(self, sg_id, win, current_level, bingo_data=None, special_game_data=None):
        # 還沒紀錄過，要先初始化紀錄欄位
        if sg_id not in self.special_game_times:
            self.special_game_times[sg_id] = 0
            self.special_game_win[sg_id] = 0
            self.enter_special_game_times[sg_id] = 0
            self.special_game_win_times[sg_id] = 0

        self.total_win += win
        self.win_list.append(float(win / self.total_bet_value))
        self.special_game_win[sg_id] += win

        if win > 0:
            self.special_game_win_times[sg_id] += 1

        # current_level代表special初始化，紀錄次數
        if current_level == 1:
            self.enter_special_game_times[sg_id] += 1
        if current_level >= 2:
            self.special_game_times[sg_id] += 1
        if bingo_data is not None:
            for bingo in bingo_data:
                temp_liner = -1
                for col_data in bingo['pos']:
                    if 1 in col_data:
                        temp_liner += 1
                if bingo['symbol_id'] not in self.bingo_count:
                    self.bingo_count[bingo['symbol_id']] = [0, 0, 0, 0, 0]
                self.bingo_count[bingo['symbol_id']][temp_liner] += 1

        if isinstance(special_game_data, dict) and len(special_game_data) > 0:
            if sg_id not in self.special_game_record:
                self.special_game_record[sg_id] = dict()
            for field in special_game_data:
                if field not in self.special_game_record[sg_id]:
                    self.special_game_record[sg_id][field] = 0
                self.special_game_record[sg_id][field] += special_game_data[field]

    def free_game(self, sg_id, win, current_times):
        self.total_win += win
        self.win_list.append(float(win / self.total_bet_value))
        self.free_win += win
        self.free_times += 1

        if win > 0:
            self.free_win_times += 1

        # 第一手free spin紀錄free spin次數
        if current_times == 1:
            self.enter_free_times += 1

    def print_record_data(self, logger, rid=None):
        during_time = int(time.time()) - self.start_timestamp
        m, s = divmod(during_time, 60)
        h, m = divmod(m, 60)
        row_output = ""
        if rid is not None:
            logger.out("===========================================")
            logger.out("Record ID: {}".format(rid))
        logger.out("===========================================")
        logger.out("during time: {}:{:2d}:{:2d}".format(h, m, s))
        logger.out("{:30s}: {:>20s}".format("GameID", self.game_id))
        logger.out("{:30s}: {:20d}".format("PlayTimes", self.total_times))
        logger.out("{:30s}: {:20d}".format("BetLines", self.bet_lines))
        logger.out("{:30s}: {:20f}".format("TotalBet", self.total_bet))
        logger.out("{:30s}: {:20f}".format("TotalCost", self.total_cost))
        logger.out("{:30s}: {:20f} {:10.4f}%".format("TotalWin", self.total_win, float(float(self.total_win) / float(self.total_bet) * 100.0)))
        logger.out("{:30s}: {:20f} {:10.4f}%".format("[Bonus]TotalWin", self.total_win, float(float(self.total_win) / float(self.total_cost) * 100.0)))
        row_output += "{:.4f},".format(float(float(self.total_win) / float(self.total_bet) * 100.0))

        logger.out("{:^46s}".format("**** Main Game ****"))
        logger.out("{:>30s}- {:20.4f}".format("MainGame win", self.main_win))
        logger.out("{:>30s}- {:20.4f}%".format("MainGame RTP", float(float(self.main_win) / float(self.total_bet) * 100.0)))
        logger.out("{:>30s}- {:20.4f}%".format("[Bonus]MainGame RTP", float(float(self.main_win) / float(self.total_cost) * 100.0)))
        logger.out("{:>30s}- {:20.4f}".format("MainGame hit rate", float(self.main_win_times) / float(self.total_times)))
        # ================================
        logger.out("{:>30s}- {:20.4f}".format("avg Combol", float(self.mgCombolCnt) / float(float(self.main_win_times))))
        # ================================
        row_output += "{:.4f},".format(float(float(self.main_win) / float(self.total_bet) * 100.0))
        row_output += "{:.4f},".format(float(float(self.main_win_times) / float(self.total_times) * 100.0))

        for id in range(len(self.feature_times)):
            logger.out("{:^46s}".format("**** Feature Game {}****".format(id)))
            logger.out("{:>30s}- {:20.4f}".format("FeatureGame win", self.feature_win[id]))
            logger.out("{:>30s}- {:20.4f}%".format("FeatureGame RTP", float(float(self.feature_win[id]) / float(self.total_bet) * 100.0)))
            logger.out("{:>30s}- {:20.4f}%".format("[Bonus]FeatureGame RTP", float(float(self.feature_win[id]) / float(self.total_cost) * 100.0)))
            logger.out("{:>30s}- {:20.4f}".format("FeatureGame frequency", float(self.total_times) / float(self.feature_times[id])))

        for sg_id in self.special_game_times:
            logger.out("{:^46s}".format("**** Special Game {} ****".format(sg_id)))
            logger.out("{:>30s}- {:20.4f}".format("SG:{} win".format(sg_id), self.special_game_win[sg_id]))
            logger.out("{:>30s}- {:20.4f}%".format("SG:{} RTP".format(sg_id), float(float(self.special_game_win[sg_id]) / float(self.total_bet) * 100.0)))
            logger.out("{:>30s}- {:20.4f}%".format("[Bonus]SG:{} RTP".format(sg_id), float(float(self.special_game_win[sg_id]) / float(self.total_cost) * 100.0)))
            logger.out("{:>30s}- {:20.4f}".format("SG:{} hit rate".format(sg_id), float(self.special_game_win_times[sg_id]) / float(self.special_game_times[sg_id] if self.special_game_times[sg_id] > 0 else 1)))
            logger.out("{:>30s}- {:20.4f}".format("SG:{} frequency".format(sg_id), float(self.total_times) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0))
            logger.out("{:>30s}- {:20.4f}".format("SG:{} avg play times".format(sg_id), float(self.special_game_times[sg_id]) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0))
            logger.out("{:>30s}- {:20.4f}".format("SG:{} avg win odds".format(sg_id), float(self.special_game_win[sg_id]) / float(self.total_bet_value) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0))
            logger.out("{:>30s}- {:20.4f}".format("[Bonus]SG:{} avg win odds".format(sg_id), float(self.special_game_win[sg_id]) / float(self.total_cost_value) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0))

            row_output += "{:.4f},".format(float(float(self.special_game_win[sg_id]) / float(self.total_bet) * 100.0))
            row_output += "{:.4f},".format(100 * float(self.special_game_win_times[sg_id]) / float(self.special_game_times[sg_id] if self.special_game_times[sg_id] > 0 else 1))
            row_output += "{:.4f},".format(float(self.total_times) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0)
            row_output += "{:.4f},".format(float(self.special_game_times[sg_id]) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0)
            row_output += "{:.4f},".format(float(self.special_game_win[sg_id]) / float(self.total_bet_value) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0)
            for field in self.special_game_record.get(sg_id, {}):
                logger.out("{:>40s}- {:20.7f}".format("{}".format(field), float(self.special_game_record[sg_id][field]) / float(self.enter_special_game_times[sg_id]) if self.enter_special_game_times[sg_id] > 0 else 0))

        if len(self.in_game_jp_dic) > 0:
            logger.out("{:^46s}".format("**** Jackpot  ****"))
            key_list = sorted(self.in_game_jp_dic.keys())
            str_jp_no = ""
            str_jp_hit_times = ""
            str_jp_hit_rate = ""
            str_jp_hit_freq = ""
            for key in key_list:
                jp_no = int(key) + 1
                str_jp_no += '       JP{:3d} '.format(jp_no)
            for key in key_list:
                str_jp_hit_times += '{:12d},'.format(self.in_game_jp_dic[key])
            for key in key_list:
                rate = float(self.in_game_jp_dic[key]) / self.total_times
                freq = float(self.total_times) / self.in_game_jp_dic[key]
                str_jp_hit_rate += '{:12.6f},'.format(rate)
                str_jp_hit_freq += '{:12.2f},'.format(freq)
            logger.out('jp_no           {}'.format(str_jp_no))
            logger.out('jp_hit_times    {}'.format(str_jp_hit_times))
            logger.out('jp_hit_rate     {}'.format(str_jp_hit_rate))
            logger.out('jp_hit_freq     {}'.format(str_jp_hit_freq))

        logger.out(row_output)

    def print_line_count(self, logger):
        logger.out("********************************")
        for symbol in self.bingo_count:
            logger.out("{:10s}:  {}".format(str(symbol), self.bingo_count[symbol]))


def FeatureRecord(game_id):
    def decorator(func):
        # record = RecordData()

        @functools.wraps(func)
        def wrapper(record, result, game_state, bingo_data=None):
            is_feature = func(record, result, game_state, bingo_data)
            if not is_feature:
                record.spin(result.this_win, game_state.is_special_game, bingo_data)

        RecordData._feature_record_func.update({game_id: wrapper})
        return wrapper

    return decorator


@FeatureRecord("FortuneOfLegends")
def FortuneOfLegends_feature_record(record, result, game_state, bingo_data):
    trans_result = result.export_ex_wheel_block_result()
    feature_wheel = trans_result[0].get("feature_wheels", {})
    if "end_feature_map" in feature_wheel:
        for i in range(len(feature_wheel["end_feature_map"])):
            if feature_wheel["end_feature_map"][i]['key'] == '3':
                result.this_win -= feature_wheel['end_feature_map'][i]['value']['1']
                golden_reels_count = sum(feature_wheel['end_feature_map'][i]['value']['0'])
                record.feature(feature_wheel['end_feature_map'][i]['value']['1'], golden_reels_count - 2,
                               is_extra=True)
    if "rot_feature_map" in feature_wheel:  # wild col
        record.feature(result.this_win, 0)
    else:
        return False
    return True


@FeatureRecord("Queen")
def Queen_feature_record(record, result, game_state, bingo_data):
    if result.win_special_game == 1:
        record.feature(result.this_win, 0)
        return True
    else:
        return False


@FeatureRecord("FuXingGaoZhao")
def FuXingGaoZhao_15_SC_record(record, result, game_state, bingo_data):
    if result.get_reel_block_data(0).end_feature.get("0"):
        if "0" not in record.in_game_jp_dic:
            record.in_game_jp_dic["0"] = 1
        else:
            record.in_game_jp_dic["0"] += 1
    return False


@FeatureRecord("SuperBar")
def SuperBar_special_7_record(record, result, game_state, bingo_data):
    if result.get_reel_block_data(0).end_feature.get("0"):
        record.feature(result.this_win, 0)
        return True
    else:
        return False


@FeatureRecord("ArcaneEgypt")
def ArcaneEgypt_feature_record(record, result, game_state, bingo_data):
    if result.get_temp_special_game_data('feature_multiple') > 1:
        record.feature(result.this_win, 0)
        return True
    else:
        return False


@FeatureRecord("LionDance")
def LionDance_feature_record(record: RecordData, result, game_state, bingo_data):
    trans_result = result.export_ex_wheel_block_result()
    feature_wheel = trans_result[0].get("feature_wheels", {})
    if "end_feature_map" in feature_wheel:
        record.feature(result.this_win, 0)
        return True
    else:
        return False
