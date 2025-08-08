#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy

from abc import ABCMeta, abstractmethod
from .Util.MathTool import floor_float

# 做為基本規則讓其他function繼承
class MainGameCheck(object):
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        self.randomer = kwargs.get('randomer')
        self.get_result_by_weight = self.randomer.get_result_by_weight
        self.get_result_by_gate = self.randomer.get_result_by_gate

        self.scatter_win = 0
        self.WildSymbolID = 1
        self.WildSymbolIdList = [self.WildSymbolID]
        self.FeverSymbolID = 2
        # 判斷pre_win及是否觸發special game用
        self.FeverLimitCount = 3
        # special game id
        self.FeverID = 1
        self._init_const()

    def _init_const(self):
        # id對照表
        pass

    #============================================Main============================================
    def game_check(self, main_result, block_id, play_info, odds, special_odds, extra_odds, reel_length, reel_amount, check_reel_length, check_reel_amount):
        """
        :param main_result: 這次spin的所有資訊
        :param play_info: 遊戲的押注、選線、特殊遊戲狀態
        :param odds: 賠率表
        :param special_odds: scatter中獎獲得的次數
        :param extra_odds: 其他機率相關的設定數值
        :param reel_length: 包含不可視的symbol，一輪有多少顆symbol
        :param reel_amount: 包含不可視的symbol，有多少輪
        :param check_reel_length: 不包含不可視的symbol，一輪有多少顆symbol
        :param check_reel_amount: 不包含不可視的symbol，有多少輪
        :return:
        """
        #note: local variable fast than class variable
        self.scatter_win = 0
        FeverSymbolID = self.FeverSymbolID
        FeverLimitCount = self.FeverLimitCount
        FeverID =self.FeverID

        block_id = 0

        # ====================================================
        # show_reel: client顯示的牌面, check_reel: 檢查各種獎項使用的牌面
        # 若有修改到show_reel，最後需要存回main_result
        show_reel = self.get_check_reel(main_result, block_id, reel_length, reel_amount, check_reel_length, check_reel_amount)
        check_reel = copy.deepcopy(show_reel)
        # ===================================================================================================
        # 檢查轉輪中的Feature
        self.feature_check_during_spin(main_result, block_id, play_info, extra_odds, check_reel, show_reel, check_reel_length, check_reel_amount)
        # 檢查停輪後的Feature
        self.feature_check_after_spin(main_result, block_id, play_info, extra_odds, check_reel, show_reel, check_reel_length, check_reel_amount)
        # ===================================================================================================
        # 檢查線獎贏分
        self.main_win_check(main_result, block_id, play_info, odds, check_reel, show_reel, check_reel_length, check_reel_amount)
        # ===================================================================================================
        # Scatter獎項檢查，包括贏的次數和是否有基本贏分
        # 檢查free spin
        self.special_symbol_check(main_result, block_id, play_info, odds, special_odds, FeverSymbolID, FeverLimitCount, FeverID,
                             check_reel, show_reel, check_reel_length, check_reel_amount, is_pass_line=False)
        # ===================================================================================================
        # pre-win檢查
        self.pre_win_check(main_result, block_id, FeverSymbolID, FeverLimitCount, check_reel, show_reel, check_reel_length, check_reel_amount)

    def get_check_reel(self, main_result, block_id, reel_length, reel_amount, check_reel_length, check_reel_amount,
                       transform=True, check_reel_length_for_non_trasform=1):
        """
        for 轉出盤面與算分盤面不同的規格，例如:15輪轉出3x15，但 算分時是用3x5
        :type main_result: MainGameResultEX
        """
        check_reel = list()
        reel_info = main_result.get_reel_block_data(block_id)
        # print 'get_check_reel block_id: ', block_id, ', reel_info: ', str(reel_info)
        """:type: MainReelInfo"""
        # 檢查reel_amount，數量不同代表輪數不一樣，需要轉換
        if reel_amount != check_reel_amount and transform:
            # print 'reel_amount != check_reel_amount and transform'
            middle_index = reel_length // 2  # 3x15每一輪的第1個(0,1,2)symbol才是對獎的時候使用的
            for col in range(check_reel_amount):
                temp_one_reel = list()
                for row in range(check_reel_length):
                    trans_col = col*3 + row  # 將3x5的位置轉換成15輪的位置，抓取symbol用
                    temp_one_reel.append(reel_info.reel_data[str(trans_col)][middle_index])
                check_reel.append(temp_one_reel)
        elif reel_amount != check_reel_amount:
            # print 'reel_amount != check_reel_amount and not transform'
            middle_index = reel_length // 2  # 不變型，像FireSpin維持9輪
            for col in range(reel_amount):
                temp_one_reel = list()
                for row in range(check_reel_length_for_non_trasform):
                    temp_one_reel.append(reel_info.reel_data[str(col)][middle_index])
                check_reel.append(temp_one_reel)
        # 將每一輪需要做check的部分取出
        else:
            # print 'else'
            half_invisible_symbol_count = (reel_length - check_reel_length) // 2
            # print 'len: ', len(reel_info.reel_data)
            for col in range(check_reel_amount):
                start_index = half_invisible_symbol_count
                end_index = reel_length - half_invisible_symbol_count
                check_reel.append(reel_info.reel_data[str(col)][start_index:end_index])
        # print 'check_reel', check_reel
        main_result.append_show_reel(check_reel)
        return check_reel

    #======================================Feature================================================
    # 比較常用還未實作共用流程的feature
    # wild伸長
    # 判斷ExtendWildFeature是否表演
    # 隨機貼上wild的feature
    def feature_check_during_spin(self, main_result, block_id, play_info, extra_odds, check_reel, show_reel, check_reel_length, check_reel_amount):
        """
        檢查停輪前要表演的feature
        """
        reel_info = main_result.get_reel_block_data(block_id)
        feature_key = 0  # type must be int
        feature_data = list()  # can set any type of data to client
        reel_info.set_rot_feature(feature_key, feature_data)

    def feature_check_after_spin(self, main_result, block_id, play_info, extra_odds, check_reel, show_reel, check_reel_length, check_reel_amount):
        """
        檢查停輪後表演的feature
        """
        reel_info = main_result.get_reel_block_data(block_id)
        feature_key = 0  # type must be int
        feature_data = list()  # can set any type of data to client
        reel_info.set_end_feature(feature_key, feature_data)


    @abstractmethod
    def main_win_check(self, main_result, block_id, play_info, odds, check_reel, show_reel, check_reel_length, check_reel_amount, is_two_way=False):
        pass

    # =======================================檢查特殊symbol============================================
    # 要將free game和 special game的拆成兩個function
    def special_symbol_check(self, main_result, block_id, play_info, odds, special_odds, check_symbol_id, check_symbol_limit, special_game_id,
                             check_reel, show_reel, check_reel_length, check_reel_amount, is_pass_line=False, check_symbol_included_wild=tuple(), increasing=False):
        """
        檢查特殊symbol的中獎，包括贏得的次數和倍數
        次數設定在special odd中，倍數設定在odds中
        :param check_symbol_included_wild: 若遊戲規格中允許wild symbol替代scatter，將tuple(wild symbol)傳入
        """
        reel_info = main_result.get_reel_block_data(block_id)
        symbol_count = 0
        symbol_line_count = 0
        symbol_pos = [[-1 for row in range(len(check_reel[col]))] for col in range(len(check_reel))]
        for col in range(len(check_reel)):
            current_col_symbol_count = 0
            for row in range(len(check_reel[col])):
                current_symbol = check_reel[col][row]
                if current_symbol == check_symbol_id or current_symbol in check_symbol_included_wild:
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
            if symbol_count > len(special_odds[key]):
                symbol_count = len(special_odds[key])
            win_times = special_odds[key][symbol_count-1]
            win_odds = odds[str(check_symbol_id)][symbol_count-1]
            win_odds *= play_info.total_bet
            win_odds = floor_float(win_odds, 3)
            main_result.scatter_win_odds = win_odds
            #print '-------game check scatter_win='+str(self.scatter_win)
            reel_info.set_special_symbol_win_pos(special_game_id, symbol_pos)
            main_result.set_win_special_game(special_game_id, win_times)
            if not play_info.is_special_game:
                current_script = {
                }
                self.set_win_special_symbol_info(main_result, block_id, special_game_id, check_symbol_id, symbol_pos, win_times, current_script, increasing)
            if win_odds > 0:
                main_result.scatter_win_odds = win_odds
                main_result.this_win += win_odds

    def set_win_special_symbol_info(self, main_result, block_id, special_game_id, symbol_id, symbol_pos, win_times, current_script, increasing=False):
        sym_pos_dict = dict()
        for col_index in range(len(symbol_pos)):
            sym_pos_dict[str(col_index)] = symbol_pos[col_index]
        current_script.update({
            'current_times': 0 if increasing else win_times,
            'total_times': win_times,
            'win_special_symbols': sym_pos_dict,
        })
        reel_info = main_result.get_reel_block_data(block_id)
        reel_info.set_special_symbol_win_pos(special_game_id, symbol_pos)
        main_result.set_win_special_game(special_game_id, win_times)
        main_result.update_special_game_current_script(special_game_id, current_script)

    # =======================================pre-win check============================================
    # is_pass_line: 是否需要連線－True: Symbol要接著出現/False:Symbol可以跳著出現
    def pre_win_check(self, main_result, block_id, check_symbol_id, check_symbol_limit, check_reel, show_reel, check_reel_length, check_reel_amount, is_pass_line=False, check_symbol_included_wild=tuple()):
        reel_info = main_result.get_reel_block_data(block_id)
        # 前兩輪不聽牌，後面的都有機會聽牌的規格
        need_pre_win_col = [(False if num < check_symbol_limit-1 else True) for num in range(check_reel_amount)]
        pre_win_info = [0 for col in range(check_reel_amount)]
        symbol_count = 0
        check_symbol_limit -= 1
        has_pre_win = False

        for col in range(len(check_reel)):
            if need_pre_win_col and is_pass_line and symbol_count < col:
                break
            if symbol_count >= check_symbol_limit and need_pre_win_col[col]:
                pre_win_info[col] = 1
                has_pre_win = True
            for row in range(len(check_reel[col])):
                current_symbol = check_reel[col][row]
                if current_symbol == check_symbol_id or current_symbol in check_symbol_included_wild:
                    symbol_count += 1

        if has_pre_win:
            reel_info.set_pre_win_info(pre_win_info)


    def double_game_check(self, client_action, bet, game_data_dic, double_game_odds):
        return 0
