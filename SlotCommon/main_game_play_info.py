#!/usr/bin/env python
# -*- coding: utf-8 -*-

class MainGamePlayInfo(object):
    def __init__(self):
        self.GameBetLines = 0
        self.GameLineBet = 0
        self.GameTotalBet = 0
        self.GameBetSegment = 0  # for all ways紀錄押注段數,n輪的slot game,有0~n-1段
        self.SpecialGame = -1
        self.StoredData = None
        self.WinnableLines = 0

        self.LineData = list()
        self.ExtraBet = False

    def set_is_fever_game(self, is_fever_game):
        if type(is_fever_game) is bool:
            self.SpecialGame = 1 if is_fever_game else -1  # int
        else:
            self.SpecialGame = int(is_fever_game)  # int

    def set_bet_info(self, line_bet, bet_lines, winnable_lines=None):
        """
        :param line_bet: (float) 押注金額
        :param bet_lines: (int) 押注線數
        :param winnable_lines: (int) 可贏線數
        """
        self.GameBetLines = int(bet_lines)

        line_bet = float(line_bet)
        if line_bet.is_integer():
            line_bet = int(line_bet)
        self.GameLineBet = line_bet

        # self.GameLineBet = float(line_bet)

        self.GameTotalBet = self.GameLineBet * self.GameBetLines

        if winnable_lines is not None:
            self.WinnableLines = int(winnable_lines)
        else:
            self.WinnableLines = self.GameBetLines

    # 目前AllWays的遊戲沒有可以切cost，所以BetSegment還沒有用
    # 之後使用到還需要做測試
    def set_all_ways_bet_info(self, line_bet, cost, bet_segment):
        self.GameBetSegment = int(bet_segment)
        self.GameBetLines = int(cost)
        self.GameLineBet = int(line_bet)
        self.GameTotalBet = self.GameLineBet * self.GameBetLines

    def set_line_data(self, line_data):
        self.LineData = line_data

    def set_extra_bet(self, is_extra_bet):
        if type(is_extra_bet) is bool:
            self.ExtraBet = is_extra_bet

    # def set_line_type(self, line_type):
    #     self.LineType = line_type

    @property
    def is_special_game(self):
        return self.SpecialGame != -1

    @property
    def special_game_id(self):
        return self.SpecialGame

    @property
    def line_bet(self):
        return self.GameLineBet

    @property
    def bet_lines(self):
        return self.GameBetLines

    @property
    def winnable_lines(self):
        return self.WinnableLines

    @property
    def total_bet(self):
        return self.GameTotalBet

    @property
    def bet_segment(self):
        return self.GameBetSegment

    @property
    def cost(self):
        return self.GameBetLines

    def set_stored_data(self, dev_mode):
        if isinstance(dev_mode, dict):
            self.StoredData = dev_mode
        return dev_mode

    @property
    def is_from_stored_data(self):
        return self.StoredData is not None

    @property
    def stored_data(self):
        return self.StoredData

    @property
    def is_extra_bet(self):
        return self.ExtraBet

    # @property
    # def line_type(self):
    #     return self.line_type

    """
    def read_line_data(self, reel_length, reel_amount):
        '''
        傳入實際對獎的牌面大小 3X5 or 4X5...
        :param reel_length: 每一輪看的到的symbol的數量
        :param reel_amount: 總共有多少輪
        :return: (2d-list)現型資訊
        '''
        return self.LineData[str(reel_amount)][str(reel_length)]

    def read_line_index(self, lineNo, index, reel_length, reel_amount):
        '''
        傳入實際對獎的牌面大小 3X5 or 4X5...
        :param lineNo: 對獎線的編號
        :param index: 線上的第幾個symbol
        :param reel_length: 每一輪看的到的symbol的數量
        :param reel_amount: 總共有多少輪
        :return: (int)symbol的位置 0、1、2 ...
        '''
        return self.LineData[str(reel_amount)][str(reel_length)][lineNo][index]
    """
