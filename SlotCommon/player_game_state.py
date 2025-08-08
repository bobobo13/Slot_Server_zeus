#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
特殊遊戲GameState部分：
1. 只能有一組free game
2. 移除bonus game
3. fever game的game state統一改成special game state
4. special game可以有多組，需要在 self._special_game_state 增加一組
"""
import time
from .Util.Util import Copy as copy

class PlayerGameState(object):
    def __deepcopy__(self, memodict={}):
        new_obj = PlayerGameState(copy.deepcopy(self.as_json_dict()))
        return new_obj

    def __init__(self, game_state):
        """
        self._fever = {                  # fever遊戲狀態
            'id': 0,
            'current_win': 0,
            'current_win_amount': 0,
            'current_bet': 0,
            'current_line': 0,
            'current_base': 0,
            'current_multiply': 0,
            'current_table': -1,
            'current_level': 0,
            'current_script': {},
        }
        """
        self._currency = ""
        self._Channel = None
        self._special_game_state = dict()   # [sg_id][dict[state]] 記錄中的special game的狀態
        self._current_sg_id = []            # 中的special game的id
        self._league_id = ""
        self._last_fever_reels = {}      # fever最後盤面
        self._last_main_reels = {}       # 最後盤面
        self._win_special_symbols = {}   # 要表演的特殊symbol的位置
        self._extra_info = {}
        self._one_play_win_amount = 0
        self._original_game_win_amount = 0   # without jp win
        self._last_game_win_amount = 0       # without jp win and for double game
        self._special_game_play_times_in_a_row = 0  # 連續玩speical game(double game)的次數
        self._bet_lines = 0             # 進入特殊遊戲時的當下線數
        self._extra_bet = None   #是否開啟extra_bet
        self._game_buffer = {}  # game buffer資料
        self._probId = None  #玩家在機台的機率表Id
        self._feverProbId = None  #玩家在機台的Fever機率表Id
        self._game_no = 0
        self._current_bet = 0
        self._cache_credit = 0         # 暫存餘額

        # BuyBonus
        self.temp_bonus_trigger_type = None
        self.temp_bonus_data = {}

        self._init_game_state(game_state)

    def __eq__(self, other):
        return other and self.as_json_dict() == other.as_json_dict()

    def __str__(self):
        return str(self.as_json_dict())

    @property
    def currency(self):
        return self._currency

    @currency.setter
    def currency(self, currency):
        self._currency = currency

    @property
    def is_item_trigger(self):
        if len(self._special_game_state) <= 0:
            return False
        elif "-1" in self._special_game_state:
            return True
        return self._special_game_state[str(self._current_sg_id[0])].get('is_item_trigger', False)

    @property
    def bonus_trigger_type(self):
        if len(self._special_game_state) <= 0:
            return None
        return self._special_game_state[str(self._current_sg_id[0])].get('bonus_trigger_type')

    @property
    def bonus_data(self):
        if len(self._special_game_state) <= 0:
            return None
        return self._special_game_state[str(self._current_sg_id[0])].get('bonus_data')

    @property
    def fever_prob_id(self):
        if self._feverProbId is None:
            return self._probId
        return self._feverProbId

    @property
    def item_status(self):
        if not self.is_item_trigger:
            return {}
        elif "-1" in self._special_game_state:
            return self._special_game_state["-1"].get('item_status', {})
        return self._special_game_state[str(self._current_sg_id[0])].get('item_status', {})

    @property
    def is_special_game(self):
        return len(self._current_sg_id) > 0
    @property
    def is_scatter_game(self):
        return len(self._current_sg_id) > 0

    @property
    def item_card_origin_bet(self):
        return self.item_status.get("origin_bet", self.current_bet)

    @property
    def item_use_log_id(self):
        return self.item_status.get("item_use_log_id", "")

    # @property
    # def is_fever_started(self):
    #     # before starting playing fever game
    #     return  (self.is_fever and self._fever['current_table'] >=0)

    # @property
    # def fever_data(self):
    #     return self._fever
    @property
    def current_special_game_data(self):
        if len(self._current_sg_id) > 0:
            return self._special_game_state[str(self._current_sg_id[0])]
        else:
            return None
    @property
    def next_special_game_data(self):
        if len(self._current_sg_id) > 1:
            return self._special_game_state[str(self._current_sg_id[1])]
        else:
            return None
    # @property
    # def fever_current_level(self):
    #     return self._fever['current_level']
    @property
    def current_special_game_level(self):
        if len(self._current_sg_id) > 0:
            return self._special_game_state[str(self._current_sg_id[0])]['current_level']
        else:
            return 0
    @property
    def next_special_game_level(self):
        if len(self._current_sg_id) > 1:
            return self._special_game_state[str(self._current_sg_id[1])]['current_level']
        else:
            return 0
    @property
    def one_play_win_amount(self):
        return self._one_play_win_amount

    @property
    def original_game_win_amount(self):
        return self._original_game_win_amount

    @property
    def last_game_win_amount(self):
        return self._last_game_win_amount

    @property
    def special_game_play_times_in_a_row(self):
        return self._special_game_play_times_in_a_row

    # @special_game_play_times_in_a_row.setter
    # def special_game_play_times_in_a_row(self, value):
    #     self._special_game_play_times_in_a_row = value

    @property
    def special_game_view_reels(self):
        if len(self._current_sg_id) > 0:
            if str(self._current_sg_id[0]) in self._last_fever_reels:
                return self._last_fever_reels[str(self._current_sg_id[0])]
        else:
            return None
    @property
    def last_main_reels(self):
        return self._last_main_reels
    @property
    def recovery_bet_lines(self):
        if self.is_special_game:
            return self.current_special_game_data['current_line']  # TODO: return self._fever['current_script'][str(0)]['bet_lines']
        return None
    @property
    def recovery_line_bet(self):
        if self.is_special_game:
            # print 'recovery_line_bet: ', str(self.current_special_game_data['current_bet'])
            return self.current_special_game_data['current_bet']
        return None
    @property
    def league_id(self):
        return self._league_id
    @property
    def all_sg_id(self):
        return self._current_sg_id
    @property
    def current_sg_id(self):
        return self._current_sg_id[0] if len(self._current_sg_id) > 0 else -1
    @property
    def next_sg_id(self):
        return self._current_sg_id[1] if len(self._current_sg_id) > 1 else -1
    @property
    def win_special_symbols(self):
        win_special_symbols = dict()
        special_game_state = self.current_special_game_data
        if special_game_state is not None:
            win_special_symbols = special_game_state['current_script'].get('win_special_symbols', dict())
        return win_special_symbols
        #return self._win_special_symbols

    @property
    def is_game_buffer_assigned(self):
        return self._game_buffer.get('assigned', False)

    @property
    def extra_info(self):
        return self._extra_info

    @property
    def extra_bet(self):
        return self._extra_bet

    @property
    def probId(self):
        return self._probId

    @property
    def extra_bet_on(self):
        return self._extra_bet
    
    @property
    def game_no(self):
        return self._game_no

    @game_no.setter
    def game_no(self, value):
        self._game_no = value

    @property
    def current_bet(self):
        return self._current_bet

    @current_bet.setter
    def current_bet(self, value):
        self._current_bet = value

    @property
    def current_line(self):
        return self._bet_lines


    def get_special_game_view_reels(self, sg_id):
        if sg_id in self._current_sg_id:
            if str(sg_id) in self._last_fever_reels:
                return self._last_fever_reels[str(sg_id)]
        else:
            return None

    def get_special_game_state(self, sg_id):
        if str(sg_id) in self._special_game_state:
            return self._special_game_state[str(sg_id)]
        else:
            return None

    def get_special_game_last_reels(self, sg_id):
        if str(sg_id)in self._last_fever_reels:
            return self._last_fever_reels[str(sg_id)]
        else:
            return None

    def as_json_dict(self, specify_attr=None):
        if (specify_attr is not None) and (isinstance(getattr(self, specify_attr, None), dict)):
            return getattr(self, specify_attr, None)
        data = {}
        for key in self.__dict__.keys():
            if not key.startswith('_'): #private attribute
                continue
            data[key[1:]] = getattr(self, key, None)
        #print 'as json data='+ppretty(data, seq_length=1000)
        data['update_time'] = int(time.time())
        return data

    def check_state_before_fever(self, sg_id):
        return self.is_special_game and self.current_sg_id == sg_id

    def end_special_game(self, sg_id):
        if sg_id in self._current_sg_id:
            self._current_sg_id.remove(sg_id)
            del self._special_game_state[str(sg_id)]
            #del self._last_fever_reels[str(sg_id)]
        elif sg_id == -1 and "-1" in self._special_game_state:
            del self._special_game_state[str(sg_id)]
        # 接下來沒有特殊遊戲，要進行贏分結算動作了
        if not self.is_scatter_game:
            self._one_play_win_amount = 0
            if self._game_buffer.get('assigned', False) is True:
                self._game_buffer['assigned'] = False

    def clean_special_game_state(self):
        self._current_sg_id = list()
        self._special_game_state = dict()

    def win_special_game_state(self, sg_id_list, current_bet, current_line, current_script_dict, spin_reels, is_queue=False):
        # 贏得特殊遊戲，需要紀錄game state
        # 可能同時中很多fever，像是吉祥如意同時中free和jp
        for sg_id in sg_id_list:
            current_script = current_script_dict.get(str(sg_id), dict())
            # current_sg_id原本沒有該sg_id，表示第一次中，需要建立新的game state
            if sg_id not in self._current_sg_id:
                # Queue的方式加入
                temp_special_game_state = self._get_init_special_game_state()
                temp_special_game_state['id'] = sg_id
                temp_special_game_state['current_bet'] = current_bet
                # print 'win_special_game_state current_bet: ', str(current_bet)
                temp_special_game_state['current_line'] = current_line
                temp_special_game_state['current_level'] = 1
                temp_special_game_state['current_script'] = current_script
                temp_special_game_state['is_item_trigger'] = self.is_item_trigger
                temp_special_game_state['item_status'] = self.item_status
                temp_special_game_state['bonus_trigger_type'] = self.temp_bonus_trigger_type
                temp_special_game_state['bonus_data'] = self.temp_bonus_data
                self._special_game_state[str(sg_id)] = temp_special_game_state
                #print type(self._last_fever_reels)
                self._last_fever_reels[str(sg_id)] = spin_reels
                # current_sg_id存在該sg_id，表示中了又中，需要增加total次數、更新game state
                #fake_game_state.update(temp_special_game_state)
                #print 'fake_db game_state='+ppretty(fake_game_state, seq_length=1000)
                if is_queue:
                    self._current_sg_id.append(sg_id)
                # Stack的方式加入
                else:
                    self._current_sg_id = [sg_id] + self._current_sg_id
            else:
                special_game_state = self._special_game_state[str(sg_id)]
                special_game_state['current_script'].update(current_script)
                #fake_game_state.update(special_game_state)

    def update_by_spin_result(self, spin_result, line_bet, bet_lines, jp_win=0, probId=None):
        # resouce from _check_game_state_from_spin_result()

        self._bet_lines = bet_lines
        self._current_bet = line_bet  #20210608  修改
        self._one_play_win_amount += (spin_result.this_win + jp_win)
        self._original_game_win_amount = spin_result.this_win
        self._last_game_win_amount = self._original_game_win_amount
        self._special_game_play_times_in_a_row = 0
        total_win = self._one_play_win_amount

        # self._fever['current_base'] = spin_result.fever_base
        ## 是否有中 fever，更新 game_state.fever
        if spin_result.is_win_special_game:
            current_bet = line_bet
            current_line = self._bet_lines
            sg_id_list = spin_result.win_special_game_id_list
            spin_reels = spin_result.spin_reels
            current_script_dict = copy.deepcopy(spin_result.special_game_current_script)
            self.win_special_game_state(sg_id_list, current_bet, current_line, current_script_dict, spin_reels)
            self._feverProbId = probId

        if not spin_result.is_win_special_game:
            self._one_play_win_amount = 0

        if not self.is_special_game:
            self._one_play_win_amount = 0
            self._league_id = ""

        if self.is_item_trigger:
            self.end_special_game(-1)

        self._last_main_reels = spin_result.spin_reels
        if spin_result.has_game_buffer:
            self._game_buffer['assigned'] = True

        return total_win   # one_play_win_amount

    def update_by_fever_result(self, fever_result, jp_win=0):
        win_amount = fever_result.win_amount
        self._one_play_win_amount += (win_amount + jp_win)
        self._original_game_win_amount += win_amount
        self._last_game_win_amount = self._original_game_win_amount
        self._special_game_play_times_in_a_row = 0
        total_win = self._one_play_win_amount

        return total_win

    def update_by_double_result(self, double_result):
        total_win = double_result.total_win
        self._last_game_win_amount = double_result.total_win
        self._special_game_play_times_in_a_row += 1
        print('self._special_game_play_times_in_a_row', self._special_game_play_times_in_a_row)
        self._one_play_win_amount = 0
        return total_win

    def update_last_main_reels(self, recovery_reels):
        self._last_main_reels = recovery_reels

    def update_last_fever_reels(self, sg_id, recovery_reels, also_update_main_reels=False):
        if len(recovery_reels) > 0:
            self._last_fever_reels[str(sg_id)] = recovery_reels
            if also_update_main_reels:
                self._last_main_reels = recovery_reels

    def update_league_id(self, league_id):
        self._league_id = league_id

    def update_extra_info(self, extra_info):
        self._extra_info = extra_info

    def update_extra_bet(self, extra_bet):
        self._extra_bet = extra_bet

    def update_bonus_data(self, bonus_trigger_type, bonus_data):
        self.temp_bonus_trigger_type = bonus_trigger_type
        self.temp_bonus_data = bonus_data

    def updateProbId(self, probId):
        self._probId = probId

    def _init_game_state(self, game_state):
        if game_state is None:
            return

        # print '_init_game_state: ', game_state
        #self._fever = game_state['fever']
        self._last_fever_reels = game_state.get('last_fever_reels', dict())      # fever最後盤面
        self._last_main_reels = game_state.get('last_main_reels', dict())       # 最後盤面
        self._one_play_win_amount = game_state.get('one_play_win_amount', 0)
        self._original_game_win_amount = game_state.get('original_game_win_amount', 0)
        self._last_game_win_amount = game_state.get('last_game_win_amount', 0)
        self._special_game_play_times_in_a_row = game_state.get('special_game_play_times_in_a_row', 0)
        self._game_no = game_state.get('game_no', 0)
        self._current_bet = game_state.get('current_bet', 0)

        self._bet_lines = game_state.get('bet_lines', 0)      # 進入特殊遊戲時的當下線數
        #self._league_id = game_state.get('league_id','')
        self._current_sg_id = game_state.get('current_sg_id', list())
        self._win_special_symbols = game_state.get('win_special_symbols', dict())
        self._special_game_state = game_state.get('special_game_state', dict())
        self._game_buffer = game_state.get('game_buffer', dict())
        self._extra_info = game_state.get('extra_info', dict())
        self._extra_bet = game_state.get('extra_bet', None)
        self._probId = game_state.get('probId', '')
        self._feverProbId = game_state.get('feverProbId', '')
        self._currency = game_state.get('currency', "")
        self._Channel = game_state.get('Channel')
        self._cache_credit = game_state.get("cache_credit")
        self.temp_bonus_trigger_type = self.bonus_trigger_type
        self.temp_bonus_data = self.bonus_data

        #print '---init game state'+str(game_state)

    def _get_init_special_game_state(self):
        init_state = {
            'id': 0,
            'this_win': 0,
            'total_win': 0,
            'current_bet': 0,
            'current_line': 0,
            'current_level': 0,
            'current_script': {}
        }
        return init_state

    def set_cache_credit(self, credit):
        self._cache_credit = credit

    def update_credit_change(self, delta):
        self._cache_credit += delta

    @property
    def cache_credit(self):
        return self._cache_credit
