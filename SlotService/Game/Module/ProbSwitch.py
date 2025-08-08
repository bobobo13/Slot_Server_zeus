#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
from .SlotMath import SlotMath
from .ProbSwitchDao import ProbSwitchDao

class ProbSwitch:
    def __init__(self, DataSource=None, logger=None, bInitDb=True, **kwargs):
        self.logger = logger
        self._prob_setting = None
        self._gameInfo = None
        self._fNextReload = 0
        self._rule_map = {
            'Default': self.default_rult,
            'SpinCount': self.spin_count_rule,
            'FeverCount': self.fever_count_rule,
            'OddsCount': self.odds_count_rule
        }
        self._dao = ProbSwitchDao(DataSource, logger, bInitDb, **kwargs)
        self.Reload(True)

    def Reload(self, bForce=True):
        # 既不強制reload,且Reload時間還沒到
        if (not bForce) and (self._fNextReload > 0) and (self._fNextReload > time.time()):
            return
        self._fNextReload = time.time() + 60  # 1分鐘reload
        self.load_setting()

    def load_setting(self):
        self._prob_setting = self._dao.load_setting()

    def is_prob_switch(self, game_name):
        if game_name not in self._prob_setting:
            return False
        if "Enable" not in self._prob_setting[game_name] or not self._prob_setting[game_name]["Enable"]:
            return False
        return True

    def get_prob(self, game_name, ark_id):
        """
        _prob_setting:{
            "GameName": "",
            "Result":["Group1","Group2","Group3"],
            "Weight":[1, 2, 1],
            "Group1": {"Type":"SpinCount", "Condition":[-1, 200],"Result":["A", "D"]},
            "Group2": {"Type":"FeverCount", "Condition":[-1, 200],"Result":["A", "D"]},
            "Group3": {"Type":"OddsCount", "Condition":[-1, 200],"Result":["A", "D"]}
        """
        if game_name not in self._prob_setting:
            return None, None
        prob_setting = self._prob_setting[game_name]
        player_prob_data = self._dao.get_prob_data(ark_id, game_name)
        prob_id, group_name, need_init = self.get_rule_prob_id(prob_setting, player_prob_data)
        default_type_doc = {}
        if group_name=="Default":
            default_type_doc = {'default_prob_id': prob_id, 'is_default_type': True}
        if need_init:
            self._dao.init_prob_data(ark_id, game_name, group_name, **default_type_doc)
        return group_name, prob_id

    def get_rule_prob_id(self, prob_setting, player_prob_data):
        need_init = False
        if "Result" not in prob_setting or "Weight" not in prob_setting:
            return None, None, False

        result, weight = prob_setting["Result"], prob_setting["Weight"]

        if len(result) <= 0 or len(result) != len(weight):
            return None, None, False

        if player_prob_data is not None and "Group" in player_prob_data and (player_prob_data["Group"] in prob_setting or player_prob_data["Group"]=="Default"):
            group_name = player_prob_data["Group"]
        else:
            idx, group_name = SlotMath.get_result_by_weight(result, weight)
            need_init = True

        # 針對Default的情況:
        if group_name == 'Default':
            if player_prob_data is not None and "DefaultProbId" in player_prob_data:
                prob_id = player_prob_data["DefaultProbId"]
                return prob_id, group_name, need_init
            default_chance = prob_setting['DefaultChanceResult']
            default_chance_weight = prob_setting['DefaultChanceWeight']
            if len(default_chance) <= 0 or len(default_chance) != len(default_chance_weight):
                self.logger.error("DefaultChanceResult or DefaultChanceWeight is not correct")
                return None, None, False
            _, prob_id = SlotMath.get_result_by_weight(default_chance, default_chance_weight)
            need_init = True
            return prob_id, group_name, need_init

        prob_data = prob_setting[group_name]
        prob_type = prob_data["Type"]
        if type(prob_type) is not list:
            return None, None, False

        res_prob = prob_data["Result"]
        all_type_level = []
        for pt in prob_type:
            if "Condition" not in prob_data[pt]:
                continue
            con = prob_data[pt]["Condition"]
            cnt = player_prob_data["{}Value".format(pt)] if player_prob_data is not None and "{}Value".format(pt) in player_prob_data else 0

            con_idx = None
            for i, c in enumerate(con):
                if cnt >= c:
                    con_idx = i
                    continue
                break
            all_type_level.append(con_idx)

        level = min(all_type_level) if prob_data["Level"] == "Lower" else max(all_type_level)
        prob_id = res_prob[level]
        return prob_id, group_name, need_init

    def check_rule(self, ark_id, game_name, result, game_state, special_game_id, group_name=None):
        if not self.is_prob_switch(game_name):
            return
        prob_setting = self._prob_setting[game_name]

        if group_name is None:
            player_prob_data = self._dao.get_prob_data(ark_id, game_name)
            if player_prob_data is None:
                self.logger.error("ProbSwitch check_rule player_prob_data is None. ark_id: {}, game_name: {}, result: {}, game_state: {}, special_game_id: {}".format(ark_id, game_name, result, game_state, special_game_id))
                return
            group_name = player_prob_data['Group']

        if group_name not in prob_setting:
            self.logger.error("ProbSwitch check_rule group_name is not in prob_setting. ark_id: {}, game_name: {}, result: {}, game_state: {}, special_game_id: {}".format(ark_id, game_name, result, game_state, special_game_id))
            return

        # 預設模式不處理
        if group_name == 'Default':
            return

        prob_data = prob_setting[group_name]
        prob_type = prob_data["Type"]

        if type(prob_type) is not list:
            self.logger.error("ProbSwitch check_rule prob_type is not list. ark_id: {}, game_name: {}, result: {}, game_state: {}, special_game_id: {}".format(ark_id, game_name, result, game_state, special_game_id))
            return

        for pt in prob_type:
            if pt not in self._rule_map:
                continue
            if pt not in prob_data:
                continue
            if self._rule_map[pt](ark_id, game_name, result, game_state, prob_data[pt], special_game_id):
                self._dao.inc_prob_data(ark_id, game_name, pt)
        return

    def default_rult(self, ark_id, game_name, result, game_state, rule_setting, special_game_id):
        return False

    def spin_count_rule(self, ark_id, game_name, result, game_state, rule_setting, special_game_id):
        return not game_state.is_special_game

    def fever_count_rule(self, ark_id, game_name, result, game_state, rule_setting, special_game_id):
        if not game_state.is_special_game:
            return False
        count_special_game = rule_setting.get("CountSpecialGame", [])
        special_game_id = game_state.current_sg_id
        if special_game_id not in count_special_game:
            return False
        special_game_state = game_state.current_special_game_data
        return special_game_state['current_level'] == 1

    def odds_count_rule(self, ark_id, game_name, result, game_state, rule_setting, special_game_id):
        # win = game_state.one_play_win_amount
        if special_game_id >= 0:
            return False
        win = game_state.pre_fever_spin_total_win
        if "Odds" not in rule_setting:
            return False
        odds = rule_setting["Odds"]
        bet_value = game_state.current_bet
        bet_line = game_state.current_line
        return win > bet_value * bet_line * odds


if "__main__" == __name__:
    # from player_game_state import PlayerGameState

    # ps = ProbSwitch()
    # game_state = PlayerGameState(None)
    #
    # ark_id = "10000001"
    # game_name = "SpinOfFate"
    # result =None
    # player_data = None
    # group_name, prob_id = ps.get_prob(game_name, ark_id)
    # ps.check_rule(ark_id, game_name, result, game_state) # 1 2 3 10 11
    # print prob_id
    pass










