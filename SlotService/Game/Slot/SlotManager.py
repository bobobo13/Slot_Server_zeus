#!/usr/bin/python
# -*- coding: utf-8 -*-
import traceback

'''
Slot遊戲流程+dispatch到各遊戲
'''
class SlotManager():
    MaxNextFeverCallTimes = 200
    def __init__(self, logger, **kwargs):
        self.name = 'SlotManager'
        self.logger = logger
        self.DataSource = kwargs.pop("DataSource", None)

        self.CallSlotMachineFunc = kwargs.get('CallSlotMachineFunc', self._CallSlotMachine)
        self._MachineSession = dict()
        self.get_machine_config = None
        self.get_machin_url_map = None
        self.get_machine_session = None
        self.get_chance = None

    def register_get_machine_config(self, func):
        self.get_machine_config = func

    def register_get_machine_url_map_func(self, func):
        self.get_machin_url_map = func

    def register_get_machine_session(self, func):
        self.get_machine_session = func

    def register_get_chance(self, func):
        self.get_chance = func

    def start_game(self, ark_id, game_name, fs_setting, gn_data, platform_data, game_data):
        """
        'platform_bet_info':{
            'BetList': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'MaxBet': 10,
            'MinBet': 1,
            'MaxWin': 1000000,
            'MaxOdds': 1000,
            'RTP': 97,
            'ExtraBetRtp': 97}
        """
        enable_game = fs_setting.get('EnableGame', False)
        if not enable_game:
            return self._Result(result={'id': -200004, 'msg': 'MACHINE CLOSE'})

        user_game_state = game_data['game_state_data']
        if user_game_state is not None and len(user_game_state["current_sg_id"]) > 0:
            # 取得機率表Id，特殊遊戲從command_data 抓
            if 'command_data' not in user_game_state:
                self.logger.error("[SlotManager] start_game command_data is None. {}".format(user_game_state))
            command_data = user_game_state['command_data']
            if 'gn_data' not in command_data:
                self.logger.error("[SlotManager] start_game gn_data is None. {}".format(command_data))
            gn_data = command_data['gn_data']
        else:
            merchant_rtp = str(int(100 * platform_data["RTP"]))
            func_group = gn_data['FunctionGroup']
            is_chance_from_db, prob_group_name, chance_key = self.get_chance(func_group, merchant_rtp, game_name, ark_id, assign_prob_id=None)
            gn_data['IsChanceFromDb'] = is_chance_from_db
            gn_data['ChanceKey'] = chance_key

        game_data['use_remote_game_state'] = True
        if game_name not in self.get_machin_url_map():
            return self._Result(result={'id': -200048, 'msg': 'MACHINE NOT EXIST'})
        ret = self._call_slot_machine('start_game', ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        if ret is None:
            return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL'})
        return ret

    def spin(self, ark_id, game_name, fs_setting, gn_data, platform_data, game_data):
        enable_game = fs_setting.get('EnableGame', False)
        if not enable_game:
            return self._Result(result={'id': -200004, 'msg': 'MACHINE CLOSE'})
        each_win_amount = []

        # 取得機率表Id
        merchant_rtp = str(int(100 * platform_data["RTP"]))
        func_group = gn_data['FunctionGroup']
        is_chance_from_db, prob_group_name, chance_key = self.get_chance(func_group, merchant_rtp, game_name, ark_id, assign_prob_id=None)
        gn_data['IsChanceFromDb'] = is_chance_from_db
        gn_data['ChanceKey'] = chance_key

        user_game_state = game_data['game_state_data']
        if user_game_state is not None:
            user_game_state.pop('command_data', None)

        game_data['use_remote_game_state'] = True
        if game_name not in self.get_machin_url_map():
            return self._Result(result={'id': -200048, 'msg': 'MACHINE NOT EXIST'})
        ret_result = self._call_slot_machine('spin', ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        if ret_result is None:
            return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL'})

        if 'result' not in ret_result or 'id' not in ret_result['result'] or ret_result['result']["id"] != 0 or ret_result.get("data") is None:
            self.logger.error("[SlotManager] machine spin error: {}".format(ret_result))
            return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL'})

        ret_user_game_state = ret_result.pop("user_game_state", {})
        game_sn = 0
        game_data.pop('dev_mode', None)  # 清除dev_mode
        ret_user_game_state['game_no'] = gn_data['GameNo']
        ret_user_game_state['game_sn'] = game_sn
        ret_user_game_state['command_data'] = {'fs_setting': fs_setting, 'gn_data': gn_data, 'platform_data': platform_data, 'game_data': game_data}

        spin_result = ret_result["data"]
        each_win_amount.append(spin_result['this_win_amount']) # 加入spin 贏分

        log_doc = {
            'HistoryDetail': ret_result.pop("history_detail", None),
            'AnalyticLog': ret_result.pop("AnalyticLog", None),
            'DetailBetWinLog': ret_result.pop("DetailBetWinLog", None)
        }
        log_doc = {k:v for k, v in log_doc.items() if v is not None}

        current_game_return = dict({'GameSn': game_sn, 'GameResult': ret_result, 'GameState': user_game_state, 'IsGameOver': True}, **log_doc)  # record_game_return: main game 要記錄的result跟game_state
        fever_game_return = None

        slot_machine_config = self.get_machine_config(game_name)
        batch_fever_enable = slot_machine_config['BatchFeverEnable']
        if batch_fever_enable and self.check_hit_fever(ret_user_game_state):
            ret_user_game_state.pop('command_data', None)
            batch_fever_id = slot_machine_config['BatchFeverId']
            ret_user_game_state, fever_game_return, each_fever_win_amount = self._next_fever(ark_id, game_name, fs_setting, gn_data, platform_data, game_data, ret_user_game_state, batch_fever_enable, batch_fever_id)
            if ret_user_game_state is None:
                return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL(1)'})
            each_win_amount.extend(each_fever_win_amount)

        ret =  {"CurrentGameReturn": current_game_return, "FeverGameReturn": fever_game_return, "AfterUserGameState": ret_user_game_state, "EachWinAmount": each_win_amount}
        return self._Result(result={'id': 0}, data=ret)

    def check_hit_fever(self, user_game_state):
        return ('current_sg_id' in user_game_state) and len(user_game_state['current_sg_id']) > 0

    def next_fever(self, ark_id, game_name, user_game_state, client_action_data=None, fever_all=False, dev_mode=0):
        slot_machine_config = self.get_machine_config(game_name)
        batch_fever_enable = slot_machine_config['BatchFeverEnable']
        batch_fever_id = slot_machine_config['BatchFeverId']
        each_win_amount = []

        tmp_command_data = user_game_state.pop('command_data', {})
        fs_setting, gn_data, platform_data, game_data = (
            tmp_command_data.get('fs_setting', {}),
            tmp_command_data.get('gn_data', {}),
            tmp_command_data.get('platform_data', {}),
            tmp_command_data.get('game_data', {})
        )
        game_data['use_remote_game_state'] = True
        game_data['dev_mode'] = dev_mode

        ret_user_game_state, fever_game_return, each_fever_win_amount = self._next_fever(ark_id, game_name, fs_setting, gn_data, platform_data, game_data, user_game_state, batch_fever_enable, batch_fever_id, client_action_data=client_action_data, fever_all=fever_all)
        if ret_user_game_state is None:
            return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL(1)'})
        each_win_amount.extend(each_fever_win_amount)

        current_game_return = fever_game_return.pop(0) if len(fever_game_return) > 0 else None  # record_game_return: main game 要記錄的result跟game_state
        # record_fever_game_return: fever game 要記錄的result跟game_state
        ret = {"CurrentGameReturn": current_game_return, "FeverGameReturn": fever_game_return, "AfterUserGameState": ret_user_game_state, "EachWinAmount": each_win_amount}
        return self._Result(result={'id': 0}, data=ret)

    def _next_fever(self, ark_id, game_name, fs_setting, gn_data, platform_data, game_data, origin_game_state, batch_fever_enable, batch_fever_id, client_action_data=None, fever_all=False):
        fever_game_return = []
        each_fever_win_amount = []
        ret_fever_user_game_state = None

        ori_sg_id = origin_game_state['current_sg_id'][0]
        game_data["sg_id"] = ori_sg_id
        #  遇到選擇型的時候，需將client_action_data帶入
        if client_action_data is not None and str(ori_sg_id) in client_action_data:
            cmd_clent_action = client_action_data[str(ori_sg_id)]
            cmd_clent_action['client_sg_id'] = str(ori_sg_id)
            game_data['client_action_data'] = cmd_clent_action
        game_data["game_state_data"] = origin_game_state
        game_sn = origin_game_state['game_sn']
        game_sn += 1
        for idx in range(SlotManager.MaxNextFeverCallTimes):
            ret_fever_result = self._call_slot_machine('next_fever', ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
            if ret_fever_result is None or ret_fever_result['result']["id"] != 0 or ret_fever_result.get("data") is None:
                return None, None, None, None
            ret_fever_user_game_state = ret_fever_result.pop("user_game_state", {})
            ret_fever_user_game_state['game_sn'] = game_sn
            is_game_over = len(ret_fever_user_game_state["current_sg_id"]) <= 0
            game_data["dev_mode"] = 0

            log_doc = {
                'HistoryDetail': ret_fever_result.pop("history_detail", None),
                'AnalyticLog': ret_fever_result.pop("AnalyticLog", None),
                'DetailBetWinLog': ret_fever_result.pop("DetailBetWinLog", None)
            }
            log_doc = {k: v for k, v in log_doc.items() if v is not None}
            game_state = dict(game_data["game_state_data"], **{"command_data": {'fs_setting': fs_setting, 'gn_data': gn_data, 'platform_data': platform_data, 'game_data': game_data}})
            fever_game_return.append(dict({'GameSn': game_sn, 'GameResult': ret_fever_result, 'GameState': game_state, 'IsGameOver': is_game_over}, **log_doc))
            each_fever_win_amount.append(ret_fever_result['data']['this_win_amount'])
            if is_game_over:
                break

            if not fever_all:
                if not batch_fever_enable:
                    # 未完成FeverGame，需存下當時的command_data
                    ret_fever_user_game_state["command_data"] = {'fs_setting': fs_setting, 'gn_data': gn_data, 'platform_data': platform_data, 'game_data': game_data}
                    break
                elif ret_fever_user_game_state["current_sg_id"][0] not in batch_fever_id:
                    ret_fever_user_game_state["command_data"] = {'fs_setting': fs_setting, 'gn_data': gn_data, 'platform_data': platform_data, 'game_data': game_data}
                    break

            game_sn += 1
            sg_id = ret_fever_user_game_state['current_sg_id'][0]
            game_data["sg_id"] = sg_id
            game_data["game_state_data"] = ret_fever_user_game_state

            #  遇到選擇型的時候，需將client_action_data帶入
            if client_action_data is not None and str(sg_id) in client_action_data:
                cmd_clent_action = client_action_data[str(sg_id)]
                cmd_clent_action['client_sg_id'] = str(sg_id)
                game_data['client_action_data'] = cmd_clent_action
        return ret_fever_user_game_state, fever_game_return, each_fever_win_amount

    # 暫時給外包開發使用
    def bonus_spin(self, ark_id, game_name, fs_setting, gn_data, platform_data, game_data):
        enable_game = fs_setting.get('EnableGame', False)
        if not enable_game:
            return self._Result(result={'id': -200004, 'msg': 'MACHINE CLOSE'})
        each_win_amount = []

        # 取得機率表Id
        merchant_rtp = str(int(100 * platform_data["RTP"]))
        func_group = gn_data['FunctionGroup']
        assign_prob_id = gn_data['assign_prob_id']
        is_chance_from_db, prob_group_name, chance_key = self.get_chance(func_group, merchant_rtp, game_name, ark_id, assign_prob_id=assign_prob_id)
        gn_data['IsChanceFromDb'] = is_chance_from_db
        gn_data['ChanceKey'] = chance_key

        user_game_state = game_data['game_state_data']
        if user_game_state is not None:
            user_game_state.pop('command_data', None)

        game_data['use_remote_game_state'] = True
        if game_name not in self.get_machin_url_map():
            return self._Result(result={'id': -200048, 'msg': 'MACHINE NOT EXIST'})
        ret_result = self._call_slot_machine('bonus_spin', ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        if ret_result is None:
            return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL'})

        if 'result' not in ret_result or 'id' not in ret_result['result'] or ret_result['result']["id"] != 0 or ret_result.get("data") is None:
            self.logger.error("[SlotManager] machine bonus_spin error: {}".format(ret_result))
            return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL'})

        ret_user_game_state = ret_result.pop("user_game_state", {})
        game_sn = 0
        game_data.pop('dev_mode', None)  # 清除dev_mode
        ret_user_game_state['game_no'] = gn_data['GameNo']
        ret_user_game_state['game_sn'] = game_sn
        ret_user_game_state['command_data'] = {'fs_setting': fs_setting, 'gn_data': gn_data, 'platform_data': platform_data, 'game_data': game_data}

        spin_result = ret_result["data"]
        each_win_amount.append(spin_result['this_win_amount'])  # 加入spin 贏分

        log_doc = {
            'HistoryDetail': ret_result.pop("history_detail", None),
            'AnalyticLog': ret_result.pop("AnalyticLog", None),
            'DetailBetWinLog': ret_result.pop("DetailBetWinLog", None)
        }
        log_doc = {k: v for k, v in log_doc.items() if v is not None}

        current_game_return = dict(
            {'GameSn': game_sn, 'GameResult': ret_result, 'GameState': user_game_state, 'IsGameOver': True},
            **log_doc)  # record_game_return: main game 要記錄的result跟game_state
        fever_game_return = None

        slot_machine_config = self.get_machine_config(game_name)
        batch_fever_enable = slot_machine_config['BatchFeverEnable']
        if batch_fever_enable and self.check_hit_fever(ret_user_game_state):
            ret_user_game_state.pop('command_data', None)
            batch_fever_id = slot_machine_config['BatchFeverId']
            ret_user_game_state, fever_game_return, each_fever_win_amount = self._next_fever(ark_id, game_name,
                                                                                             fs_setting, gn_data,
                                                                                             platform_data, game_data,
                                                                                             ret_user_game_state,
                                                                                             batch_fever_enable,
                                                                                             batch_fever_id)
            if ret_user_game_state is None:
                return self._Result(result={'id': -200051, 'msg': 'MACHINE API FAIL(1)'})
            each_win_amount.extend(each_fever_win_amount)

        ret = {"CurrentGameReturn": current_game_return, "FeverGameReturn": fever_game_return, "AfterUserGameState": ret_user_game_state, "EachWinAmount": each_win_amount}
        return self._Result(result={'id': 0}, data=ret)

    def _Result(self, nResult=0, Src=None, *args, **kwargs):
        r = {} if 'OutParam' not in kwargs else kwargs['OutParam']
        r['Code'] = nResult
        # 透過args挑出想留下的欄位
        if Src is not None:
            if len(args) <= 0:
                r.update(Src)
            else:
                for k in args:
                    r[k] = Src[k]
        # 透過kwargs合併欄位
        r.update(kwargs)
        r.pop('OutParam', None)
        '''
        for k in kwargs:
            if k != 'OutParam':
                r[k] = kwargs[k]
        '''
        return r

    def _call_slot_machine(self, cmd, ark_id, game_name, fs_setting, gn_data, platform_data, game_data, **kwargs):
        url = "http://" + self.get_machin_url_map()[game_name]+ "/SlotMachine/" + cmd
        return self.CallSlotMachineFunc(url, ark_id, game_name, fs_setting, gn_data, platform_data, game_data, **kwargs)

    def _CallSlotMachine(self, url, ark_id, game_name, fs_setting, gn_data, platform_data, game_data, headers=None, **kwargs):
        # url = "http://" + self._SlotMachineUrlMap[game_name] + cmd
        headers = headers or {'Content-Type': 'application/json'}
        headers.update({'User-Agent': 'My User Agent 1.0'})

        data = {}
        data["ark_id"] = ark_id
        data["game_name"] = game_name
        data["fs_setting"] = fs_setting
        data["gn_data"] = gn_data
        data["platform_data"] = platform_data
        data["game_data"] = game_data
        data.update(kwargs)

        resp, elapsed = None, None
        try:
            machine_session = self.get_machine_session(game_name)
            if machine_session is None:
                return resp
            r = machine_session.post(url, headers=headers, json=data)
            elapsed = r.elapsed.microseconds / 1000
            self.logger.info("[SlotService] url:{}, reqData:{}, resp:{}, elapsed:{}".format(url, data, resp, elapsed))
            resp = r.json()
        except:
            self.logger.error("[SlotService] url:{}, reqData:{}, e:{}".format(url, data, traceback.format_exc()))
        return resp

    def get_code_name(self, game_name):
        ret = self.get_machine_config(game_name)
        if ret is None:
            return None
        return ret.get("CodeName", game_name)

    def get_cost(self, game_name):
        ret = self.get_machine_config(game_name)
        if ret is None:
            return None
        return ret.get("Cost")