#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import random
from datetime import datetime, UTC, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
import traceback

from .Server.Network.System import System
from .Slot.SlotDao import SlotDao
from .Module.FunctionSwitch import FunctionSwitch
from .Module.DisconnectSettle import DisconnectSettle
from .Module.ProbSwitch import ProbSwitch
from .Module.Util import Copy as copy
from .IngameJp.IngameJpDao import IngameJpMongoDao, IngameJpPoolDao
from .IngameJp.InGameJpMgr import InGameJpMgr

'''
Command入口+Slot週邊機制流程
'''
class SlotSystem(System):
    UTC_PLUS_8 = timezone(timedelta(hours=8))
    UTC_PLUS_0 = timezone.utc
    def __init__(self, server, logger, **kwargs):
        super(SlotSystem, self).__init__(server, passTime=1)
        self.logger = logger
        self.is_test = server.is_test
        self._Dao = SlotDao(logger, **kwargs)
        self._FuncSwitch = FunctionSwitch(logger, kwargs.get("SlotDataSource"), bInitDb=True)

        self.slot_manager = kwargs['SlotManager']
        self.slot_manager.register_get_machine_config(self.get_machine_config)
        self.slot_manager.register_get_machine_url_map_func(self.get_machin_url_map)
        self.slot_manager.register_get_machine_session(self.get_machine_session)
        self.slot_manager.register_get_chance(self.get_chance_key)

        self.register('START_GAME', self.onStartGame)
        self.register('SPIN', self.onSpin)
        self.register('NEXT_FEVER', self.onNextFever)
        self.register('GET_BONUS_INFO', self.onGetBonusInfo)
        self.register('BONUS_SPIN', self.onBonusSpin)
        self.register('get_in_game_jp_info', self.onGetIngameJpInfo)


        self.GetPlayerDataFunc = kwargs.get("GetPlayerDataFunc", None)
        self.GetGameDataFunc = kwargs.get("GetGameDataFunc", None)
        self._WalletManager = kwargs.get("WalletManager", None)
        self.KioskBuffer = kwargs.get("KioskBuffer", None)
        self._pixiuLobbyUrl = kwargs.get("PixiuLobbyUrl")


        self.BuyBonus = kwargs.get("BuyBonus", None)
        self.CommonBuffer = kwargs.get("CommonBuffer", None)
        self.BuyBonusBuffer = kwargs.get("BuyBonusBuffer", None)
        self._BonusModuleMap = {}
        self.register_slot_bonus("BuyBonus", self.BuyBonus)


        if False:
            self._IngameJpMongoDao = IngameJpMongoDao(logger=self.Logger, DataSource=kwargs.get("IngameJpMongo"), GetPlayerData=self.GetPlayerDataFunc)
            self._IngameJpPoolDao = IngameJpPoolDao(logger=self.Logger, DataSource=kwargs.get("IngameJpMongo"), **kwargs)
            self._IngameJpMgr = InGameJpMgr(logger=self.Logger, ParseGroup=self.parseGroup, MongoDao=_IngameJpMongoDao, PoolDao=_IngameJpPoolDao)


        self.disconnect_settle = DisconnectSettle(self.logger, self._Dao,
                                                 DataSource=kwargs['SlotDataSource'],
                                                 LogDataSource=kwargs['BackendLogDataSource'],
                                                 DoFeverAllFunc=self.do_fever_all,
                                                 SettlePlayerGameFunc=self.settle_player_game,
                                                 ForceLockGameStateFunc=self.ForceLockGameState,
                                                 SaveGameStateFunc=self.SaveGameState)

        self.prob_switch = ProbSwitch(DataSource=kwargs['SlotDataSource'], logger=self.logger)

        self._next_reload = {
            'put_queue_log': {'Func': self.put_queue_log, 'NextReload': 0, 'Interval': 1},
            'pixiuLobbySession': {'Func': self._create_pixiu_session, 'NextReload': 0, 'Interval': 600},
            'load_machine_config': {'Func': self.load_machine_config, 'NextReload': 0, 'Interval': 30},
            'machineSession': {'Func': self._create_machine_session, 'NextReload': 0, 'Interval': 600},
            'prob_switch_setting': {'Func': self.prob_switch.load_setting, 'NextReload': 0, 'Interval': 60}
        }
        self.MachineConfig = {}
        self.MachineUrlMap = {}
        self.MachineSession = {}
        self.Reload(True)

    def register_slot_bonus(self, bonus_type, bonus_module):
        self._BonusModuleMap[bonus_type] = bonus_module
        if bonus_type == "FreeGameCardBonus":
            self.UseFreeGameCard = bonus_module.UseFreeGameCard
            bonus_module._CheckGameState = self.GetGameState


    def update(self, passTime):
        self.Reload(False)  # 重讀設定檔

    # ========== Reload ==========
    def Reload(self, bForce=True):
        now = time.time()
        for func_name , item in self._next_reload.items():
            func, _fNextReload, interval = item['Func'], item['NextReload'], item['Interval']
            if (not bForce) and (_fNextReload>0) and _fNextReload > now:
                continue
            item['NextReload'] = int(now) + int(interval)
            func()

    def load_machine_config(self):
        self.MachineConfig = self._Dao.load_slot_machine()
        self.MachineUrlMap = {k: v['GameUrl'] for k, v in self.MachineConfig.items() if 'GameUrl' in v}

    def get_machine_config(self, game_name):
        return self.MachineConfig.get(game_name)

    def get_machin_url_map(self):
        return self.MachineUrlMap

    def get_machine_session(self, game_name):
        return self.MachineSession.get(game_name)

    def put_queue_log(self):
        self._Dao.put_queue_log()
        return

    def _create_pixiu_session(self):
        self._pixiuLobbySession = self.create_api_session()
        self.logger.info("[SlotSystem] _create_pixiu_session success")
        return

    def _create_machine_session(self):
        for i in self.MachineUrlMap.keys():
            self.MachineSession[i] = self.create_api_session()
        self.logger.info("[SlotSystem] _create_machine_session success")
        return

    def create_api_session(self):
        sission = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=200, pool_block=True)
        sission.mount('http://', adapter)
        sission.mount('https://', adapter)
        return sission

    # ↑↑↑↑↑↑↑↑↑↑↑ Reload ↑↑↑↑↑↑↑↑↑↑↑

    def onStartGame(self, ark_id, cmd_data, data):
        self.logger.info("[SlotSystem] onStartGame: ark_id={}, cmd_data={}".format(ark_id, cmd_data))
        game_name = cmd_data["GameName"]

        # Read info from userinfo
        platform_data = self.GetGameDataFunc(ark_id, bSecondary=False)   # 由於登入寫入UserInfo.GameData到StartGame讀取的時間很短，可能來不及更新資料導致出錯，採取直接讀Primary
        func_group = platform_data.get("FuncGroup", "test" if self.is_test else "default")
        # self.logger.info("[SlotSystem] onStartGame: ark_id={}, game_name={}, platform_data:{}, func_group={}".format(ark_id, game_name, platform_data, func_group))
        if not self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableGame"):
            # self.logger.info("[SlotSystem] onStartGame: GameName={} not found".format(game_name))
            return self._Result(Code=-1, Msg="GameName={} not found".format(game_name))

        gn_data = {
            # "ChanceKey": str(int(100 * platform_data["RTP"]))
            "FunctionGroup": func_group
        }

        #  斷線結算的玩家需要先處理未完成的遊戲
        user_game_state = self._Dao.GetGameState(ark_id, game_name, bSecondary=True)
        if self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableDisconnectSettle"):
            self.disconnect_settle.OnStartGame(ark_id, game_name, user_game_state.get("lock", 0))
            user_game_state = self._Dao.GetGameState(ark_id, game_name, bSecondary=False)
            if user_game_state.get("lock", 0) != 0:
                self.logger.error('[SlotSystem] start_game: settle went wrong, user_id={}, game_id={}'.format(ark_id, game_name))
                return self._Result(result={'id': -200029, 'msg': 'MAINGAME STATE ERROR'})
        else:  #  先取盤面存的game_state，取不到在拿玩家當前game_state
            player_data_game_return = self._Dao.get_data_game_return(ark_id, game_name)
            if player_data_game_return is None:  # DB ERROR
                return {'id': -200033, 'msg': 'GET GAME STATE ERROR'}
            if isinstance(player_data_game_return, dict) and len(player_data_game_return) > 0:
                user_game_state = player_data_game_return['GameState']

        game_data = {}
        game_data['game_state_data'] = user_game_state

        fs_setting = self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group)
        resp = self.slot_manager.start_game(ark_id, game_name,fs_setting=fs_setting, platform_data=platform_data, gn_data=gn_data, game_data=game_data)

        if resp is None:
            return self._Result(Code=-999, Msg="Call SlotService fail")
        if resp['result']['id'] != 0:
            return self._Result(resp['result']['id'], Src=resp)

        # Proceed result
        respdata = resp["data"]
        if "DefaultBetIndex" in platform_data and "BetList" in platform_data and platform_data["DefaultBetIndex"] < len(platform_data["BetList"]):
            defaultBet = platform_data["BetList"][platform_data["DefaultBetIndex"]]
            if defaultBet in respdata["bet_list"]:
                respdata["DefaultBet"] = defaultBet
            else:
                respdata["DefaultBet"] = respdata["BetList"][0]

        # 檢查玩家當前押注段是否在bet list中
        if 'game_state' in respdata and 'current_bet' in respdata['game_state']:
            current_bet = respdata['game_state']['current_bet']
            if not isinstance(current_bet, int) or current_bet <= 0 or current_bet not in respdata["bet_list"]:
                respdata['game_state']['current_bet'] = respdata["BetList"][0]

        return self._Result(0, Src=resp)

    def onSpin(self, ark_id, cmd_data, data):
        self.logger.info("[SlotSystem] onSpin: ark_id={}, cmd_data={}".format(ark_id, cmd_data))
        game_name = cmd_data["GameName"]
        bet_value = cmd_data["BetValue"]
        if bet_value <= 0:
            return self._Result(Code=-2, Msg="Invalid BetValue:{}".format(bet_value))

        cost = self.slot_manager.get_cost(game_name)
        game_data = {
            "bet_value": bet_value,
            "bet_lines": cost,
            "extra_bet": cmd_data.get("ExtraBet", False),
        }
        user_game_state = self._Dao.GetGameState(ark_id, game_name, bSecondary=True)
        game_data['game_state_data'] = user_game_state

        total_bet = self._GetTotalBet(cmd_data["BetValue"], cost)

        # Read info from userinfo
        platform_data = self.GetGameDataFunc(ark_id)

        time_now = datetime.now(SlotSystem.UTC_PLUS_0)
        date_key = time_now.strftime("%Y%m%d")
        func_group = platform_data.get("FuncGroup", "test" if self.is_test else "default")

        if not self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableGame"):
            return self._Result(Code=-1, Msg="GameName={} not found".format(game_name))

        # 檢查玩家狀態是否在MainGame裡
        player_data_game_return = self._Dao.get_data_game_return(ark_id, game_name)
        if player_data_game_return is None:
            return {'id': -200033, 'msg': 'GET GAME STATE ERROR'}
        if not isinstance(player_data_game_return, dict) or len(player_data_game_return) > 0:
            return self._Result(result={'id': -200029, 'msg': 'MAINGAME STATE ERROR'})

        game_no = self._Dao.GetGameSn()
        wagers_id = game_no
        gn_data = {
            "GameNo": game_no,
            # "ChanceKey": str(int(100*platform_data["RTP"])),
            "FunctionGroup":func_group
        }
        # dev mode
        if self.is_test and "dev_mode" in cmd_data and self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableTestMode"):
            game_data["dev_mode"] = cmd_data["dev_mode"]
            if "fs_data" not in platform_data:
                platform_data["fs_data"] = {}
            platform_data["fs_data"]["EnableTestMode"] = True

        # Buffer
        player_data = self.GetPlayerDataFunc(ark_id)
        no_win_gate, buffer_max_win = None, None
        if self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableBuffer"):
            no_win_gate, buffer_max_win = self.KioskBuffer.get_gate_and_max_win(player_data["ThirdPartyCurrency"], player_data["MerchantId"], game_name, ark_id, nTotalBet=total_bet)

        # Call SlotManager
        current_game_return = {}
        fever_game_return = None
        ret_user_game_state = None
        fs_setting = self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group)
        for i in range(20):
            gameResp = self.slot_manager.spin(ark_id, game_name, fs_setting=fs_setting, platform_data=platform_data, gn_data=gn_data, game_data=game_data)
            if gameResp is None or gameResp['result']["id"] != 0 or gameResp.get("data") is None:
                continue

            gData = gameResp['data']
            current_game_return, fever_game_return, ret_user_game_state, each_win_amount = (
                gData["CurrentGameReturn"],
                gData["FeverGameReturn"],
                gData["AfterUserGameState"],
                gData["EachWinAmount"]
            )

            # if buffer_max_win is None:
            #     break
            if buffer_max_win is not None and sum(each_win_amount) > buffer_max_win:
                current_game_return = {}
                continue
            break

        game_return_processing = self._Dao.data_game_return_processing(ark_id, game_name, game_no, wagers_id, [current_game_return], bet_type="MAINGAME")
        if current_game_return is None or len(current_game_return) <= 0:
            return self._Result(result={'id': -999999, 'msg': 'MAINGAME RETURN PROCESSING ERROR'})
        current_game_return = game_return_processing[0]
        game_result = current_game_return.get('GameResult')
        game_sn = current_game_return['GameSn']

        if game_result is None or game_result['result']["id"] != 0 or game_result.get("data") is None:
            self.logger.error("[SlotSystem] onSpin: resp={}".format(game_result))
            return self._Result(game_result['result']["id"] if game_result is not None else -999, Src=game_result)

        fever_game_return_processing = None
        hit_fever = False
        if fever_game_return is not None and len(fever_game_return) > 0:
            hit_fever = True
            fever_game_return_processing = self._Dao.data_game_return_processing(ark_id, game_name, game_no, None, fever_game_return, bet_type="FEVER")
            if fever_game_return_processing is None or len(fever_game_return_processing) <= 0:
                return self._Result(result={'id': -999999, 'msg': 'FEVERGAME RETURN PROCESSING ERROR'})

        #  結算流程
        code, detail_log, current_game_return, balance, bet_amount, win_amount = self.settle_flow(ark_id, game_name, current_game_return, bet_amount=total_bet, player_data=player_data, bet_type="MAINGAME")
        if code != 0:
            return self._Result(code)

        #  更新遊戲狀態
        code = self._update_user_game_state(ark_id, game_name, ret_user_game_state, fever_game_return_processing, bet_type="MAINGAME")
        if code != 0:
            return self._Result(code)

        #  紀錄Log
        self._log_processing(ark_id, game_name, game_no, wagers_id, game_sn, bet_amount, win_amount, player_data, balance, detail_log, platform_data, game_data, current_game_return, date_key)

        #  活動有效押注累積、Buffer
        self._trigger_events(ark_id, game_name, bet_amount, win_amount, player_data, wagers_id, data, func_group, platform_data, current_game_return)

        if self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableDisconnectSettle") and hit_fever:
            can_settle_now = (len(ret_user_game_state["current_sg_id"]) <= 0)
            self.disconnect_settle.cmd_after(ark_id, game_name, can_settle_now=can_settle_now)

        return self._Result(0, Src=game_result)

    def onNextFever(self, ark_id, cmd_data, data):
        self.logger.info("[SlotSystem] onNextFever: ark_id={}, cmd_data={}".format(ark_id, cmd_data))
        game_name = cmd_data["GameName"]

        client_sg_id = cmd_data.get('sg_id', 0)
        if type(client_sg_id) != int or client_sg_id < 0:
            self.logger.warn('[SlotSystem] fever: client_sg_id error, user_id={}, cmd_data={}'.format(ark_id, cmd_data))
            return self._Result(result={'id': -200032, 'msg': 'GAME IS UNDER CONSTRUCTION'})

        client_action_data = cmd_data.get('data')
        if client_action_data is None:
            client_action_data = {}
        client_action_data['client_sg_id'] = client_sg_id

        # Read info from userinfo
        platform_data = self.GetGameDataFunc(ark_id)
        time_now = datetime.now(SlotSystem.UTC_PLUS_0)
        date_key = time_now.strftime("%Y%m%d")

        func_group = platform_data.get("FuncGroup", "test" if self.is_test else "default")
        if not self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableGame"):
            return self._Result(Code=-1, Msg="GameName={} not found".format(game_name))

        game_data = {}
        dev_mode = 0
        if self.is_test and "dev_mode" in cmd_data and self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableTestMode"):
            dev_mode = cmd_data["dev_mode"]
            if "fs_data" not in platform_data:
                platform_data["fs_data"] = {}
            platform_data["fs_data"]["EnableTestMode"] = True

        # 取得玩家當前Fever遊戲盤面
        ret_user_game_state = None
        fever_game_return = []
        current_game_return = self._Dao.get_and_delete_data_game_return(ark_id, game_name)
        if current_game_return is None:
            user_game_state = self.GetLockGameState(ark_id, game_name, client_sg_id)
            if user_game_state is None:
                return -200033, None, None, None, None, None
            code, current_game_return, fever_game_return, ret_user_game_state, each_win_amount = self.gen_current_game_return(ark_id, game_name, user_game_state=user_game_state, client_action_data=client_action_data, dev_mode=dev_mode)
            if code != 0:
                return self._Result(code)

        game_no = current_game_return['GameNo']
        game_result = current_game_return.get('GameResult')
        game_sn = current_game_return['GameSn']

        fever_game_return_processing = None
        if fever_game_return is not None and len(fever_game_return) > 0:
            fever_game_return_processing = self._Dao.data_game_return_processing(ark_id, game_name, game_no, None, fever_game_return, bet_type="FEVER")
            if fever_game_return_processing is None or len(fever_game_return_processing) <= 0:
                return self._Result(result={'id': -999999, 'msg': 'FEVERGAME RETURN PROCESSING ERROR'})

        # 如果已處理過資產，則直接回傳封包
        if "WalletReturn" in current_game_return:
            return self._Result(0, Src=game_result)

        #  更新遊戲狀態
        code = self._update_user_game_state(ark_id, game_name, ret_user_game_state, fever_game_return_processing, bet_type="FEVER")
        if code != 0:
            return self._Result(code)

        #  不需要進行結算(如: 進入Fever遊戲Init)
        if not self._need_transaction(current_game_return):
            self._Dao.log_game_return(date_key, current_game_return)
            return self._Result(0, Src=game_result)

        player_data = self.GetPlayerDataFunc(ark_id)
        wagers_id = current_game_return['WagersId']

        #  結算流程
        code, detail_log, current_game_return, balance, bet_amount, win_amount  = self.settle_flow(ark_id, game_name, current_game_return, bet_amount=0, player_data=player_data, bet_type="FEVER")
        if code != 0:
            return self._Result(code)

        #  紀錄Log
        self._log_processing(ark_id, game_name, game_no, wagers_id, game_sn, bet_amount, win_amount, player_data, balance, detail_log, platform_data, game_data, current_game_return, date_key)

        #  活動有效押注累積、Buffer
        self._trigger_events(ark_id, game_name, bet_amount, win_amount, player_data, wagers_id, data, func_group, platform_data, current_game_return)

        if self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableDisconnectSettle"):
            if ret_user_game_state is not None:
                can_settle_now = (ret_user_game_state is not None and len(ret_user_game_state["current_sg_id"]) <= 0)
                self.disconnect_settle.cmd_after(ark_id, game_name, can_settle_now=can_settle_now)
        return self._Result(0, Src=game_result)

    def onGetBonusInfo(self, ark_id, cmd_data, data):
        game_name = cmd_data['GameName']
        bonus_type = cmd_data['BonusType']
        if bonus_type not in self._BonusModuleMap or self._BonusModuleMap[bonus_type] is None:
            self.logger.info('[SlotSystem] bonus_spin: BonusType is not exist, user_id={}, cmd_data={}'.format(ark_id, cmd_data))
            return self._Result(result={'id': -200032, 'msg': 'GAME IS UNDER CONSTRUCTION'})
        bonusModule = self._BonusModuleMap[bonus_type]

        player_data = None
        if self.GetPlayerDataFunc is not None:
            player_data = self.GetPlayerDataFunc(ark_id)
        if player_data is None:
            self.logger.error('[SlotSystem] on_get_bonus_info: player={} not exist!'.format(ark_id))
            return self._Result(result={'id': -200004, 'msg': 'USER DOES NOT EXIST'})

        # user_state_doc = self.GetGameState(ark_id, game_name, bSecondary=True)
        # game_state = PlayerGameState(user_state_doc)
        # gameSetting = self.GetGameSetting(game_name, ark_id, player_data, game_state)
        # bet_list = gameSetting['line_bet_list']

        platform_data = self.GetGameDataFunc(ark_id)
        bet_list = platform_data["BetList"]
        ret = bonusModule.get_info(ark_id, game_name, bet_list)
        return ret

    def onBonusSpin(self, ark_id, cmd_data, data):
        self.logger.info("[SlotSystem] onBonusSpin: ark_id={}, cmd_data={}".format(ark_id, cmd_data))
        game_name = cmd_data['GameName']
        bonus_type = cmd_data['BonusType']  # BuyBonus/FreeGameCardBonus
        special_game = cmd_data['SpecialGame']
        bet_value = cmd_data.get('Bet')
        extra_bet = cmd_data.get('ExtraBet')
        model_name = cmd_data.get('Name')  # LegendOfTheWhiteSnake_0
        if bonus_type not in self._BonusModuleMap or self._BonusModuleMap[bonus_type] is None or self._BonusModuleMap[bonus_type] == "FreeGameCardBonus":  # 道具卡只能透過內部觸發，不接受封包
            self.logger.warning('[SlotSystem] bonus_spin: BonusType is not exist, user_id={}, cmd_data={}'.format(ark_id, cmd_data))
            return self._Result(result={'id': -200032, 'msg': 'GAME IS UNDER CONSTRUCTION'})

        bonusModule = self._BonusModuleMap[bonus_type]
        if not isinstance(game_name, str) or not bonusModule.check_game_valid(ark_id, game_name, special_game, model_name, extra_bet):
            self.logger.warning('[SlotSystem] bonus_spin: game_name not exist2, user_id={}, game_id={}, sg_id:{}, model_name:{}, extra_bet:{}'.format(ark_id, game_name, special_game, model_name, extra_bet))
            return self._Result(result={'id': -200032, 'msg': 'GAME IS UNDER CONSTRUCTION'})


        model_info = bonusModule.get_model_info(game_name, model_name)
        if model_info is None:
            self.logger.warning('[SlotSystem] bonus_spin: Bonus Model Info is not exist, user_id={}, game_name={}, name{}'.format(ark_id, game_name, model_name))
            return self._Result(result={'id': -200032, 'msg': 'GAME IS UNDER CONSTRUCTION'})


        cost = self.slot_manager.get_cost(game_name)
        game_data = {
            "bet_value": bet_value,
            "bet_lines": cost,
            "extra_bet": extra_bet if extra_bet is not None else False,
            "bonus_type": bonus_type,
            "special_game": special_game
        }

        # Todo 新世界要將user_game_state另外存
        user_game_state = self._Dao.GetGameState(ark_id, game_name, bSecondary=True)
        game_data['game_state_data'] = user_game_state


        # Read info from userinfo
        platform_data = self.GetGameDataFunc(ark_id)

        # 從SlotBonus 模組取設定資料
        bet_info = bonusModule.get_bet_info(model_info, bet_value, platform_data["BetList"])
        cost_multi = bet_info['CostMulti']
        extra_cost_multi = bet_info['ExtraCostMulti']
        bonus_cost_multi = extra_cost_multi if extra_bet else cost_multi
        assign_prob_id = bet_info.get('ProbId')
        total_bet = self._GetTotalBet(bet_value, cost, bonus_cost_multi=bonus_cost_multi)


        time_now = datetime.now(SlotSystem.UTC_PLUS_0)
        date_key = time_now.strftime("%Y%m%d")
        func_group = platform_data.get("FuncGroup", "test" if self.is_test else "default")

        if not self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableGame"):
            return self._Result(Code=-1, Msg="GameName={} not found".format(game_name))

        # 檢查玩家狀態是否在MainGame裡
        player_data_game_return = self._Dao.get_data_game_return(ark_id, game_name)
        if player_data_game_return is None:
            return {'id': -200033, 'msg': 'GET GAME STATE ERROR'}
        if not isinstance(player_data_game_return, dict) or len(player_data_game_return) > 0:
            return self._Result(result={'id': -200029, 'msg': 'MAINGAME STATE ERROR'})

        game_no = self._Dao.GetGameSn()
        wagers_id = game_no
        gn_data = {
            "GameNo": game_no,
            "FunctionGroup":func_group,
            "assign_prob_id": assign_prob_id,
        }
        # # dev mode
        # if self.is_test and "dev_mode" in cmd_data and self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableTestMode"):
        #     game_data["dev_mode"] = cmd_data["dev_mode"]
        #     if "fs_data" not in platform_data:
        #         platform_data["fs_data"] = {}
        #     platform_data["fs_data"]["EnableTestMode"] = True

        # Buffer
        player_data = self.GetPlayerDataFunc(ark_id)
        # no_win_gate, buffer_max_win = None, None
        # if self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableBuffer"):
        #     no_win_gate, buffer_max_win = self.KioskBuffer.get_gate_and_max_win(player_data["ThirdPartyCurrency"], player_data["MerchantId"], game_name, ark_id, nTotalBet=total_bet)

        # Call SlotManager
        current_game_return = {}
        fever_game_return = None
        ret_user_game_state = None
        fs_setting = self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group)
        for i in range(20):
            gameResp = self.slot_manager.bonus_spin(ark_id, game_name, fs_setting=fs_setting, platform_data=platform_data, gn_data=gn_data, game_data=game_data)
            if gameResp is None or gameResp['result']["id"] != 0 or gameResp.get("data") is None:
                continue

            gData = gameResp['data']
            current_game_return, fever_game_return, ret_user_game_state, each_win_amount = (
                gData["CurrentGameReturn"],
                gData["FeverGameReturn"],
                gData["AfterUserGameState"],
                gData["EachWinAmount"]
            )


            # if buffer_max_win is not None and sum(each_win_amount) > buffer_max_win:
            #     current_game_return = {}
            #     continue
            break

        game_return_processing = self._Dao.data_game_return_processing(ark_id, game_name, game_no, wagers_id, [current_game_return], bet_type="BUYBONUS")
        if current_game_return is None or len(current_game_return) <= 0:
            return self._Result(result={'id': -999999, 'msg': 'BONUS RETURN PROCESSING ERROR'})
        current_game_return = game_return_processing[0]
        game_result = current_game_return.get('GameResult')
        game_sn = current_game_return['GameSn']

        if game_result is None or game_result['result']["id"] != 0 or game_result.get("data") is None:
            self.logger.error("[SlotSystem] onBonusSpin: resp={}".format(game_result))
            return self._Result(game_result['result']["id"] if game_result is not None else -999, Src=game_result)

        fever_game_return_processing = None
        hit_fever = False
        if fever_game_return is not None and len(fever_game_return) > 0:
            hit_fever = True
            fever_game_return_processing = self._Dao.data_game_return_processing(ark_id, game_name, game_no, None, fever_game_return, bet_type="FEVER")
            if fever_game_return_processing is None or len(fever_game_return_processing) <= 0:
                return self._Result(result={'id': -999999, 'msg': 'FEVERGAME RETURN PROCESSING ERROR'})

        #  結算流程
        code, detail_log, current_game_return, balance, bet_amount, win_amount = self.settle_flow(ark_id, game_name, current_game_return, bet_amount=total_bet, player_data=player_data, bet_type="MAINGAME")
        if code != 0:
            return self._Result(code)

        #  更新遊戲狀態
        code = self._update_user_game_state(ark_id, game_name, ret_user_game_state, fever_game_return_processing, bet_type="MAINGAME")
        if code != 0:
            return self._Result(code)

        #  紀錄Log
        self._log_processing(ark_id, game_name, game_no, wagers_id, game_sn, bet_amount, win_amount, player_data, balance, detail_log, platform_data, game_data, current_game_return, date_key)

        #  活動有效押注累積、Buffer
        self._trigger_events(ark_id, game_name, bet_amount, win_amount, player_data, wagers_id, data, func_group, platform_data, current_game_return)

        if self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableDisconnectSettle") and hit_fever:
            can_settle_now = (len(ret_user_game_state["current_sg_id"]) <= 0)
            self.disconnect_settle.cmd_after(ark_id, game_name, can_settle_now=can_settle_now)

        return self._Result(0, Src=game_result)

    #  結算玩家已產生的盤面
    #  is_save_result: 是否儲存結果，提供玩家表演用
    def settle_player_game(self, ark_id, game_name, is_save_result=False):
        player_data = self.GetPlayerDataFunc(ark_id)
        time_now = datetime.now(SlotSystem.UTC_PLUS_0)
        date_key = time_now.strftime("%Y%m%d")
        save_result = []

        for i in range(200):
            current_game_return = self._Dao.get_and_delete_data_game_return(ark_id, game_name)
            if current_game_return is None:
                return -200033
            if len(current_game_return) <= 0:
                return 0
            game_no = current_game_return['GameNo']
            wagers_id = current_game_return['WagersId']
            game_sn = current_game_return['GameSn']
            user_game_state = current_game_return['GameState']

            command_data = user_game_state.pop('command_data', {})
            fs_setting, gn_data, platform_data, game_data = (
                command_data.get('fs_setting', {}),
                command_data.get('gn_data', {}),
                command_data.get('platform_data', {}),
                command_data.get('game_data', {})
            )
            func_group = platform_data.get("FuncGroup", "test" if self.is_test else "default")

            #  不需要進行結算(如: 進入Fever遊戲Init)
            if not self._need_transaction(current_game_return):
                self._Dao.log_game_return(date_key, current_game_return)
                continue

            #  結算流程
            code, detail_log, current_game_return, balance, bet_amount, win_amount = self.settle_flow(ark_id, game_name, current_game_return, bet_amount=0, player_data=player_data, bet_type="FEVER")
            if code != 0:
                return code

            #  紀錄Log
            self._log_processing(ark_id, game_name, game_no, wagers_id, game_sn, bet_amount, win_amount, player_data, balance, detail_log, platform_data, game_data, current_game_return, date_key)

            #  活動有效押注累積、Buffer
            data = {'ark_token': None}
            self._trigger_events(ark_id, game_name, bet_amount, win_amount, player_data, wagers_id, data, func_group, platform_data, current_game_return)

            if is_save_result:
                save_result.append(current_game_return)

        if len(save_result) > 0:
            self._Dao.save_data_game_return(save_result)
        return

    #  取得玩家預設選擇
    def get_default_client_action(self, sg_id, fever_action):
        client_action_data = {}
        if fever_action is not None and str(sg_id) in fever_action:
            client_action_data = copy.copy(fever_action[str(sg_id)])
            for key, val in client_action_data.iteritems():
                if type(val) == dict and "RAND_PICK" in val:
                    client_action_data[key] = random.choice(val["RAND_PICK"])
        return client_action_data

    #  產生玩家遊戲盤面
    def do_fever_all(self, ark_id, game_name, fever_action):
        user_game_state = self.ForceLockGameState(ark_id, game_name)
        sg_id = user_game_state['current_sg_id'][0]
        client_action_data = self.get_default_client_action(sg_id, fever_action)
        client_action_data['client_sg_id'] = sg_id

        code, current_game_return, fever_game_return, ret_user_game_state, each_win_amount = self.gen_current_game_return(ark_id, game_name, user_game_state=user_game_state, client_action_data=client_action_data, fever_all=True)
        if code != 0:
            return code
        game_no = current_game_return['GameNo']
        fever_game_return = [current_game_return, *(fever_game_return or [])]

        fever_game_return_processing = None
        if fever_game_return is not None and len(fever_game_return) > 0:
            fever_game_return_processing = self._Dao.data_game_return_processing(ark_id, game_name, game_no, None, fever_game_return, bet_type="FEVER")
            if fever_game_return_processing is None or len(fever_game_return_processing) <= 0:
                code = -999999
                self.logger.error("[SlotSystem] do_fever_all: id={}, msg={}".format(code, 'FEVERGAME RETURN PROCESSING ERROR'))
                return code

        code = self._update_user_game_state(ark_id, game_name, ret_user_game_state, fever_game_return_processing, bet_type="DISCONNECT")
        if code != 0:
            return code
        return code

    #  結算流程
    def settle_flow(self, ark_id, game_name, current_game_return, bet_amount, player_data=None, bet_type="FEVER"):
        if player_data is None:
            player_data = self.GetPlayerDataFunc(ark_id)
        game_result = current_game_return.get('GameResult')
        game_no = current_game_return['GameNo']
        wagers_id = current_game_return['WagersId']
        game_sn = current_game_return['GameSn']
        game_state = current_game_return['GameState']
        origin_bet = game_state['current_bet'] if bet_type == "FEVER" else bet_amount
        total_bet = 0 if bet_type == "FEVER" else bet_amount
        time_now = datetime.now(UTC)
        code = 0

        # Proceed result
        history_detail = current_game_return.get('HistoryDetail', None)
        respdata = game_result["data"]
        win_amount = respdata.get("this_win_amount")

        detail_log = self._Dao.log_filter(game_name, ark_id, total_bet, win_amount, player_data, originBet=origin_bet, game_no=game_no, game_sn=game_sn, wagers_id=wagers_id, HistoryDetail=history_detail, TimeNow=time_now)
        wallet_ret = self._WalletManager.Transaction(ark_id, {"Coin": total_bet}, {"Coin": win_amount}, PlayerData=player_data, WagersId=wagers_id, DetailLog=detail_log)
        if wallet_ret[0] != 0:
            code = wallet_ret[0]
            return code, None, None, None, None, None
        balance = wallet_ret[1]["Coin"]
        respdata["balance"] = balance
        respdata["serial_id"] = wagers_id

        current_game_return['WalletReturn'] = wallet_ret
        return code, detail_log, current_game_return, balance, bet_amount, win_amount

    #  更新遊戲狀態
    def _update_user_game_state(self, ark_id, game_name, user_game_state=None, fever_game_return_processing=None, bet_type="MAINGAME"):
        code = 0
        if user_game_state is not None:
            result = self.SaveGameState(ark_id, game_name, user_game_state, bet_type=bet_type)
            if result is None:
                code = -200034
                self.logger.error("[SlotSystem] user_game_state: id={}, msg={}".format(code, 'UPDATE GAME STATE ERROR'))
                return code
        if fever_game_return_processing is not None and len(fever_game_return_processing) > 0:
            is_success = self._Dao.save_data_game_return(fever_game_return_processing)
            if not is_success:
                code = -200044
                self.logger.error("[SlotSystem] user_game_state: id={}, msg={}".format(code, 'SAVE_DATA_GAME_RETURN_FAIL'))
                return code
        return code

    #  紀錄Log
    def _log_processing(self, ark_id, game_name, game_no, wagers_id, game_sn, bet_amount, win_amount, player_data, balance, detail_log, platform_data, game_data, current_game_return, date_key):
        self._Dao.log_game_return(date_key, current_game_return)
        self._Dao.WriteHistory(game_name, ark_id, game_no, wagers_id, game_sn, bet_amount, win_amount, player_data, Balance=balance, DetailLog=detail_log, DateKey=date_key)

        #  Log紀錄
        analytic_log = current_game_return.get('AnalyticLog', None)
        if analytic_log is not None:
            self._Dao.WriteAnalyticLog(ark_id, game_name, game_no, wagers_id, game_sn, player_data, bet_amount, bet_amount, 0, win_amount, win_amount, analytic_log, DateKey=date_key)

        detail_bet_win_log = current_game_return.get('DetailBetWinLog', {})
        self._Dao.WriteDetailBetWinLog(game_name, ark_id, game_no, wagers_id, game_sn, bet_amount, win_amount, player_data, PlatformData=platform_data, GameData=game_data, DetailLog=detail_log, DetailBetWinLog=detail_bet_win_log, DateKey=date_key)
        return

    #  活動有效押注累積、Buffer
    def _trigger_events(self, ark_id, game_name, bet_amount, win_amount, player_data, wagers_id, data, func_group, platform_data, current_game_return):
        if not self.is_test:
            game_result = current_game_return['GameResult']
            respdata = game_result["data"]
            self._CallLobbyService("PlayResult", ark_id, data["ark_token"], game_name, Bet=bet_amount, Win=win_amount, ExtraData={"GameSn": wagers_id, 'GameRate': platform_data["RTP"], "WinType": respdata.get("win_type")})
        if self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableBuffer"):
            self.KioskBuffer.incr_buffer(player_data["ThirdPartyCurrency"], player_data["MerchantId"], game_name, bet_amount, win_amount, player_data=player_data)
        return

    #  產生Fever遊戲盤面(→ SlotManager → SlotMachine)
    def gen_current_game_return(self, ark_id, game_name, user_game_state=None, client_action_data=None, fever_all=False, dev_mode=0):
        code = 0

        # Call SlotManager
        game_no = user_game_state['game_no']
        gameResp = self.slot_manager.next_fever(ark_id, game_name, user_game_state=user_game_state, client_action_data=client_action_data, fever_all=fever_all, dev_mode=dev_mode)
        if gameResp is None or gameResp['result']["id"] != 0 or gameResp.get("data") is None:
            code = gameResp['result']["id"] if gameResp is not None else -999
            self.logger.error("[SlotSystem] onNextFever: resp={}".format(self._Result(code, Src=gameResp)))
            return code, None, None, None, None, None

        gData = gameResp['data']
        current_game_return, fever_game_return, ret_user_game_state, each_win_amount = (
            gData["CurrentGameReturn"],
            gData.get("FeverGameReturn", []),
            gData.get("AfterUserGameState", None),
            gData.get("EachWinAmount", [])
        )

        if current_game_return is None or 'GameResult' not in current_game_return or current_game_return['GameResult']['result']["id"] != 0 or current_game_return['GameResult'].get("data") is None:
            code = current_game_return['result']["id"] if current_game_return is not None else -999
            self.logger.error("[SlotSystem] onNextFever: resp={}".format(self._Result(code, Src=current_game_return)))
            return code, None, None, None, None, None
        game_return_processing = self._Dao.data_game_return_processing(ark_id, game_name, game_no, None, current_game_return, bet_type="FEVER")
        current_game_return = game_return_processing[0]
        return code, current_game_return, fever_game_return, ret_user_game_state, each_win_amount

    #  是否需要進行結算(排除初始化階段)
    def _need_transaction(self, current_game_return):
        game_result = current_game_return.get('GameResult')
        return not (game_result['data']['sg_state'] == 1)

    #  取ChanceKey
    def get_chance_key(self, func_group, merchant_rtp, game_name, ark_id, assign_prob_id=None):
        # "ChanceKey"
        prob_group_name = None
        is_chance_from_db = False
        chance_key = merchant_rtp
        if assign_prob_id is not None:
            is_chance_from_db = True
            chance_key = assign_prob_id
        elif self._FuncSwitch.get_fs_setting_without_platform(game_name, func_group, "EnableProbSwitch"):
            if not self.prob_switch.is_prob_switch(game_name):
                self.logger.warning("[SlotSystem] get_chance_key: game_name={}, ark_id={}, merchant_rtp={}, assign_prob_id={}, prob_group_name={}, chance_key={}".format(game_name, ark_id, merchant_rtp, assign_prob_id, prob_group_name, chance_key))
                return is_chance_from_db, prob_group_name, chance_key
            prob_group_name, chance_key = self.prob_switch.get_prob(game_name, ark_id)
            is_chance_from_db = True
        self.logger.debug("[SlotSystem] get_chance_key: game_name={}, ark_id={}, merchant_rtp={}, assign_prob_id={}, prob_group_name={}, chance_key={}".format(game_name, ark_id, merchant_rtp, assign_prob_id, prob_group_name, chance_key))
        return is_chance_from_db, prob_group_name, chance_key

    #  取得總押注
    def _GetTotalBet(self, bet_value, cost, bonus_cost_multi=None):
        if cost is None:
            return None
        total_bet = bet_value * cost
        if bonus_cost_multi is not None:
            total_bet = bet_value * bonus_cost_multi
        return total_bet

    #  取得JP資料(Todo)
    def onGetIngameJpInfo(self, ark_id, cmd_data, data):
        return self._Result(0)

    # def onGetLogGameResult(self, ark_id, cmd_data):
    #     game_name = cmd_data["GameName"]
    #     if self._GetSlotServiceUrl(game_name) is None:
    #         return self._Result(Code=-1, Msg="GameName not found")
    #     GameNo = cmd_data["GameNo"]
    #     GameSn = cmd_data["GameSn"]
    #
    #     # Call SlotService
    #     resp = self._CallSlotService(cmd="get_log_game_result", ark_id=ark_id, game_name=game_name, cmd_data=cmd_data)

    # def _ResetGameState(self, ark_id, game_name, GameState):
    #
    #     # Call SlotService
    #     resp = self._CallSlotService(cmd="ResetGameState", ark_id=ark_id, game_name=game_name, GameState=GameState)
    #
    #     return self._Result(resp=resp)

    def GetGameState(self, ark_id, game_name, bSecondary=False, bCheckLock=False):
        return self._Dao.GetGameState(ark_id, game_name, bSecondary, bCheckLock)

    def GetLockGameState(self, ark_id, game_name, client_sg_id=None):
        user_state_doc = self._Dao.GetLockGameState(ark_id, game_name)
        if not user_state_doc:
            return None
        if client_sg_id is not None and client_sg_id != user_state_doc['current_sg_id'][0]:
            self.logger.error('[SlotSystem] fever sg_id error!: user_id={}, game_id={}, client sg id={}, game_state.current_sg_id={}, game_state={}'.format(ark_id, game_name, client_sg_id, user_state_doc['current_sg_id'][0], user_state_doc))
            self._Dao.UnlockGameState(ark_id, game_name)
            return None
        return user_state_doc

    def ForceLockGameState(self, ark_id, game_name):
        return self._Dao.GetLockGameState(ark_id, game_name, bForceLock=True)

    def SaveGameState(self, ark_id, game_name, user_game_state, bet_type):
        if bet_type == "MAINGAME":
            return self._Dao.SetGameState(ark_id, game_name, user_game_state, upsert=True, new=True, lock=0)
        elif bet_type == "DISCONNECT":
            return self._Dao.SetGameState(ark_id, game_name, user_game_state, upsert=False, new=False, lock=-1)
        return self._Dao.SetGameState(ark_id, game_name, user_game_state, upsert=False, new=False, lock=1)

    def _GetLobbyServiceUrl(self, cmd):
        if self._pixiuLobbyUrl is None:
            return None
        return self._pixiuLobbyUrl + "/" + cmd

    def _CallLobbyService(self, cmd, ark_id, ark_token, game_name, headers=None, **kwargs):
        headers = headers or {'Content-Type': 'application/x-www-form-urlencoded'}
        headers.update({'User-Agent': 'My User Agent 1.0'})
        url = self._GetLobbyServiceUrl(cmd)
        if url is None:
            return
        # {"user_id": user_id, "token": token, "game_name": game_name, "Bet": bet, "Win": win, "ExtraData": extra_data}
        data = kwargs
        data["user_id"] = ark_id
        data["token"] = ark_token
        data["GameName"] = self.slot_manager.get_code_name(game_name)
        r = None
        try:
            r = self._pixiuLobbySession.post(url, headers=headers, json=kwargs)
            if r.status_code != 200:
                self.logger.error("[SlotSystem] url:{}, during:{}ms, reqData:{}, status_code:{}".format(url, r.elapsed.microseconds//1000, data, r.status_code))
                return None
            resp = r.json()
        except:
            if r is None:
                self.logger.error("[SlotSystem] url:{}, reqData:{}, error:{}".format(url, data, traceback.format_exc()))
            else:
                self.logger.error("[SlotSystem] url:{}, during:{}ms, reqData:{}, resp:{}, error:{}".format(url, r.elapsed.microseconds//1000, data, r.text, traceback.format_exc()))
            return None
        self.logger.info("[SlotSystem] url:{}, during:{}ms, reqData:{}, resp:{}".format(url, r.elapsed.microseconds//1000, data, resp))
        return resp

    def _Result(self, nResult=0, Src=None, *args, **kwargs):
        r = {} if 'OutParam' not in kwargs else kwargs['OutParam']
        r['Code'] = nResult
        r['ts'] = time.time()
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
        return r
