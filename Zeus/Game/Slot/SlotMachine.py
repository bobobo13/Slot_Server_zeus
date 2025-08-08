#!/usr/bin/python
# -*- coding: utf-8 -*-

import importlib
from SlotServer.Common.Util import Copy as copy
from SlotServer.Common.MathTool import floor_float
from SlotServer.Common.SlotMath import SlotMath
from SlotCommon.player_game_state import PlayerGameState
from SlotCommon.game_status_code import *
from .SlotDao import SlotDao


class SlotMachine:
	UPDATE_INTERVAL = 60

	def __init__(self, Logger, DataSource=None, CodeName="Zeus", **kwargs):
		self.code_name = CodeName
		self.logger = Logger
		self.DataSource = DataSource
		self._slotDao = SlotDao(Logger)
		self._tabGameSetting = self._slotDao.LoadSlotGameSetting(self.code_name)
		self._tabGameInfo = self._slotDao.LoadSlotGameInfo(self.code_name)
		self.calculator_cls = importlib.import_module(".Game.Slot.{0}.{0}".format(self.code_name), package=self.code_name).__getattribute__(self.code_name)
		self._tabWinType = {"Default": [-1, 0, 1, 5, 10, 25, 50]}
		self.InGameJpMgr = None

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
		game_setting = self._tabGameSetting[self.code_name]
		if type(game_setting) == list:
			prob_id = self.GetChanceKey(gn_data, platform_data, game_data)
			print(prob_id)
			if prob_id is None:
				self.logger.error('[SlotMachine] start_game: No Game Setting for game:{}, prob_id:{}.'.format(game_name, prob_id))
				return self._Result(result=STATUS_CODE_ERR_GAME_IS_UNDER_CONSTRUCTION, Desc="No Game Setting for game:{}, prob_id:{}.".format(game_name, prob_id))

			for i in game_setting:
				if i['ProbId'] == prob_id:
					game_setting = i
					break
			if type(game_setting) == list:
				self.logger.error('[SlotMachine] start_game: No Game Setting for game:{}, prob_id:{}.'.format(game_name, prob_id))
				return self._Result(result=STATUS_CODE_ERR_GAME_IS_UNDER_CONSTRUCTION, Desc="No Game Setting for game:{}, prob_id:{}.".format(game_name, prob_id))

		calculator = self.calculator_cls(self.logger, game_setting)
		if calculator is None:
			return self._Result(result=STATUS_CODE_ERR_GAME_IS_UNDER_CONSTRUCTION, Desc="No Calculator Module for game:{}.".format(game_name))

		use_remote_game_state = game_data.get('use_remote_game_state', False)
		game_state_data = game_data.get('game_state_data', None)

		# 如果有傳 game_state_data ，則使用傳入的 game_state_data，否則從資料庫取得
		if not use_remote_game_state:
			game_state_data = self._slotDao.GetGameState(ark_id)

		game_state = PlayerGameState(game_state_data)

		# 取得押注相關資訊
		bet_list= self._get_bet_info(game_setting, platform_data, game_state)

		# 取得遊戲相關設定資料
		game_play_info, slot_type, lineways_amount = self._get_play_game_info(game_setting, calculator, game_state)

		#  取得玩家遊戲狀態
		ret_game_state, bet_value = self._get_ret_game_state(game_setting, calculator, game_state, bet_list, slot_type, lineways_amount)

		ret = {
			'current_line_bet': bet_value,  # recovery from game state or 0
			'bet_list': bet_list,
			'wheel_blocks': calculator.get_init_reel_info(game_state),
			'game_state': ret_game_state,
			'total_win_amount': game_state.one_play_win_amount,
			'extra_info': game_setting['extra_info']
		}
		ret.update(game_play_info)

		winTypeInfo = self._tabWinType.get(self.code_name, self._tabWinType.get("Default"))
		ret['WinType'] = winTypeInfo

		ret_extra_bet = self._get_ret_extra_bet(bet_list, lineways_amount, game_state.extra_bet, game_setting)
		if ret_extra_bet is not None:
			ret["ExtraBet"] = ret_extra_bet

		self.logger.info('[SlotMachine] start_game: ark_id={}, game_name={}, code_name:{}, fs_setting={}, gn_data={}, platform_data={}, game_data:{}'.format(ark_id, game_name, self.code_name, fs_setting, gn_data, platform_data, game_data))
		self.logger.debug('[SlotMachine] start_game: ret={}'.format(ret))
		return self._Result(result=STATUS_CODE_OK, data=ret)

	def GetChanceKey(self, gn_data, platfrom_data, game_data):
		if platfrom_data is None:
			return None
		gameRatio = platfrom_data.get("GameRatio")
		chanceKey = self.convert_to_int_if_possible(gameRatio)
		return chanceKey


	def spin(self, ark_id, game_name, fs_setting, gn_data, platform_data, game_data):
		EnableTestMode = fs_setting.get('EnableTestMode', False)
		EnableJpWinIndependent = fs_setting.get('EnableJpWinIndependent', False)
		chance_key = self.GetChanceKey(gn_data, platform_data, game_data)

		bet_value = game_data.get('bet_value', game_data.get('line_bet', None))
		bet_lines = game_data.get('bet_lines', game_data.get('lines', None))
		dev_mode = int(game_data.get('dev_mode', 0)) if EnableTestMode else 0
		extra_bet = game_data.get('ExtraBet', game_data.get('extra_bet', False))
		use_remote_game_state = game_data.get('use_remote_game_state', False)
		game_state_data = game_data.get('game_state_data', None)
		game_extra_data = platform_data

		# 如果有傳 game_state_data ，則使用傳入的 game_state_data，否則從資料庫取得
		if not use_remote_game_state:
			game_state_data = self._slotDao.GetGameState(ark_id)

		game_state = PlayerGameState(game_state_data)

		game_state.update_extra_bet(extra_bet)  # 將extra_bet資訊存入game_state

		if game_state.is_special_game:
			self.logger.error('[SlotMachine] spin: user in bonus/fever! user_id={}, game_name={}, code_name:{}, fs_setting:{}, gn_data={}, platform_data={}, game_data={}'.format(ark_id, game_name, self.code_name, fs_setting, gn_data, platform_data, game_data))
			return self._Result(result=STATUS_CODE_ERR_MAINGAME_STATE_EXCEPTION)

		game_setting = self._tabGameSetting[self.code_name]
		if type(game_setting) == list:
			prob_id = self.GetChanceKey(gn_data, platform_data, game_data)
			print(prob_id)
			if prob_id is None:
				self.logger.error( '[SlotMachine] spin: No Game Setting for game:{}, prob_id:{}.'.format(game_name, prob_id))
				return self._Result(result=STATUS_CODE_ERR_GAME_IS_UNDER_CONSTRUCTION, Desc="No Game Setting for game:{}, prob_id:{}.".format(game_name, prob_id))
			for i in game_setting:
				if i['ProbId'] == prob_id:
					game_setting = i
					break
			if type(game_setting) == list:
				self.logger.error('[SlotMachine] start_game: No Game Setting for game:{}, prob_id:{}.'.format(game_name, prob_id))
				return self._Result(result=STATUS_CODE_ERR_GAME_IS_UNDER_CONSTRUCTION, Desc="No Game Setting for game:{}, prob_id:{}.".format(game_name, prob_id))

		# create calculator
		calculator = self.calculator_cls(self.logger, game_setting)
		if calculator is None:
			return self._Result(result=STATUS_CODE_ERR_GAME_IS_UNDER_CONSTRUCTION)

		# 取得押注相關資訊
		bet_list= self._get_bet_info(game_setting, platform_data, game_state)

		# 取得遊戲相關設定資料
		game_play_info, slot_type, lineways_amount = self._get_play_game_info(game_setting, calculator, game_state)

		if bet_value not in bet_list:
			self.logger.error('[SlotMachine] spin: user bet id error! user_id={}, game_name={}, code_name:{}, fs_setting:{}, gn_data={}, platform_data={}, game_data={}'.format(ark_id, game_name, self.code_name, fs_setting, gn_data, platform_data, game_data))
			return self._Result(result=STATUS_CODE_ERR_BET_DATA_NOT_EXISTS)

		extra_bet_ratio = 1
		if extra_bet:
			if 'extra_bet' not in game_setting:
				return self._Result(result=STATUS_CODE_ERR_EXTRABET_NOT_SUPPORT)
			if 'Enable' not in game_setting['extra_bet'] or not game_setting['extra_bet']['Enable']:
				return self._Result(result=STATUS_CODE_ERR_EXTRABET_NOT_SUPPORT)
			extra_bet_info = copy.deepcopy(game_setting['extra_bet'])
			extra_bet_ratio = extra_bet_info['Ratio']

		win_type_info = self._tabWinType.get(self.code_name, self._tabWinType.get("Default"))

		original_bet = floor_float(bet_lines * bet_value, 3)  # 這次用的bet,log要用
		bet_amount = original_bet*extra_bet_ratio
		prob_id = str(chance_key)

		prob_id, game_info = self._get_game_info(game_name, assignProbId=prob_id)
		spin_result, origin_total_win, extra_info = calculator.spin_flow(bet_value, bet_lines, game_state, game_info, dev_mode=dev_mode, user_id=ark_id, extra_bet=extra_bet, game_data=game_data, extra_data=game_extra_data)

		if extra_info is not None:
			game_state.update_extra_info(extra_info)

		spin_result, game_state, jackpot_win, winJpList, jp_system, jackpot_con_data = self._get_jp_result(ark_id, bet_value, bet_lines, spin_result, game_state, extra_bet, calculator.JackpotFlow, self.InGameJpMgr, game_info, ratio=extra_bet_ratio, isJpWinIndependent=EnableJpWinIndependent)

		main_total_win = game_state.update_by_spin_result(spin_result, bet_value, bet_lines, jp_win=jackpot_win if EnableJpWinIndependent else 0, probId=prob_id)
		# update 客製化 recovery 牌面
		game_state.update_last_main_reels(calculator.custom_last_main_reels(spin_result))

		# 使用DB的話在這更新遊戲狀態，不使用DB則把多回傳game_state_data
		if not use_remote_game_state:
			_res = self._slotDao.SetGameState(ark_id, game_state.as_json_dict())
			if _res is None:
				return self._Result(result=STATUS_CODE_ERR_UPDATE_GAME_STATE_ERROR)

		win_type = calculator.get_win_type(original_bet, spin_result.this_win, game_state, spin_result, win_type_info)
		current_script = calculator.after_spin_current_script(game_state)
		new_user_game_state = None if not use_remote_game_state else game_state.as_json_dict()
		ret = self._get_spin_ret(spin_result, game_state, main_total_win, win_type, slot_type, current_script, bet_value, bet_lines, None, jackpot_win)

		HistoryDetail = calculator.build_result_log(None, spin_result, jp_win=0, extra_bet=extra_bet)
		AnalyticLog = spin_result.get_log_custom()
		AnalyticLog["ProbId"] = prob_id

		self.logger.info('[SlotMachine] spin: ark_id={}, game_name={}, code_name:{}, fs_setting:{}, gn_data={}, platform_data={}, game_data={}'.format(ark_id, game_name, self.code_name, fs_setting, gn_data, platform_data, game_data))
		self.logger.debug('[SlotMachine] spin: ret={}, user_game_state={}, history_detail={}'.format(ret, new_user_game_state, HistoryDetail))
		return self._Result(result=STATUS_CODE_OK, data=ret, user_game_state=new_user_game_state, history_detail=HistoryDetail, AnalyticLog=AnalyticLog)
	
	def next_fever(self, ark_id, game_name, fs_setting, gn_data, platform_data, game_data, **kwargs):
		""" 收到特殊遊戲的指令
		Args:
			ark_id: 玩家ID
			game_name: client傳來的遊戲名稱
			fs_setting: 平台的功能開關設定 (Function Switch)
			gn_data: 平台針對遊戲的設定
			platform_data: 平台資料
			game_data: client傳來的封包資料 (cmd_data)
		"""
		# 平台指定功能開關
		EnableTestMode = fs_setting.get('EnableTestMode', False)  # dev 模式
		EnableJpWinIndependent = fs_setting.get('EnableJpWinIndependent', False)

		chance_key = gn_data.get("ChanceKey")
		special_game_id = game_data.pop('sg_id', None)
		dev_mode = int(game_data.get('dev_mode', 0)) if EnableTestMode else 0
		extra_bet = game_data.get('ExtraBet', game_data.get('extra_bet', False))
		use_remote_game_state = game_data.get('use_remote_game_state', False)
		game_state_data = game_data.get('game_state_data', None)

		client_action_data = kwargs.get('client_action', dict())

		# 如果有傳 game_state_data ，則使用傳入的 game_state_data，否則從資料庫取得
		if not use_remote_game_state:
			game_state_data = self._slotDao.GetGameState(ark_id)

		game_state = PlayerGameState(game_state_data)
		game_state.update_extra_bet(extra_bet)  # 將extra_bet資訊存入game_state

		# 檢查遊戲狀態是否是特殊遊戲
		if not game_state.is_special_game:
			self.logger.error("[next_fever] {} Not in special game, game:{}".format(ark_id, game_name))
			return self._Result(result=STATUS_CODE_ERR_FEVERGAME_STATE_EXCEPTION)

		# sg_id 對不起來
		if special_game_id != game_state.current_sg_id:
			self.logger.error("[next_fever] sg_id is not match, {} != {}".format(special_game_id, game_state.current_sg_id))
			return self._Result(result=STATUS_CODE_ERR_FEVERGAME_STATE_EXCEPTION)

		# 目前的特殊遊戲狀態
		special_game_state = game_state.current_special_game_data
		bet_lines = special_game_state['current_line']  # 押注線數
		bet_value = special_game_state['current_bet']  # 押注倍數
		# current_level = special_game_state['current_level', 0]
		# current_script = special_game_state.get('current_script', {})

		# 取得 GameSetting
		game_setting = self._tabGameSetting[self.code_name]

		# 建立 Calculator
		calculator = self.calculator_cls(self.logger, game_setting)
		if not calculator:
			return self._Result(result=STATUS_CODE_ERR_GAME_IS_UNDER_CONSTRUCTION)

		# 取得遊戲相關設定資料
		bet_list = self._get_bet_info(game_setting, platform_data, game_state)
		game_play_info, slot_type, lineways_amount = self._get_play_game_info(game_setting, calculator, game_state)
		win_type_info = self._tabWinType.get(self.code_name, self._tabWinType.get("Default"))

		# Extra Bet
		extra_bet_ratio = 1
		if game_state.extra_bet_on:
			if 'extra_bet' not in game_setting:
				return self._Result(result=STATUS_CODE_ERR_EXTRABET_NOT_SUPPORT)
			if 'Enable' not in game_setting['extra_bet'] or not game_setting['extra_bet']['Enable']:
				return self._Result(result=STATUS_CODE_ERR_EXTRABET_NOT_SUPPORT)
			extra_bet_info = copy.deepcopy(game_setting['extra_bet'])
			extra_bet_ratio = extra_bet_info['Ratio']

		win_type_info = self._tabWinType.get(self.code_name, self._tabWinType.get("Default"))
		original_bet = floor_float(bet_lines * bet_value, 3)  # 這次用的bet,log要用
		bet_amount = original_bet * extra_bet_ratio

		# 依 ChanceKey 取得對應 RTP 的 GameInfo
		prob_id = str(chance_key)
		prob_id, game_info = self._get_game_info(game_name, assignProbId=prob_id)

		# 進入 next_fever_flow
		fever_result, game_state = calculator.next_fever_flow(client_action_data, game_state, game_info, dev_mode=dev_mode, user_id=ark_id, extra_bet=extra_bet)
		if fever_result.has_error:
			self.logger.error('[SlotMachine][next_fever] fever result error={}. ark_id={}, game_name={}, code_name:{}, fs_setting:{}, gn_data={}, platform_data={}, game_data={}'.format(
				fever_result.error, ark_id, game_name, self.code_name, fs_setting, gn_data, platform_data, game_data))
			return self._Result(result=STATUS_CODE_ERR_FEVERGAME_STATE_EXCEPTION)
		self.logger.debug('[SlotMachine][next_fever] fever_result={}, game_info={}, use_remote_game_state:{}'.format(fever_result, game_info, use_remote_game_state))

		# InGameJackpot (該遊戲沒有JP)
		fever_update_data = {}
		calculator.update_fever_result(fever_result, fever_update_data)

		game_state.update_last_fever_reels(special_game_id, *calculator.custom_last_fever_reels(fever_result))  # 更新 recovery 盤面
		if fever_result.win_amount:
			game_state.update_credit_change(fever_result.win_amount)  # 更新 credit 值
		total_win = game_state.update_by_fever_result(fever_result, jp_win=0)  # 更新贏分累計

		#判斷是否FG結束
		if fever_result.is_gameover:
			calculator.fever_after_action(game_state)
			now_state = copy.deepcopy(game_state)  # 接下來會清掉一些狀態，先shadow copy一份，以利後續處理
			game_state.end_special_game(special_game_id)
		else:
			now_state = game_state

		# 使用DB的話在這更新遊戲狀態，不使用DB則把多回傳game_state_data
		if not use_remote_game_state:
			_res = self._slotDao.SetGameState(ark_id, game_state.as_json_dict())
			if _res is None:
				return self._Result(result=STATUS_CODE_ERR_UPDATE_GAME_STATE_ERROR)

		current_script = calculator.after_spin_current_script(game_state)
		new_user_game_state = None if not use_remote_game_state else game_state.as_json_dict()

		# 要回傳的內容
		ret = {'sg_id': fever_result.sg_id}
		if fever_result.fever_map:
			ret.update(fever_result.fever_map.export())
		# ret['current_script'] = current_script
		ret['total_win_amount'] = total_win
		ret['this_win_amount'] = fever_result.win_amount
		ret['win_type'] = calculator.get_fever_win_type(original_bet, fever_result.this_win, game_state, fever_result, win_type_info)

		ret['game_state'] = {
			'current_sg_id': game_state.current_sg_id,
			'sg_state': now_state.current_special_game_level,
			'current_script': current_script
		}

		# 中了别的特殊游戏更新
		# if fever_result.is_win_special_game:
		# 	sg_id_list = fever_result.win_special_game_id_list
		# 	spin_reels = fever_result.spin_reels
		# 	current_script_dict = copy.deepcopy(fever_result.special_game_current_script)
		# 	game_state.win_special_game_state(sg_id_list, bet_value, bet_lines, current_script_dict, spin_reels)

		# todo: history_detail 未實作
		history_detail = {}
		# history_detail = calculator.build_result_log(None, fever_result, jp_win=0, extra_bet=extra_bet)

		self.logger.info('[SlotMachine][next_fever] ark_id={}, game_name={}, code_name:{}, fs_setting:{}, gn_data={}, platform_data={}, game_data={}'.format(ark_id, game_name, self.code_name, fs_setting, gn_data, platform_data, game_data))
		self.logger.debug('[SlotMachine][next_fever] ret={}, user_game_state={}, history_detail={}'.format(ret, new_user_game_state, history_detail))
		return self._Result(result=STATUS_CODE_OK, data=ret, user_game_state=new_user_game_state, history_detail=history_detail)
	
	def _get_bet_info(self, gameSetting, platform_data, game_state):
		'''
		'platform_bet_info':{
			'BetList': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
			'MaxBet': 10,
			'MinBet': 1,
			'MaxWin': 1000000,
			'MaxOdds': 1000,
			'RTP': 97,
			'ExtraBetRtp': 97}
		'''
		if platform_data is None:
			return gameSetting.get('line_bet_list', []), None, None

		platform_bet_list = platform_data.get('BetList', [])
		game_bet_list = gameSetting.get('line_bet_list', [])
		if platform_bet_list is None or len(platform_bet_list)<=0:
			return game_bet_list
		inter_bet_list = list(set(platform_bet_list) & set(game_bet_list))
		if game_state.is_scatter_game and game_state.current_bet not in inter_bet_list:
			inter_bet_list.append(game_state.current_bet)
		bet_list = sorted(inter_bet_list)
		return bet_list

	def _get_play_game_info(self, gameSetting, calculator, game_state):
		"""
		取得遊戲相關設定
		"""
		game_play_info = {
			'odds': gameSetting['odds'],
			'special_odds': gameSetting.get('special_odds', dict())
		}
		field_map = {
			'allways': ('max_costs', 'cost'),
			'lines': ('max_lines', 'cost'),
		}

		game_play_info.update(calculator.get_custom_info())

		extra_info = copy.deepcopy(gameSetting.get('extra_info', dict()))
		extra_info.update(calculator.get_extra_info(game_state))
		game_play_info['extra_info'] = extra_info

		slot_type = gameSetting['slot_type']
		ret_lineways_name, lineways_name = field_map.get(slot_type, (None, None))
		lineways_amount = gameSetting.get(lineways_name, dict())
		game_play_info[ret_lineways_name] = lineways_amount
		return game_play_info, slot_type, lineways_amount

	def _get_ret_game_state(self, gameSetting, calculator, game_state, bet_list, slot_type, lineways_amount):
		"""
		取得玩家遊戲狀態
		"""
		field_map = {
			'allways': 'current_costs',
			'lines': 'current_lines',
		}

		ret_game_state = {
			'current_script': {},
			'current_sg_id': game_state.current_sg_id,
			'sg_state': game_state.current_special_game_level if game_state.current_special_game_level < 2 else 3,
		}

		#  取得當前押注
		bet_value = game_state.current_bet
		if bet_value <= 0:
			bet_value = gameSetting.get('default_bet', bet_list[0])
		ret_game_state['current_bet'] = bet_value  # recovery from game state or 0

		ret_lineways_name = field_map.get(slot_type, None)
		ret_lineways_amount = game_state.recovery_bet_lines
		if ret_lineways_amount is None and ret_lineways_name is not None:
			ret_lineways_amount = lineways_amount
		ret_game_state[ret_lineways_name] = ret_lineways_amount

		if game_state.is_special_game:
			special_game_data = {
				'current_script': calculator.get_fever_recovery(game_state),
				'current_sg_total_times': game_state.current_special_game_data['current_script'].get('total_times', 0)
			}
			ret_game_state.update(special_game_data)
		return ret_game_state, bet_value

	def _get_ret_extra_bet(self, bet_list, bet_lines, status=None, gameSetting=None):
		if gameSetting is None:
			return None
		extra_bet_info = gameSetting.get('extra_bet')
		if extra_bet_info is None or bet_list is None or bet_lines is None:
			return None
		enable = extra_bet_info.get("Enable")
		ratio = extra_bet_info.get("Ratio")
		status = extra_bet_info.get("Default", False) if status is None else status
		ret_extra_bet = {}
		extra_bet_list = [i * bet_lines * (ratio-1) for i in bet_list]
		ret_extra_bet.update({"Enable": enable, "Status": status, "Ratio": ratio, "extra_bet_list": extra_bet_list})
		return ret_extra_bet

	def _get_game_info(self, game_name, assignProbId=None, GameInfoList=None):
		gameInfoData = GameInfoList if GameInfoList is not None else self._tabGameInfo[self.code_name]
		if assignProbId is not None:
			prob_id = assignProbId
		if prob_id is None or prob_id not in gameInfoData.keys():
			raise Exception("No Game Info Found For {}[{}]".format(game_name, prob_id))
		gameInfo = copy.deepcopy(gameInfoData[prob_id])
		return prob_id, gameInfo

	def _get_def_prob_id(self, game_info):
		tmpProbWeightTable = {"result": [], "weight": []}
		for key, value in game_info.items():
			tmpProbWeightTable["result"].append(key)
			tmpProbWeightTable["weight"].append(value.get("weight", 1))
		if len(tmpProbWeightTable["result"]) <= 0:
			# raise Exception("No Game Info Found For {}".format(game_id))
			return None
		if len(tmpProbWeightTable["result"]) == 1:
			return tmpProbWeightTable["result"][0]
		_, prob_id = SlotMath.get_result_by_weight(tmpProbWeightTable["result"], tmpProbWeightTable["weight"])
		return prob_id

	def _get_jp_result(self, ark_id, bet_value, bet_lines, spin_result, game_state, extra_bet, JackpotFlowFunc, InGameJpMgr, gameInfo, enable_jp=True, ratio=1, isJpWinIndependent=True):
		jp_system = ""
		jackpot_win = 0
		winJpList = []
		jackpot_con_data = None

		# TODO(JhengSian): CheckJp Enable from SlotBonus
		# InGameJP
		if not enable_jp:
			return spin_result, game_state, jackpot_win, winJpList, jp_system, jackpot_con_data

		if InGameJpMgr is not None:
			spin_result, game_state, jackpot_win, winJpList, jp_system, jackpot_con_data = JackpotFlowFunc(gameInfo, InGameJpMgr, ark_id, bet_value, bet_lines, spin_result, game_state, playerData=None, isJpWinIndependent=isJpWinIndependent, extra_bet=extra_bet, ratio=ratio)
		return spin_result, game_state, jackpot_win, winJpList, jp_system, jackpot_con_data

	def _get_spin_ret(self, spin_result, game_state, main_total_win, win_type, slot_type, current_script, bet_value, bet_lines, serial_id, jackpot_win, game_state_data=None):
		field_map = {
			'allways': 'current_costs',
			'lines': 'current_lines',
		}

		ret = dict()
		ret['wheel_blocks'] = spin_result.export_ex_wheel_block_result()
		ret['game_state'] = {
			'sg_state': game_state.current_special_game_level,
			'current_bet': bet_value,  # recovery from game state or 0
			'current_script': current_script
		}
		if game_state.current_sg_id >= 0:
			ret['game_state']['current_sg_id'] = game_state.current_sg_id
			ret['game_state']['current_sg_total_times'] = game_state.current_special_game_data['current_script'].get('total_times', 0)
		# 判斷是lines 還是 ways，回傳current_lines or current_costs
		ret[field_map[slot_type]] = bet_lines
		ret['total_win_amount'] = main_total_win
		ret['this_win_amount'] = spin_result.this_win

		ret['win_type'] = win_type
		if spin_result.have_extra_data:
			ret['extra_info'] = spin_result.get_extra_data()

		return ret

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

	def convert_to_int_if_possible(self, s):
		if s is None:
			return None
		try:
			val = float(s)
			return str(int(val) if val.is_integer() else val)
		except ValueError:
			return None