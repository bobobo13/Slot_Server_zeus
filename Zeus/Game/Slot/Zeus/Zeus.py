#!/usr/bin/env python
# -*- coding: utf-8 -*-

from SlotCommon.slot_calculator import *


class Zeus(DefaultSlotCalculator):

	def _init_consts(self):
		self.FreeID = 1
		self.ThunderFireID = 2

	def get_init_reel_info(self, game_state):
		"""
		START_GAME的時候需要取得初始牌面
		:return:
		"""
		# 根据当前游戏阶段初始化牌面
		special_game_id = game_state.current_sg_id
		special_game_state = game_state.current_special_game_data

		if special_game_state is not None:
			# 目前是免费游戏阶段
			fake_reels = self._gameSetting['fake_fever_reels']["0"]
			if special_game_id == self.FreeID:
				if special_game_state['current_level'] == 1:
					view_reels = game_state.last_main_reels.get('0', {})
				else:
					view_reels = game_state.get_special_game_last_reels(special_game_id).get('0', {})
			elif special_game_id == self.ThunderFireID:
				if special_game_state['current_level'] == 1:
					view_reels = game_state.get_special_game_last_reels(self.FreeID).get('0', {})
				else:
					view_reels = game_state.get_special_game_last_reels(special_game_id).get('1', {})
		
		else:
			fake_reels = self._gameSetting['fake_main_reels']["0"]
			view_reels = game_state.last_main_reels.get('0', {})
		
		if len(view_reels) <= 0:
			view_reels = self._chance.get_init_reel(fake_reels)
		

		block_data = dict()
		ret = list()
		block_data.update({
			'id': 0,
			'init_wheels': view_reels,
			'fake_wheels': fake_reels,
		})
		ret.append(block_data)
		return ret
	
	def get_extra_info(self, game_state):
		# FOR START_GAME
		result = dict()
		if not game_state.is_special_game:
			result["thunder_value"] = game_state.extra_info.get("thunder_value", { str(col):[0]*self.reel_length for col in range(self.reel_amount)})
		return result
	
	def get_fever_recovery(self, game_state):
		result = dict()
		special_game_state = game_state.current_special_game_data
		if special_game_state is not None:
			result.update(special_game_state["current_script"])
			# 点击开始才开始
			result["recovery_need_start"] = True
		return result
	
	def get_spin_reel_data(self, gameInfo, is_fever=False):

		reel_key = "main_reels"
		gtype = "main"
		if is_fever:
			reel_key = "fever_reels"
			gtype = "fever"

		extra_odds = gameInfo["extra_odds"]
		# 获取
		# 過權重決定各輪要使用哪組轉輪帶, reels_set=[0, 1, 1, 0, 0], 0&1分別是轉輪帶代號
		_, reels_set = self.randomer.get_result_by_weight(extra_odds["spin_reel"][gtype]["result"],
											extra_odds["spin_reel"][gtype]["weight"])

		# 依各輪轉輪帶代號, 準備好5輪轉輪帶
		spin_reel_data = dict()
		for col in range(self.reel_amount):
			spin_reel_data[str(col)] = gameInfo[reel_key][str(reels_set[col])][str(col)]

		return spin_reel_data


	def spin(self, bet_value, bet_lines, game_state, gameInfo, dev_mode=DevMode.NONE, special_input_data=None, **kwargs):

		play_info = MainGamePlayInfo()
		play_info.set_is_fever_game(False)
		play_info.set_bet_info(bet_value, bet_lines)
		is_extra_bet = kwargs.get('extra_bet', False)
		play_info.set_extra_bet(is_extra_bet)
		extra_odds = gameInfo.get('extra_odds', {})

		block_id = 0
		result = MainGameResult([block_id])
		spin_reel_data = self.get_spin_reel_data(gameInfo, False)
		self._chance.get_spin_reels(result, block_id, spin_reel_data, bet_value, extra_odds["thunder"]["main_odds"], dev_mode=dev_mode)

		game_state.extra_info["thunder_value"] = result.get_extra_data()["thunder_value"]

		self._check.game_check(result, block_id, play_info, self._odds, self._special_odds, extra_odds, game_state,
							   self.reel_length, self.reel_amount, self.check_reel_length, self.check_reel_amount)
		main_win = result.this_win

		# AnalysysLog
		result.set_log_custom("MainReels", result.get_temp_special_game_data("CheckReel"))
		result.set_log_custom("MainWin", main_win)
		if result.get_temp_special_game_data("Multiplier", 1) > 1:
			result.set_log_custom("Multi", result.get_temp_special_game_data("Multiplier"))

		return result
	
	def after_spin_current_script(self, game_state):
		current_script = {}
		special_game_state = game_state.current_special_game_data
		if special_game_state is not None:
			current_level = special_game_state['current_level']
			if current_level == 1:
				current_script = special_game_state.get('current_script', {})
		return current_script
	
	def next_fever(self, client_action, game_state, gameInfo, dev_mode=DevMode.NONE, **kwargs):
		""" 特殊遊戲處理 """

		special_game_id = game_state.current_sg_id  # 目前所在的特殊遊戲
		fever_result = FeverLevelResult(special_game_id)  # 最後要回傳的內容

		# 遊戲狀態非特殊遊戲
		if not game_state.is_special_game:
			self.logger.error("[Zeus][next_fever] Not in special game, game:{}".format(self._game_id))
			fever_result.error = True
			return fever_result

		# 這邊依照遊戲自行設計
		if special_game_id == self.FreeID:
			self._free_game(fever_result, game_state, gameInfo, dev_mode)
		elif special_game_id == self.ThunderFireID:
			self._thunder_fire_game(fever_result, game_state, gameInfo, dev_mode)
		else:
			# 未知的特殊遊戲
			raise Exception("[ERROR] Error Special Game id:{}, game:{}".format(special_game_id, self._game_id))

		return fever_result

	def get_win_type(self, bet, win, gameState, result, winTypeInfo):
		# "NO_WIN": 0, "NORMAL_WIN": 1, "LIGHT_WIN": 2, "SMALL_WIN": 3, "BIG_WIN": 4,"MEGA_WIN": 5, "SUPER_WIN": 6
		type = 0
		if bet == 0 or win == 0 or bet is None or win is None:
			return 0
		win_bet_multiple = float(win) / bet
		for t, g in enumerate(winTypeInfo):
			if win_bet_multiple >= g:
				type = t
			else:
				break
		return type

	def build_result_log(self, ret_data, spin_result, jp_win, extra_bet=False):
		result_log = DefaultSlotCalculator.build_result_log(self, ret_data, spin_result, jp_win, extra_bet)
		# 需要顯示上下兩顆
		showReel = self._check.get_check_reel(spin_result, 0, self.reel_length, self.reel_amount, self.check_reel_length, self.reel_amount, transform=False)
		result_log['reel_info'] = [showReel]

		return result_log

	def get_feature_win_log(self, spin_result):
		feature_win_log = []
		end_feature = spin_result.get_reel_block_data(0).end_feature
		main_win = end_feature.get("Main", 0)
		feature_win_log.append({'feature_id':0, "win": main_win})
		return feature_win_log


	def _free_game(self, fever_result, game_state, gameInfo, dev_mode):
		""" 特殊遊戲: FreeGame

		Args:
			fever_result (FeverLevelResult): 要回傳的結果
			game_state (MainGameState): 遊戲狀態
			game_info (dict): 跟機率 (prod_id) 對應的 Info 資料
			dev_mode (DevMode): 測試模式 trigger
		"""
		# 特殊遊戲基本資料
		special_game_id = game_state.current_sg_id  # 特殊遊戲ID
		special_game_state = game_state.current_special_game_data  # 該特殊遊戲的資料
		bet_lines = special_game_state['current_line']  # 押注線數
		bet_value = special_game_state['current_bet']  # 押注倍數
		current_level = special_game_state['current_level']  # 目前特殊遊戲的狀態
		current_script = special_game_state.get('current_script', {})  # 對應特殊遊戲的暫存內容 (自定義資料)
		is_extra_bet = special_game_state.get('is_extra_bet', False)  # 是否有額外押注
		extra_odds = gameInfo.get('extra_odds', {})

		# 準備 play_info
		block_id = 0
		play_info = MainGamePlayInfo()
		play_info.SpecialGame = special_game_id
		play_info.set_bet_info(bet_value, bet_lines)

		if current_level == 1:
			fever_result.fever_map = FeverMap(1)
			
			special_game_state['current_level'] += 1
			fever_result.fever_map.append("total_times", current_script['total_times'])
			fever_result.fever_map.append("current_times", current_script['current_times'])
			fever_result.fever_map.append("fake_fever_reels", self._gameSetting['fake_fever_reels']["0"])
			fever_result.fever_map.append("is_extra_bet", is_extra_bet)
			
			thunder_value = game_state.extra_info.get("thunder_value", {})
			init_wheel = game_state.last_main_reels
			# 把结果存储起来
			game_state.update_last_fever_reels(special_game_id, init_wheel)
			current_script["thunder_value"] = thunder_value

			result = MainGameResult([block_id])
			reel_info = result.get_reel_block_data(block_id)
			reel_info.reel_data = init_wheel["0"]
			fever_result.fever_map.append("wheel_blocks", result.export_ex_wheel_block_result())
			fever_result.fever_map.append("thunder_value",  thunder_value)

		elif current_level == 2:
			result = MainGameResult([block_id])

			spin_reel_data = self.get_spin_reel_data(gameInfo, True)
			self._chance.get_spin_reels(result, block_id, spin_reel_data, bet_value, extra_odds["thunder"]["fever_odds"],spin_mode="fever",dev_mode=dev_mode)

			current_script["thunder_value"] = result.get_temp_special_game_data("thunder_value")

			self._check.game_check(result, block_id, play_info, self._odds, self._special_odds, extra_odds, game_state,
							   self.reel_length, self.reel_amount, self.check_reel_length, self.check_reel_amount)

			fever_result.win_amount = result.this_win
			current_script['current_times'] -= 1

			next_sg_id = special_game_id
			if result.is_win_special_game:
				sg_id_list = result.win_special_game_id_list
				# 中的是免费就把免费次数和总次数修改了
				if special_game_id in sg_id_list:
					win_times = result.get_special_game_times(special_game_id)
					current_script['current_times'] += win_times
					current_script['total_times'] += win_times
				
				if self.ThunderFireID in sg_id_list:
					next_sg_id = self.ThunderFireID
					spin_reels = result.spin_reels
					current_script_dict = copy.deepcopy(result.special_game_current_script)
					game_state.win_special_game_state([next_sg_id], bet_value, bet_lines, current_script_dict, spin_reels)

			if current_script['current_times'] <= 0 :
				fever_result.is_gameover = True
				# if result.get_special_game_times(self.ThunderFireID) <= 0:
				# 	fever_result.is_gameover = True
				# else:
				# 	# 如果最后一把中了Feature level设置为4
				# 	special_game_state['current_level'] += 1

			# 整理回傳的資料
			if fever_result.is_gameover:
				fever_result.fever_map = FeverMap(4)
				main_reel = game_state.last_main_reels.get('0', None)
				fever_result.fever_map.append("main_reels", main_reel)
				fever_result.fever_map.append("fake_main_reels", self._gameSetting['fake_main_reels']["0"])
				thunder_value = game_state.extra_info.get("thunder_value", {})
				fever_result.fever_map.append("main_thunder_value", thunder_value)
			else:
				fever_result.fever_map = FeverMap(2)

			fever_result.fever_map.append("wheel_blocks", result.export_ex_wheel_block_result())
			fever_result.fever_map.append("current_times", current_script['current_times'])
			fever_result.fever_map.append("next_sg_id", next_sg_id)
			fever_result.fever_map.append("total_times", current_script['total_times'])
			fever_result.fever_map.append("thunder_value", current_script["thunder_value"])
			fever_result.last_reel = result.spin_reels
			fever_result.show_reel = result.show_reel
		elif current_level == 3:
			# 最后一把
			fever_result.is_gameover = True
			fever_result.fever_map = FeverMap(4)
			main_reel = game_state.last_main_reels.get('0', None)
			fever_result.fever_map.append("main_reels", main_reel)
			fever_result.fever_map.append("fake_main_reels", self._gameSetting['fake_main_reels']["0"])
			thunder_value = game_state.extra_info.get("thunder_value", {})
			fever_result.fever_map.append("main_thunder_value", thunder_value)




	def _thunder_fire_game(self, fever_result, game_state, gameInfo, dev_mode):
		""" 特殊遊戲: ThunderFire

		Args:
			fever_result (FeverLevelResult): 要回傳的結果
			game_state (MainGameState): 遊戲狀態
			game_info (dict): 跟機率 (prod_id) 對應的 Info 資料
			dev_mode (DevMode): 測試模式 trigger
		"""

		# 特殊遊戲基本資料
		special_game_id = game_state.current_sg_id  # 特殊遊戲ID
		special_game_state = game_state.current_special_game_data  # 該特殊遊戲的資料
		bet_lines = special_game_state['current_line']  # 押注線數
		bet_value = special_game_state['current_bet']  # 押注倍數
		current_level = special_game_state['current_level']  # 目前特殊遊戲的狀態
		current_script = special_game_state.get('current_script', {})  # 對應特殊遊戲的暫存內容 (自定義資料)
		is_extra_bet = special_game_state.get('is_extra_bet', False)  # 是否有額外押注
		extra_odds = gameInfo.get('extra_odds', {})

		# 準備 play_info
		block_id = 1
		play_info = MainGamePlayInfo()
		play_info.SpecialGame = special_game_id
		play_info.set_bet_info(bet_value, bet_lines)

		if current_level == 1:
			fever_result.fever_map = FeverMap(1)
			
			special_game_state['current_level'] += 1
			
			fever_result.fever_map.append("thunder_current_times", current_script['thunder_current_times'])
			fever_result.fever_map.append("thunder_total_times", current_script['thunder_total_times'])
			fever_result.fever_map.append("is_extra_bet", is_extra_bet)

			next_special_game = game_state.next_special_game_data
			if next_special_game is not None:
				fever_result.fever_map.append("total_times", current_script['total_times'])
				fever_result.fever_map.append("current_times", current_script['current_times'])
				thunder_value = next_special_game["current_script"].get("thunder_value", {})
			else:
				thunder_value = game_state.extra_info.get("thunder_value", {})
			
			# 把结果存储起来
			reels, thunder_value, thunder_count = self._chance.get_init_thunder_reel(thunder_value)
			init_wheel = {str(block_id): reels}
			game_state.update_last_fever_reels(special_game_id, init_wheel)
			current_script["thunder_value"] = thunder_value

			# 先确定好增加的次数
			thunder_ext = self._get_thunder_ext(thunder_count, extra_odds["thunder_fire"])
			if thunder_ext is None:
				special_game_state["use_thunder_ext"] = True
			else:
				special_game_state["thunder_ext"] = thunder_ext
				# 是否已经增加了次数
				special_game_state["use_thunder_ext"] = False

			result = MainGameResult([block_id])
			reel_info = result.get_reel_block_data(block_id)
			reel_info.reel_data = reels
			fever_result.fever_map.append("wheel_blocks", result.export_ex_wheel_block_result())
			fever_result.fever_map.append("thunder_value",  thunder_value)

		elif current_level == 2:
			result = MainGameResult([block_id])

			empty_count = self._chance.get_thunder_result(result, block_id, current_script, bet_value, extra_odds["thunder_fire"], dev_mode)

			current_script["thunder_value"] = result.get_temp_special_game_data("thunder_value")

			current_script['thunder_current_times'] -= 1

			is_use_thunder_ext = False	# 是否使用此变量
			if empty_count == 0:
				fever_result.is_gameover = True
			elif current_script['thunder_current_times'] <= 0:
				if special_game_state["use_thunder_ext"]:
					fever_result.is_gameover = True
				else:
					is_use_thunder_ext = True
					# 标识为已使用
					special_game_state["use_thunder_ext"] = True
					current_script['thunder_current_times'] += special_game_state["thunder_ext"][0]

			# 整理回傳的資料
			if fever_result.is_gameover:
				# 结束的时候才收集结算
				self._check.thunder_win_check(result, block_id, play_info, extra_odds["thunder_fire"], empty_count)
				fever_result.win_amount = result.this_win

				fever_result.fever_map = FeverMap(4)
				# 如果不是从fg进来的就回到mg游戏
				if game_state.next_special_game_data is None:
					main_reel = game_state.last_main_reels.get('0', None)
					fever_result.fever_map.append("main_reels", main_reel)
					fever_result.fever_map.append("fake_main_reels", self._gameSetting['fake_main_reels']["0"])

				fever_result.fever_map.append("thunder_double", result.get_temp_special_game_data("thunder_double"))
			else:
				fever_result.fever_map = FeverMap(2)

			fever_result.fever_map.append("wheel_blocks", result.export_ex_wheel_block_result())
			fever_result.fever_map.append("thunder_value", current_script["thunder_value"])
			fever_result.fever_map.append("thunder_current_times", current_script['thunder_current_times'])
			if is_use_thunder_ext:
				fever_result.fever_map.append("thunder_ext", special_game_state["thunder_ext"])
			fever_result.last_reel = result.spin_reels
			fever_result.show_reel = result.show_reel

	def _get_thunder_ext(self, thunder_count, thunder_fire):
		"""
		return: [第一个为实际增加的值,其它值给前端随机]
		"""
		key = str(thunder_count)
		if key not in thunder_fire["add_count"]:
			return
		cfg = thunder_fire["add_count"][key]
		add_count = self.randomer.get_result_by_weight(cfg["result"], cfg["weight"])[1]

		return [add_count] + [value for value in cfg["result"] if value != add_count]
	
	






