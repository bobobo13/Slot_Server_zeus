#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy

from SlotCommon.game_check_ways import MainGameCheckWays


# ======================================================================
class ZeusCheck(MainGameCheckWays):
	def _init_const(self):
		"""
		"""
		self.ThunderSymbolID = 3
		self.ThunderFireID = 2
		self.ThunderFireLimitCount = 6


	def game_check(self, main_result, block_id, play_info, odds, special_odds, extra_odds, game_state, 
				reel_length, reel_amount, check_reel_length, check_reel_amount):
		"""
		:param main_result: 這次spin的所有資訊
		:param feature_reel: main game命中的feature在哪一輪，若-1表示沒中
		:param play_info: 遊戲的押注、選線、特殊遊戲狀態
		:param odds: 賠率表
		:param special_odds: scatter中獎獲得的次數
		:param extra_odds: 其他機率相關的設定數值
		:param game_state: 玩家游戏缓存信息
		:param reel_length: 包含不可視的symbol，一輪有多少顆symbol
		:param reel_amount: 包含不可視的symbol，有多少輪
		:param check_reel_length: 不包含不可視的symbol，一輪有多少顆symbol
		:param check_reel_amount: 不包含不可視的symbol，有多少輪
		:return:
		"""

		# note: local variable fast than class variable

		# ====================================================
		# show_reel: game log顯示的牌面, check_reel: 檢查各種獎項使用的牌面
		# 若有修改到show_reel，最後需要存回main_result
		show_reel = self.get_check_reel(main_result, block_id, reel_length, reel_amount, check_reel_length, check_reel_amount, transform=False)
		check_reel = copy.deepcopy(show_reel)
		reel_info = main_result.get_reel_block_data(block_id)
		main_result.set_temp_special_game_data("CheckReel", check_reel)

		# 檢查線獎贏分
		self.main_win_check(main_result, block_id, play_info, odds, check_reel, show_reel, check_reel_length, check_reel_amount)

		# 檢查free spin
		self.special_symbol_check(main_result, block_id, play_info, odds, special_odds, self.FeverSymbolID, self.FeverLimitCount, self.FeverID,
							check_reel, show_reel, check_reel_length, check_reel_amount, is_pass_line=False)
		# pre-win檢查
		self.pre_win_check(main_result, block_id, self.FeverSymbolID, self.FeverLimitCount, check_reel, show_reel, check_reel_length, check_reel_amount)

		# 检查ThunderFire
		self._thunder_symbol_check(main_result, block_id, special_odds, check_reel, game_state.current_special_game_data)

		if not reel_info.has_pre_win:
			# pre-win檢查
			self.pre_win_check(main_result, block_id, self.ThunderSymbolID, self.ThunderFireLimitCount, check_reel, show_reel, check_reel_length, check_reel_amount)
	
	def thunder_win_check(self, main_result, block_id, play_info, thunder_fire, empty_count):
		"""
		计算 thunder fire玩法的总赢
		:param thunder_fire:玩法相关配置
		:param empty_count:暗雷盾图腾的数量
		"""
		thunder_value = main_result.get_temp_special_game_data("thunder_value")
		thunder_double = thunder_fire["zero_empty_odds"] if empty_count == 0 else 0
		main_result.set_temp_special_game_data("thunder_double", thunder_double)

		total_win = 0
		for value_list in thunder_value.values():
			for value in value_list:
				if value > 0:
					# 普通雷盾
					total_win += value
				elif value < 0:
					# jackpot雷盾
					index = (-value) - 1
					total_win += play_info.line_bet * thunder_fire["jackpot_odds"][index]
		
		main_result.this_win += total_win*thunder_double if thunder_double > 0 else total_win


 	# =======================================檢查特殊symbol============================================
	def _thunder_symbol_check(self, main_result, block_id, special_odds, check_reel, special_game=None):
		"""
		檢查特殊symbol的中獎，包括贏得的次數和倍數
		次數設定在special odd中
		"""
		reel_info = main_result.get_reel_block_data(block_id)
		symbol_count = 0
		symbol_pos = { str(col):[-1]*len(check_reel[col]) for col in range(len(check_reel))}
		for col in range(len(check_reel)):
			for row in range(len(check_reel[col])):
				current_symbol = check_reel[col][row]
				if current_symbol == self.ThunderSymbolID:
					symbol_count += 1
					symbol_pos[str(col)][row] = 1

		if symbol_count >= self.ThunderFireLimitCount:
			key = "thunder_fire"
			symbol_count = min(symbol_count, len(special_odds[key]))
			win_times = special_odds[key][symbol_count-1]
			current_script = {
				'thunder_current_times': win_times,
				'thunder_total_times': win_times,
				'win_special_symbols': symbol_pos,
			}

			# 如果是从fg进来就把fg的次数的数据给加进来
			if special_game is not None:
				current_script.update({
					'current_times': special_game["current_script"]["current_times"],
					'total_times': special_game["current_script"]["total_times"],
				})

			reel_info = main_result.get_reel_block_data(block_id)
			reel_info.set_special_symbol_win_pos(self.ThunderFireID, symbol_pos)
			main_result.set_win_special_game(self.ThunderFireID, win_times)
			main_result.update_special_game_current_script(self.ThunderFireID, current_script)

	
	

		


