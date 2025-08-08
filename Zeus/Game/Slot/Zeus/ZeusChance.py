#!/usr/bin/env python
# -*- coding: utf-8 -*-

from SlotCommon.game_chance import MainGameChance
from SlotCommon.slot_status_code import *
from SlotCommon.main_game_result import *
from SlotCommon.Util.Util import Copy as copy


# ======================================================================
class ZeusChance(MainGameChance):
	FeverSymID = 2
	ThunderSymID = 3
	DarkThunderSymID = 4
	NormalSymList = [10,11,12,13,14,15,16,17,18,19] # 其它普通图腾

	def get_init_reel(self, reel_data):
		
		main_result = MainGameResult([0])
		reel_info = main_result.get_reel_block_data(0)
		self._get_spin_result_from_reel(reel_info, reel_data, self.reel_length, self.reel_amount)
		# 把Thunder图腾修改掉
		reels = { 
			col:[ symbol if symbol != self.ThunderSymID else self.NormalSymList[self.randomer.randint(0, len(self.NormalSymList)-1)] for symbol in reel ] 
			for col, reel in reel_info.reel_data.items()
		}


		return reels
	

	def get_init_thunder_reel(self, thunder_value):
		"""
		这个玩法滚轴改成可见
		初始化thunder结果
		return: reels:初始化的界面, new_thunder_value:新的thunder_value,thunder_count:雷盾图腾的数量(可见数量)
		"""
		thunder_count = 0
		reels = {}
		new_thunder_value = {}
		start = self.invisible_symbols//2
		end = start+self.check_reel_length-1
		for col, value_list in thunder_value.items():
			reel = [self.DarkThunderSymID]*self.check_reel_length
			values = [0]*self.check_reel_length
			for row, value in enumerate(value_list):
				if row < start or row > end:
					continue
				index = row-start
				if value != 0:
					reel[index] = self.ThunderSymID
					values[index] = value
					thunder_count += 1
				
			reels[col] = reel
			new_thunder_value[col] = values

		return reels, new_thunder_value, thunder_count


	
	def get_thunder_info(self, reels, bet_value, thunder_odds):
		thunder_value = {}
		for col, reel in reels.items():
			data_list = [0]*self.reel_length
			for row, symbol in enumerate(reel) :
				if symbol == self.ThunderSymID:
					odds = self.randomer.get_result_by_weight(thunder_odds["result"], thunder_odds["weight"])[1]
					data_list[row] = odds*bet_value

			thunder_value[col] = data_list

		return thunder_value

	def get_spin_reels(self, main_result, block_id, reel_data, bet_value, thunder_odds, spin_mode="normal", dev_mode=DevMode.NONE):
		"""
		:param main_result: 這次spin的所有資訊
		:param reel_data: fever_reels对应的类型权重
		:param thunder_odds: 闪电倍数权重(odds*线注)
		"""

		self.get_spin_result(main_result, block_id, reel_data, self.reel_length, self.reel_amount,self.check_reel_length, self.check_reel_amount, dev_mode, spin_mode=spin_mode)
		reel_info = main_result.get_reel_block_data(block_id)
		thunder_value = self.get_thunder_info(reel_info.reel_data, bet_value, thunder_odds)

		if spin_mode == "normal":
			main_result.set_extra_data("thunder_value", thunder_value)
		else:
			main_result.set_temp_special_game_data("thunder_value", thunder_value)


	def get_thunder_result(self, main_result, block_id, current_script, bet_value, thunder_fire, dev_mode=DevMode.NONE):
		"""
		:param main_result: 這次spin的所有資訊
		:param thunder_fire: 玩法相关配置
		return: 空的数量
		"""
		thunder_value = current_script["thunder_value"]
		reel_info = main_result.get_reel_block_data(block_id)

		# 空的数量
		empty_count = 0

		for col, value_list in thunder_value.items():
			reel = [self.DarkThunderSymID]*self.check_reel_length
			for row, value in enumerate(value_list) :
				if value == 0:
					# 获取中奖类型
					type = self.randomer.get_result_by_weight(thunder_fire["win_type"]["result"], thunder_fire["win_type"]["weight"])[1]
					if type == 0:
						empty_count += 1
						continue

					reel[row] = self.ThunderSymID
					value = 0
					if type == 1:
						odds = self.randomer.get_result_by_weight(thunder_fire["odds"]["result"], thunder_fire["odds"]["weight"])[1]
						value = bet_value*odds
					elif type == 2:
						jackpot = self.randomer.get_result_by_weight(thunder_fire["jackpot"]["type"], thunder_fire["jackpot"]["weight"])[1]
						value = -jackpot
					
					# 修改i对应的结果值
					thunder_value[col][row] = value
				
				else:
					reel[row] = self.ThunderSymID

			reel_info.set_one_reel_data(col, reel)

		main_result.set_temp_special_game_data("thunder_value", thunder_value)

		return empty_count
	
	def _get_dev_spin_result(self, reel_info, dev_mode):
		"""
		測試用直接指定牌面
		:param reel_info:
		:retur
		"""

		if dev_mode == 1: # not Win
			for i in range(self.reel_amount):
				reel_info.set_one_reel_data(i, [i+10]*self.reel_length)
		elif dev_mode == 2: # Big Win 10
			for i in range(self.reel_amount):
				symbol = 19 if i < 4 else 10+i
				reel_info.set_one_reel_data(i, [symbol]*self.reel_length)
		elif dev_mode == 3: # Mega Win 20
			for i in range(self.reel_amount):
				symbol = 14 if i < 4 else 15+i
				reel_info.set_one_reel_data(i, [symbol]*self.reel_length)
		elif dev_mode == 4: # Super Win 50
			for i in range(self.reel_amount):
				reel_info.set_one_reel_data(i, [14]*self.reel_length)
		elif dev_mode == 5: # 免费
			for i in range(self.reel_amount):
				reel = [i+10]*self.reel_length
				if i < 3:
					reel[self.invisible_symbols//2] = self.FeverSymID
				reel_info.set_one_reel_data(i, reel)
		elif dev_mode == 6: # thunder
			for i in range(self.reel_amount):
				reel = [self.ThunderSymID]*self.reel_length if i < 2 else [i+10]*self.reel_length
				reel_info.set_one_reel_data(i, reel)

	def _get_dev_fever_result(self, reel_info, reel_data, reel_length, reel_amount, dev_mode=DevMode.NONE, **kwargs):
		"""
		測試用直接指定牌面
		:param reel_info:
		:return:
		"""
		if dev_mode == 5: # 免费
			for i in range(self.reel_amount):
				reel = [i+10]*self.reel_length
				if i < 3:
					reel[self.invisible_symbols//2] = self.FeverSymID
				reel_info.set_one_reel_data(i, reel)
		elif dev_mode == 6: # thunder
			for i in range(self.reel_amount):
				reel = [self.ThunderSymID]*self.reel_length if i < 2 else [i+10]*self.reel_length
				reel_info.set_one_reel_data(i, reel)

			

if __name__ == "__main__":
	from SlotCommon.IgsRandomer import IgsRandomer
	randomer = IgsRandomer()
	chance = ZeusChance(randomer=randomer)
	chance.init_game_reel_info(5,3,5,3)
	main_result = MainGameResult([0])
	reels = chance.get_init_reel({
				"0":[10,11,12,13,14,15,16,17,18,19,1,2,3],
				"1":[10,11,12,13,14,15,16,17,18,19,1,2,3],
				"2":[10,11,12,13,14,15,16,17,18,19,1,2,3],
				"3":[10,11,12,13,14,15,16,17,18,19,1,2,3],
				"4":[10,11,12,13,14,15,16,17,18,19,1,2,3],
			})
	print(reels)
