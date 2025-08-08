# <editor-fold desc="Hide">
cost = 1
if __name__ == "__main__":
	from Zeus.Game.Slot.Zeus.ZeusInfo import GameInfo
else:
	from .ZeusInfo import GameInfo

from SlotCommon.total_bet_list import get_total_bet_list
from SlotCommon.Util.MathTool import floor_float

Setting = {     # 1
	"game_id": "Zeus",
	"game_name": "Zeus",
	"ProbId": 97,
	"line_bet_list": [floor_float(float(bet) / cost, 3) for bet in get_total_bet_list("Zeus")],
	"slot_type": "allways",
	"max_costs": 88,  # total_bet = line_bet * max_costs(cost)
	"cost": 88,  # 新增cost欄位，計算total_bet盡量用這個，也能用這來辨認是該遊戲是否適配單一押注段
	"winnable_lines": cost,  # 可贏線數，即拿來計算線獎的線數
	"reel_amount": 5,
	"reel_length": 5,
	"check_reel_amount": 5,
	"check_reel_length": 3,
	"extra_info": {
		"jackpot_odds":[5,15,50,100],
		"fake_thunder_value":{
			"0":[10, 50, 100, 150, 200],
			"1":[10, 50, 100, 150, 200],
			"2":[10, 50, 100, 150, 200],
			"3":[10, 50, 100, 150, 200],
			"4":[10, 50, 100, 150, 200],
		},
		"fake_thunder_weight":{
			"0":[800, 600, 450, 150, 0],
			"1":[800, 600, 450, 150, 0],
			"2":[800, 600, 450, 150, 0],
			"3":[800, 600, 450, 150, 0],
			"4":[800, 600, 450, 150, 0],
		}
	},
	"extra_bet": {"Enable": False, "Ratio": 1.5},
	"fake_main_reels": {
		"0":{
			"0":[10,11,12,13,14,15,16,17,18,19,2,3],
			"1":[10,11,12,13,14,15,16,17,18,19,2,3],
			"2":[10,11,12,13,14,15,16,17,18,19,2,3],
			"3":[10,11,12,13,14,15,16,17,18,19,2,3],
			"4":[10,11,12,13,14,15,16,17,18,19,2,3],
		}
		
	},
	"fake_fever_reels": {
		"0":{
			"0":[10,11,12,13,14,15,16,17,18,19,2,3],
			"1":[10,11,12,13,14,15,16,17,18,19,2,3],
			"2":[10,11,12,13,14,15,16,17,18,19,2,3],
			"3":[10,11,12,13,14,15,16,17,18,19,2,3],
			"4":[10,11,12,13,14,15,16,17,18,19,2,3],
		}
	},
	"special_odds": {
		"fever":[0,0,8,16,24],
		"fever_again":[0,0,8,16,24],
		"thunder_fire":[0,0,0,0,0,6,7,8,9,10],
	},
	"odds": {
		"1":[0,0,1,2,3],	# Scatter	
		"3":[0,0,0,0,0],	# Feature(閃電盾)
		"4":[0,0,0,0,0],	# 暗閃電盾

		"10":[0,8,25,45,100],	# M1
		"11":[0,5,15,40,80],	# M2
		"12":[0,5,15,40,80],	# M3
		"13":[0,0,10,30,70],	# M4
		"14":[0,0,10,30,70],	# M5
		"15":[0,0,7,15,40],		# M6
		"16":[0,0,7,15,40],		# M7
		"17":[0,0,7,15,40],		# M8
		"18":[0,0,7,15,40],		# M9
		"19":[0,0,7,15,40],		# M10
		
		
	}
}
