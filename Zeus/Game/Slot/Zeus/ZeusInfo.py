from collections import defaultdict

def transform_dict_list(data):
	result = defaultdict(list)
	for d in data:
		for key, value in d.items():
			result[key].append(value)
	return dict(result)


GameInfo = [
	{
		"game_id": "Zeus",
		"game_name": "Zeus",
		"chance_version": "Macross_v5(1)_20241219",
		"ProbId": "90",
		"base_game_rate": 9800,
		"main_reels": {
			"0":{
				"0":[10,11,12,13,14,15,16,17,18,19,2,3],
				"1":[10,11,12,13,14,15,16,17,18,19,2,3],
				"2":[10,11,12,13,14,15,16,17,18,19,2,3],
				"3":[10,11,12,13,14,15,16,17,18,19,2,3],
				"4":[10,11,12,13,14,15,16,17,18,19,2,3],
			}
		},
		"fever_reels": {
			"0":{
				"0":[10,11,12,13,14,15,16,17,18,19,2,3],
				"1":[10,11,12,13,14,15,16,17,18,19,19,3],
				"2":[10,11,12,13,14,15,16,17,18,19,2,3],
				"3":[10,11,12,13,14,15,16,17,18,19,19,3],
				"4":[10,11,12,13,14,15,16,17,18,19,2,3],
			}
		},
		"extra_odds": {
			# 有雷盾图腾时的用到的配置
			"thunder": {
				# mg游戏中金币倍数权重(odds*线注)
				"main_odds":transform_dict_list([
					{"result": 10, "weight":100},
					{"result": 20, "weight":100},
					{"result": 50, "weight":100},
					{"result": 100, "weight":100},
					{"result": 200, "weight":100},
					{"result": 500, "weight":100},
				]),
				# fg游戏中金币倍数权重
				"fever_odds":transform_dict_list([
					{"result": 10, "weight":100},
					{"result": 20, "weight":100},
					{"result": 50, "weight":100},
					{"result": 100, "weight":100},
					{"result": 200, "weight":100},
					{"result": 500, "weight":100},
				]),
			},
			# thunder_fire相关配置
			"thunder_fire": {
				# 在thunder_fire游戏过程中reels出现类型的权重(0:没中，1:中倍数，2:中jackpot)
				"win_type":transform_dict_list([
					{"result":0, "weight":1000},
					{"result":1, "weight":100},
					{"result":2, "weight":10},
				]),
				# 中倍数时的倍数权重
				"odds":transform_dict_list([
					{"result": 10, "weight":100},
					{"result": 20, "weight":100},
					{"result": 50, "weight":100},
					{"result": 100, "weight":100},
					{"result": 200, "weight":100},
					{"result": 500, "weight":100},
				]),
				# 中jackpot时确定jackpot类型
				"jackpot":transform_dict_list([
					{"type":1, "weight":100},
					{"type":2, "weight":100},
					{"type":3, "weight":100},
					{"type":4, "weight":100},
				]),
				# jackpot 赔付
				"jackpot_odds":[5,15,50,100],
				# 满屏雷顿额外倍数
				"zero_empty_odds":2,
				# 额外中的次数
				"add_count":{
					"6":{"result":[1,2,3], "weight":[100,50,20]},
					"7":{"result":[1,2,3], "weight":[100,50,20]},
					"8":{"result":[2,3,4], "weight":[100,50,20]},
					"9":{"result":[2,3,4], "weight":[100,50,20]},
					"10":{"result":[2,3,4], "weight":[100,50,20]},
				}
			},
			"spin_reel":{
				"main": transform_dict_list([
					{"result":[0,0,0,0,0], "weight":1},
				]),
				"fever":transform_dict_list([
					{"result":[0,0,0,0,0], "weight":1},
				]),
			},
			
		},
	},
]
