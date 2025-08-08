# coding=utf-8
Default_A = [50, 100, 200, 300, 500, 800, 1000, 2000, 3000, 5000, 8000, 10000, 20000, 30000, 50000, 80000,
             100000, 200000, 300000, 500000, 800000, 1000000]
Default_B = [100, 200, 300, 500, 800, 1000, 2000, 3000, 5000, 8000, 10000, 20000, 30000, 50000, 80000,
             100000, 200000, 300000, 500000, 800000, 1000000]

# 押注段支援哪些遊戲
Default_A_games = ['LionDance', 'ChaChaCha', 'RobinHood', 'FortuneGems', 'DragonBaoBao',
                   'HulaoBattle', 'CandyRush', 'Razor', 'WuJinPen', 'Zeus']
Default_B_games = []

# init_fixed_total_bet_info.py 會將押注段支援的遊戲分類寫入 db
# token-verify 時才能從 game id 對照回押注段類型
bet_category_list = [{'bet_category': 'Default_A', 'game_list': Default_A_games},
                     {'bet_category': 'Default_B', 'game_list': Default_B_games}]


# 僅供遊戲初始化時使用
def get_total_bet_list(game_id):
    if game_id in Default_A_games:
        return Default_A
    elif game_id in Default_B_games:
        return Default_B
    else:
        return []
