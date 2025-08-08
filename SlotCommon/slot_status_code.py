# -*- coding: utf-8 -*-
__author__ = 'yuhaolu'


# 要新增enumeration的話，請從每個種類的最後index繼續新增
class enum:
    def __init__(self, **enums):
        self.__dict__['_instance'] = type('enum', (), enums)

    def __setattr__(self, name, value):
        self._instance.__dict__[name] = value

    def __getattr__(self, name):
        return self._instance.__dict__[name]

    def is_defined(self, e):
        return self._instance.__dict__.has_key(e)


SLOT_TYPE = enum(
    LINES=0,
    WAYS=1,
    COUNTS=2
)

DevMode = enum(
    NONE=0,
    FREE_SPIN=1,
    FEVER=2,
    BONUS=3,
    FEATURE=4,
    JP_GAME=5,
    GOOD_WIN=6,
    BIG_WIN=7,
    MEGA_WIN=8,
    SUPER_WIN=9,

    SYMBOL_10_ALL=10,
    SYMBOL_11_ALL=11,
    SYMBOL_12_ALL=12,
    SYMBOL_13_ALL=13,
    SYMBOL_14_ALL=14,
    SYMBOL_15_ALL=15,
    SYMBOL_16_ALL=16,
    SYMBOL_17_ALL=17,
    SYMBOL_18_ALL=18,
    SYMBOL_19_ALL=19,
    SYMBOL_20_ALL=20,

    BONUS_MULTI_BASE=101,
    GAME_7_BONUS=102,
    SYMBOL_CHECK=103,
    PREWIN=104,
    MYSTERY=105,

    GUIDE_BIG_WIN=201,
    GUIDE_MEGA_WIN=202,

    ITEM_TRIGGER_SPECIAL_GAME='ITEM_TRIGGER_SPECIAL_GAME',
    BUY_BONUS='BUY_BONUS'

)
