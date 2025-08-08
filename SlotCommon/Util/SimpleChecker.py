# -*- coding: utf-8 -*-
from operator import *


def op_deny(target, value):
    if not isinstance(value, (set, dict, list, str)):
        return False
    return target not in value


def op_allow(target, value):
    if not isinstance(value, (set, dict, list, str)):
        return False
    return target in value


def op_among(target, value):
    if not isinstance(value, list) or len(value) != 2:
        return False
    return value[0] <= target <= value[1]


class SimpleChecker():
    OP_FUNC_MAP = {
        '>': gt,
        '>=': ge,
        '<': lt,
        '<=': le,
        '==': eq,
        '!=': ne,
        'deny': op_deny,
        'nin': op_deny,
        'allow': op_allow,
        'in': op_allow,
        'among': op_among
    }

    @staticmethod
    def check(op, target, value):
        """
            参数:
            value: 需要比较的值
            target: 比较的目标值
            op: 比较操作符
            返回值: 如果给定值满足比较条件，则返回 True，否则返回 False。
            """
        if op not in SimpleChecker.OP_FUNC_MAP:
            return False
        return SimpleChecker.OP_FUNC_MAP[op](target, value)
