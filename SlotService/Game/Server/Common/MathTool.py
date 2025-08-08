#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math


# floor_float精準度會根據整數位數做調整
def floor_float(fValue, n):
    if isinstance(fValue, float):
        float_value = fValue
    elif isinstance(fValue, int):
        float_value = float(fValue)
    elif isinstance(fValue, str):
        float_value = float(fValue)
    else:
        raise TypeError("floor_float, fValue={}, type={}".format(fValue, type(fValue)))

    # print 'float_value: {:.20f}'.format(float_value)
    precision = math.ulp(float_value)
    decimal_value = pow(10, n)
    float_value = float_value + precision
    float_value = float_value * decimal_value
    # print 'float_value: {:.20f}'.format(float_value)
    int_value = int(float_value)
    float_value = float(int_value) / decimal_value
    return float_value


# round_float精準度會根據整數位數做調整
def round_float(fValue, n):
    float_value = fValue
    # print 'float_value: {:.20f}'.format(float_value)
    precision = math.ulp(float_value)
    float_value = float_value + precision
    return round(float_value, n)


# ceil_float精準度會根據整數位數做調整
def ceil_float(fValue, n):
    precision = math.ulp(fValue)
    floor_float_value = floor_float(fValue, n)
    if fValue - floor_float_value > precision:
        decimal_value = pow(0.1, n)
        return floor_float_value + decimal_value
    return floor_float_value


# 浮點數轉字串時去除非必要小數
def float_to_string_without_tail_zero(fValue, precision=2):
    return str(floor_float(fValue, precision)).rstrip("0").rstrip(".")
