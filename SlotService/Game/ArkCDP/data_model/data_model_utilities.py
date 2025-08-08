# -*- coding: utf-8 -*-
import time
import re
import six
from decimal import Decimal
from decimal import getcontext
from sys import platform as _platform

getcontext().prec = 38
if _platform == "win32":
    date_reg_pattern = r'^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$'
else :
    date_reg_pattern = '^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$'

if six.PY3:
    NUMBER_TYPE = (int, float)
    STRING_TYPE = (str,)
    INT_TYPE = (int,)
    BIG_NUMBER_TYPE = (int, float, Decimal)
else:
    NUMBER_TYPE = (int, long, float)
    STRING_TYPE = (str, unicode)
    INT_TYPE = (int, long)
    BIG_NUMBER_TYPE = (int, long, float, Decimal)


def timestamp_valid(fun_name, attr_name, ts, allow_none=False):
    err_mess = None
    if type(ts) in NUMBER_TYPE:
        return err_mess, int(ts * 1000000)

    send_err = '%s: %s is %s it must be time.time()' % (fun_name, attr_name, ts)
    if ts is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, int(time.time() * 1000000)
    elif type(ts) is str:
        return err_mess, int(ts)
    else:
        err_mess = send_err
        return err_mess, int(time.time() * 1000000)


def timestamp_valid_base(fun_name, attr_name, ts, allow_none=True):
    err_mess = None
    if type(ts) in NUMBER_TYPE:
        return err_mess, int(ts * 1000000)

    send_err = '%s: %s is %s it must be time.time()' % (fun_name, attr_name, ts)
    if ts is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, 0
    elif type(ts) is str:
        return err_mess, int(ts)
    else:
        err_mess = send_err
        return err_mess, 0


def str_valid(fun_name, attr_name, val, default, allow_none=False, replace_special_word=False):
    err_mess = None
    if type(val) in STRING_TYPE:
        if replace_special_word:
            return err_mess, replace_special(val, '')
        else:
            return err_mess, val

    send_err = '%s: %s is %s it must be str or unicode' % (fun_name, attr_name, val)
    if val is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, default
    elif type(val) in INT_TYPE:
        return err_mess, str(val)
    elif type(val) in (float, Decimal):
        return err_mess, format(val, '.5f')
    else:
        err_mess = send_err
        return err_mess, default


def str_valid_base(val, default, replace_special_word=False):
    if type(val) in STRING_TYPE:
        if replace_special_word is True:
            return replace_special(val, '')
        else:
            return val

    if val is None:
        return default
    elif type(val) in INT_TYPE:
        return str(val)
    elif type(val) in (float, Decimal):
        return format(val, '.5f')
    else:
        return default


def int_valid(fun_name, attr_name, val, default, allow_none=False):
    err_mess = None

    if type(val) in INT_TYPE:
        return err_mess, val

    send_err = '%s: %s is %s it must be int' % (fun_name, attr_name, val)
    if val is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, default

    elif type(val) in (float, Decimal):
        return err_mess, int(val)
    elif type(val) in STRING_TYPE:
        if not val.replace('-', '', 1).replace('.', '', 1).isdigit():
            err_mess = send_err
            return err_mess, default
        else:
            return err_mess, int(float(val))
    else:
        err_mess = send_err
        return err_mess, default


def float_valid(fun_name, attr_name, val, default, allow_none=False, allow_str=False):
    err_mess = None

    if type(val) is float:
        return err_mess, val

    send_err = '%s: %s is %s it must be float' % (fun_name, attr_name, val)
    if val is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, default
    elif type(val) in INT_TYPE:
        return err_mess, val
    elif type(val) is Decimal:
        return err_mess, val
    elif type(val) in STRING_TYPE:
        if not val.replace('-', '', 1).replace('.', '', 1).isdigit():
            err_mess = send_err
            return err_mess, default
        else:
            if allow_str:
                return err_mess, val
            else:
                return err_mess, float(val)
    else:
        err_mess = send_err
        return err_mess, default


def int_and_float_valid_base(val, default):
    return default if val is None else val


def longitude_valid(fun_name, attr_name, val, default, allow_none=False):
    err_mess = None

    if type(val) in NUMBER_TYPE:
        if val == default:
            return err_mess, val
        elif val > 180 or val < -180:
            send_err = '%s: %s is %s it must be float and range in -180~180' % (fun_name, attr_name, val)
            err_mess = send_err
            return err_mess, default
        else:
            return err_mess, val

    send_err = '%s: %s is %s it must be float and range in -180~180' % (fun_name, attr_name, val)
    if val is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, default
    elif type(val) in STRING_TYPE:
        if not val.replace('-', '', 1).replace('.', '', 1).isdigit():
            err_mess = send_err
            return err_mess, default
        else:
            if float(val) > 180 or float(val) < -180:
                err_mess = send_err
                return err_mess, default
            else:
                return err_mess, float(val)
    else:
        err_mess = send_err
        return err_mess, default


def latitude_valid(fun_name, attr_name, val, default, allow_none=False):
    err_mess = None

    if type(val) in NUMBER_TYPE:
        if val == default:
            return err_mess, val
        elif val > 90 or val < -90:
            send_err = '%s: %s is %s it must be float and range in -90~90' % (fun_name, attr_name, val)
            err_mess = send_err
            return err_mess, default
        else:
            return err_mess, round(val, 8)

    send_err = '%s: %s is %s it must be float and range in -90~90' % (fun_name, attr_name, val)
    if val is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, default
    elif type(val) in STRING_TYPE:
        if not val.replace('-', '', 1).replace('.', '', 1).isdigit():
            err_mess = send_err
            return err_mess, default
        else:
            if float(val) > 90 or float(val) < -90:
                err_mess = send_err
                return err_mess, default
            else:
                return err_mess, round(float(val), 8)
    else:
        err_mess = send_err
        return err_mess, default


def date_valid(fun_name, attr_name, val, default, allow_none=False):
    err_mess = None

    if type(val) in STRING_TYPE:
        if re.match(date_reg_pattern, val) is None:
            send_err = '%s: %s is %s it must be YYYY-mm-DD' % (fun_name, attr_name, val)
            err_mess = send_err
            return err_mess, default
        else:
            return err_mess, val

    send_err = '%s: %s is %s it must be YYYY-mm-DD' % (fun_name, attr_name, val)
    if val is None:
        if allow_none is False:
            err_mess = send_err
        return err_mess, default
    else:
        err_mess = send_err
        return err_mess, default


def negative_valid(val):
    r = False
    if type(val) in BIG_NUMBER_TYPE:
        if val < 0:
            r = True
    elif type(val) in STRING_TYPE:
        if val.replace('-', '', 1).replace('.', '', 1).isdigit():
            if val.find('-') > -1:
                r = True
    return r


def positive_valid(val):
    r = False
    if type(val) in STRING_TYPE:
        if val.replace('-', '', 1).replace('.', '', 1).isdigit():
            if val.find('-') == -1:
                r = True
    elif type(val) in BIG_NUMBER_TYPE:
        if val > 0:
            r = True
    return r


def float_round(val):
    return round(val, 5)


def convert_large_numbers(val, large_numbers):
    if val is None:
        return 0
    elif large_numbers is True:
        return format(Decimal(val), '.5f')
    else:
        return float_round(val)


# 將字串移除\n \r \t “雙引號 ,逗號
def replace_special(val, default=""):
    if type(val) in STRING_TYPE:
        return val.strip().replace("\n", "").replace("\r", "").replace("\t", "").replace("\f", "").replace("\v",
                                                                                                           "").replace(
            "\"", "").replace(",", "")
    elif type(val) in BIG_NUMBER_TYPE:
        return str_valid_base(val, '0')
    else:
        return default


# 將以秒為單位浮點數轉為整數16位，非int、long、float、Decimal會自動填0
def convert_timestamp(ts):
    if type(ts) in BIG_NUMBER_TYPE:
        return int(ts * 1000000)
    else:
        return 0
