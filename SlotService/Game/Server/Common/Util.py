# -*- coding: utf-8 -*-

import os
import configparser
import copy as _copy


def Result(status, Src=None, *args, **kwargs):
    r = {} if 'OutParam' not in kwargs else kwargs['OutParam']
    r["Code"] = status
    # 透過args挑出想留下的欄位
    if Src is not None:
        if len(args) <= 0:
            r.update(Src)
        else:
            for k in args:
                r[k] = Src[k]
    # 透過kwargs合併欄位
    r.update(kwargs)
    r.pop('OutParam', None)
    return r


def CheckPermit(permitData, kind, checkData, **kwargs):
    def check(checkList, checkData):
        result = False

        for c in checkList:
            result = False

            for k, v in c.items():
                result = (v == checkData[k])
                if not result:
                    break

            if result:
                return result
        return result

    if (type(permitData) is dict) and (len(permitData) <= 0):
        return True

    baseCheck = len(kwargs) <= 0
    if len(kwargs) > 0:
        for k, v in kwargs.items():
            baseCheck = (permitData[k] == v)
            if not baseCheck:
                break

    deny = permitData.get('Deny')
    if deny is not None:
        deny = [d if type(d) is dict else {kind: d} for d in deny]
    dCheck = check(deny, checkData) if deny is not None else False

    allow = permitData.get('Allow')
    if allow is not None:
        allow = [a if type(a) is dict else {kind: a} for a in allow]
    aCheck = check(allow, checkData) if allow is not None else True

    return baseCheck and (not dCheck) and aCheck


def GetConfigOption(configPath, section, option, valType=str, **kwargs):
    ConfigObj = configparser.RawConfigParser()
    if not os.path.exists(configPath):
        raise Exception("Config file not found:{}".format(configPath))
    ConfigObj.read(configPath)
    if "default" in kwargs:
        if not ConfigObj.has_section(section):
            return kwargs["default"]
        if not ConfigObj.has_option(section, option):
            return kwargs["default"]
    return valType(ConfigObj.get(section, option))


def ReadConfigDict(configPath, section):
    ConfigObj = configparser.RawConfigParser()
    ConfigObj.read(configPath)
    if not ConfigObj.has_section(section):
        return None
    return dict(ConfigObj.items(section))


class Copy(object):
    @staticmethod
    def _copy_list(_l):
        ret = _copy.copy(_l)
        for idx, item in enumerate(ret):
            cp = Copy._dispatcher.get(type(item))
            if cp is not None:
                ret[idx] = cp(item)
        return ret

    @staticmethod
    def _copy_dict(d):
        ret = _copy.copy(d)
        for key, value in ret.items():
            cp = Copy._dispatcher.get(type(value))
            if cp is not None:
                ret[key] = cp(value)
        return ret

    _dispatcher = {
        list: _copy_list.__get__(object),
        dict: _copy_dict.__get__(object),
        int: lambda x: x,
        tuple: lambda x: x,
        str: lambda x: x,
        bool: lambda x: x,
        float: lambda x: x,
        type(None): lambda x: x,
    }

    @staticmethod
    def deepcopy(sth):
        cp = Copy._dispatcher.get(type(sth))
        if cp is None:
            return _copy.deepcopy(sth)
            # raise TypeError("Type not supported:{}/{}".format(sth,type(sth)))
        else:
            return cp(sth)

    @staticmethod
    def copy(sth):
        return _copy.copy(sth)


if __name__ == "__main__":
    import timeit
    import copy

    a = [1, 2, 3, 4, 5]
    b = {i: i + 1 for i in range(1000)}
    d = {"123": {"456": [[["i", "i", "i"], 2, 3], 8, 9, 10, 11]}}
    e = Copy.deepcopy(d)

    # d["123"]["456"][0][0][0] = 100
    print(d)
    print(e)
    print(timeit.timeit("Copy.deepcopy(d)", setup="from __main__ import Copy, d", number=10000))
    print(timeit.timeit("copy.deepcopy(d)", setup="from __main__ import copy, d", number=10000))
