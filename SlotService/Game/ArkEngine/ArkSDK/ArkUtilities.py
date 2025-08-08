# -*- coding: utf-8 -*-
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import object
import struct
import urllib.request, urllib.parse, urllib.error
import urllib.parse as urlparse
import requests

import geoip2.database
from future.utils import with_metaclass

__author__ = 'paddyyang'
'''
#--------- about json file ---------#
'''

try:
    import simplejson as json
except ImportError:
    import json
import time
import traceback, inspect
import sys


get_frame = getattr(sys, '_getframe')


class Singleton(type):
    # reference:
    # http://hychen.wuweig.org/blog/2012/03/13/metaclass-in-python-1-singleton/
    def __init__(cls, name, bases, dic):
        super(Singleton, cls).__init__(name, bases, dic)
        cls.instance = None

    def __call__(cls, *args, **kwargs):
        print("please use get_instance function to get the instance")

    def get_instance(cls, *args, **kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance


class Register(object):
    def __init__(self, logger):
        self.logger = logger
        self.cmd_funcs = {}

    def register_cmd(self, cmd, func):
        if cmd in self.cmd_funcs:
            self.logger.warning("%s.%s cmd already exists:%s, it will by override.\n%s" % (
            str(self.__class__.__name__), get_frame().f_code.co_name, cmd, traceback.format_exc()))
        self.cmd_funcs[cmd] = func

    def registerAllCmdFunc(self):
        # funcs=inspect.getmembers(self, predicate=inspect.ismethod)
        funcs = Register.getmembers(self, predicate=inspect.ismethod)
        for func in funcs:
            if str(func[0]).startswith('cmd_'):
                cmd_name = str(func[0]).replace('cmd_', '')
                methodToCall = getattr(self, str(func[0]))
                self.cmd_funcs[cmd_name] = methodToCall

    @staticmethod
    def getmembers(object, predicate=None):
        """Return all members of an object as (name, value) pairs sorted by name.
        Optionally, only return members that satisfy a given predicate."""
        results = []
        for key in dir(object):
            try:
                value = getattr(object, key)
                if not predicate or predicate(value):
                    results.append((key, value))
            except:
                pass
        results.sort()
        return results


def loadJsonFile(filePath):
    try:
        with open(filePath, 'r') as file:
            data = json.load(file)
            file.close()
            return data;
    except Exception as e:
        raise Exception('ArkUtilities.loadJsonFile :' + str(traceback.format_exc()));


def saveJsonFile(filePath, data):
    with open(filePath, 'w') as file:
        json.dumps(data, file);
        file.close();


'''
#--------- about file path ---------#
'''
import os


def getArkEnginePath(with_join_dir=None):
    try:
        uilities_path = os.path.split(os.path.realpath(__file__))
        engine_dir = os.path.split(uilities_path[0])[0]
        if with_join_dir:
            engine_dir = os.path.join(engine_dir, with_join_dir)
        return engine_dir
    except:
        return os.path.join(os.path.split(os.path.realpath(__file__)), '../../')


'''
#--------- about http ---------#
'''

# from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError
# def http_post_status_code(url, dict_post_data=None, header_dict=None, verify_host=False,
#                           timeout=10.0, session=None, cert_host=None, files=None):
#     # if session is None:
#     #     request_result = requests.post(url.encode('utf-8'), data=dict_post_data, headers=header_dict,
#     #                                    verify=verify_host, timeout=timeout, cert=cert_host, files=files)
#     # else:z
#     #     request_result = session.post(url.encode('utf-8'), data=dict_post_data, headers=header_dict,
#     #                                   verify=verify_host, timeout=timeout, cert=cert_host, files=files)
#     try:
#         if session is None:
#             request_result = requests.post(url, data=dict_post_data, headers=header_dict,
#                                            verify=verify_host, timeout=timeout, cert=cert_host, files=files)
#         else:
#             request_result = session.post(url, data=dict_post_data, headers=header_dict,
#                                           verify=verify_host, timeout=timeout, cert=cert_host, files=files)
#             # session.close()
#         return request_result.text, request_result.status_code
#     except RequestException as e:
#         print(f"Request failed: {e}")
#         return None, None

def http_post_status_code(url, dict_post_data=None, header_dict=None, verify_host=False,
                          timeout=10.0, session=None, cert_host=None, files=None):
    if session is None:
        request_result = requests.post(url, data=dict_post_data, headers=header_dict,
                                       verify=verify_host, timeout=timeout, cert=cert_host, files=files)
    else:
        request_result = session.post(url, data=dict_post_data, headers=header_dict,
                                      verify=verify_host, timeout=timeout, cert=cert_host, files=files)
    return request_result.text, request_result.status_code



def http_post(url, dict_post_data=None, header_dict=None, verify_host=False,
              timeout=10.0, session=None, cert_host=None, files=None):
    result, status_code = http_post_status_code(url, dict_post_data, header_dict, verify_host, timeout, session,
                                                cert_host, files)
    return result

def http_get(url, timeout=10.0, session=None, verify_host=False, headers=None):
    try:
        if session is None:
            request_result = requests.get(url.encode('utf-8'), timeout=timeout, verify=verify_host, headers=headers)
        else:
            request_result = session.get(url.encode('utf-8'), timeout=timeout, verify=verify_host, headers=headers)
        return request_result.text.encode('utf-8')
    except:
        raise

class StoreReceiveException(Exception):
    pass