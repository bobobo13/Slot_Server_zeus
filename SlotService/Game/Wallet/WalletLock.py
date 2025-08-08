# -*- coding: utf-8 -*-
from __future__ import print_function

import future.standard_library
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
__author__ = "eitherwang"

import traceback
import redis
import os, socket
import sys
if future.standard_library.PY2:
    import ConfigParser as configparser
else:
    import configparser

class WalletLock(object):
    # _DataSource = redis.StrictRedis(db=2)
    _DataSource = None
    _Logger = None
    _TransIdFunc = None
    PROC_ID = socket.gethostname() + "." + str(os.getpid())
    LOCK_KEY = "WalletLock:{}"
    CONFIG_SECTION = "WalletLock"
    DEFAULT_LOCK_TTL = 60

    @classmethod
    def initCls(cls, Logger, strLockKey=None, WalletLockDataSource=None, **kwargs):
        cls._Logger = Logger
        if strLockKey is not None:
            cls.LOCK_KEY = strLockKey
        cls._DataSource = WalletLockDataSource
        if cls._DataSource is None:
            host, port, db = kwargs.get("RedisHost", "localhost"), kwargs.get("RedisPort", 6379), kwargs.get("RedisDb", 2)
            if kwargs.get("WalletLockConfig"):
                configObj = configparser.RawConfigParser()
                configObj.read([kwargs.get("WalletLockConfig")])
                host = configObj.get(cls.CONFIG_SECTION, "RedisHost")
                port = configObj.getint(cls.CONFIG_SECTION, "RedisPort")
                db = configObj.getint(cls.CONFIG_SECTION, "RedisDB")
                if strLockKey is None and configObj.has_option(cls.CONFIG_SECTION, "LockKey"):
                    cls.LOCK_KEY = configObj.get(cls.CONFIG_SECTION, "LockKey")
                if configObj.has_option(cls.CONFIG_SECTION, "TTL"):
                    cls.DEFAULT_LOCK_TTL = configObj.getint(cls.CONFIG_SECTION, "TTL")
            cls._DataSource = redis.StrictRedis(host, port, db)
        WalletLock._TransIdFunc = kwargs.get("TransIdFunc", WalletLock._TransId)

    def __init__(self, ark_id, PlayerData=None, ttl=None, bCheckSameProcId=True):
        if ark_id is None:
            raise
        self.ark_id = ark_id
        self.PlayerData = PlayerData
        self.TransIdFunc = self._TransIdFunc
        user_id = self.TransIdFunc(ark_id, PlayerData=PlayerData)
        self.key = WalletLock.LOCK_KEY.format(user_id)
        self._ttl = ttl
        self.ReleaseRequired = False
        self._bCheckSameProcId = bCheckSameProcId

    def __enter__(self):
        success = self.Acquire()
        if not success:
            return False
        return True

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._Logger.error("%s.%s \n%s %s %s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, exc_type, exc_val, traceback.format_exc()))
        if self.ReleaseRequired:
            self.Release()

    def Acquire(self, ttl=None, bCheckSameProcId=None):
        """
        ttl: (int) Time to live (seconds), None for default, 0 for forever
        :return: (bool) True if this process has locked or get lock successfully, otherwise False
        """
        key = self.key
        if key is None:
            return False
        if ttl is None:
            ttl = self._ttl or self.DEFAULT_LOCK_TTL
        if ttl <= 0:
            ttl = None
        if bCheckSameProcId is None:
            bCheckSameProcId = self._bCheckSameProcId
        try:
            r = WalletLock._DataSource.get(key)
        except:
            self._Logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return False
        if r == WalletLock.PROC_ID:
            return True
        if r is not None:
            if bCheckSameProcId:
                return False
            return True
        try:
            r = WalletLock._DataSource.set(key, WalletLock.PROC_ID, nx=True, ex=ttl)
        except:
            self._Logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return False
        if not r:
            return False
        self.ReleaseRequired = True
        return True

    def Release(self):
        key = self.key
        if key is None:
            return
        try:
            r = WalletLock._DataSource.delete(key)
        except:
            self._Logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))

    def IsLocked(self):
        """
        :return: True if key exists, else false
        """
        key = self.key
        if key is None:
            return None
        r = None
        try:
            r = WalletLock._DataSource.get(key)
        except:
            self._Logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
        return r is not None

    def Refresh(self, ttl=None):
        key = self.key
        if key is None:
            return
        if ttl is None:
            ttl = self._ttl or self.DEFAULT_LOCK_TTL
        if ttl <= 0:
            return
        try:
            r = WalletLock._DataSource.expire(key, ttl)
        except:
            self._Logger.error("%s.%s \n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return
        if not r:
            return
        self.ReleaseRequired = True
        return

    def _TransId(self, ark_id, **kwargs):
        return ark_id


import logging
import socket
import redis
import os
class Tester(object):
    USERNAME = "ABC"

    def TransIdF(self, ark_id, **kwargs):
        return self.USERNAME

    def testWalletLock(self):
        import logging
        redisCli = redis.StrictRedis(db=2)
        logging.basicConfig()
        logger = logging.getLogger("TestWallet")
        logger.setLevel(logging.DEBUG)

        WalletLock.initCls(logger, WalletLockDataSource=redisCli, TransIdFunc=self.TransIdF)
        # WalletLock.initCls(logger, WalletLockDataSource=redisCli)
        print((WalletLock.PROC_ID, socket.gethostname() + "."+ str(os.getpid())))

        user_id = ark_id = "0"
        user_id = self.TransIdF(ark_id)
        redisCli.delete(WalletLock.LOCK_KEY.format(user_id))
        assert WalletLock(ark_id).IsLocked() is False
        with WalletLock(ark_id) as success:
            assert success is True
            assert WalletLock(ark_id).IsLocked() is True
        assert WalletLock(ark_id).IsLocked() is False

        redisCli.delete(WalletLock.LOCK_KEY.format(user_id))
        redisCli.set(WalletLock.LOCK_KEY.format(user_id), -1)
        assert WalletLock(ark_id).IsLocked() is True
        with WalletLock(ark_id) as success:
            assert success is False
            assert WalletLock(ark_id).IsLocked() is True
        assert WalletLock(ark_id).IsLocked() is True

        redisCli.delete(WalletLock.LOCK_KEY.format(user_id))
        redisCli.set(WalletLock.LOCK_KEY.format(user_id), socket.gethostname() + "." + str(os.getpid()))
        assert WalletLock(ark_id).IsLocked() is True
        with WalletLock(ark_id) as success:
            assert success is True
            assert WalletLock(ark_id).IsLocked() is True
        assert WalletLock(ark_id).IsLocked() is True

        redisCli.delete(WalletLock.LOCK_KEY.format(user_id))

        assert WalletLock(ark_id).IsLocked() is False
        print("WalletLock Test OK")


if __name__ == "__main__":
    t = Tester()
    t.testWalletLock()