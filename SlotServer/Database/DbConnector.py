# -*- coding: utf-8 -*-
__author__ = 'NUiLL'

import sys, traceback
import pymongo
import ConfigParser

class DbConnector:
    def __init__(self, strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', nPoolSize=100):
        self.DbName = strDbName
        self.Host = strHost
        self.Port = nPort
        self.DataSource = None
        if len(strDbName) > 0:
            self.Open(strDbName, strHost, nPort, strUser, strPassword, nPoolSize)

    def Open(self, strDbName, strHost='localhost', nPort=27017, strUser='', strPassword='', nPoolSize=100):
        if len(strDbName) > 0:
            self.DbName = strDbName
        if len(strHost) > 0:
            self.Host = strHost
        if nPort > 0:
            self.Port = nPort

        ds = DbConnector.Connect(self.DbName, self.Host, self.Port, strUser, strPassword, nPoolSize)
        self.DataSource = ds
        return ds

    def OpenWith(self, strConfigPath, strConfigSection):
        nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigPath, strConfigSection)
        return self.Open(nam, host, port, user, pwd, ps)

    @staticmethod
    def GetDbConfig(strConfigPath, strConfigSection):
        if (type(strConfigPath) not in [str,unicode]) or (len(strConfigPath) <= 0):
            return None

        DbConf = ConfigParser.RawConfigParser()
        DbConf.read(strConfigPath)
        return DbConf if DbConf.has_section(strConfigSection) else None

    @staticmethod
    def GetDbInfo(strConfigPath, strConfigSection, ConfParser=None):
        DbConf = ConfParser
        if DbConf is None:
            DbConf = ConfigParser.RawConfigParser()
            DbConf.read(strConfigPath)

        strDbName = ''
        if DbConf.has_option(strConfigSection,'DatabaseName'):
            strDbName = DbConf.get(strConfigSection,'DatabaseName')
        strHost = 'localhost'
        if DbConf.has_option(strConfigSection,'MongoHost'):
            strHost = DbConf.get(strConfigSection,'MongoHost')
        nPort = 27017
        if DbConf.has_option(strConfigSection,'MongoPort'):
            nPort = DbConf.getint(strConfigSection,'MongoPort')

        strUser = ''
        if DbConf.has_option(strConfigSection,'MongoUser'):
            strUser = DbConf.get(strConfigSection,'MongoUser')
        strPassword = ''
        if DbConf.has_option(strConfigSection,'MongoPwd'):
            strPassword = DbConf.get(strConfigSection,'MongoPwd')
        nPoolSize = 100
        if DbConf.has_option(strConfigSection,'MongoPoolSize'):
            nPoolSize = DbConf.getint(strConfigSection,'MongoPoolSize')

        return strDbName, strHost, nPort, strUser, strPassword, nPoolSize

    @staticmethod
    def Connect(strDbName, strHost='localhost', nPort=27017, strUser='', strPassword='', nPoolSize=100):
        if (len(strDbName) <= 0) or (len(strHost) <= 0) or (nPort <= 0):
            return None

        nPoolSize = nPoolSize if nPoolSize > 0 else 100
        mc = None
        if pymongo.get_version_string().startswith("2."):
            mc = pymongo.MongoClient(host=strHost, port=nPort, max_pool_size=nPoolSize, _connect=False)
        else:
            mc = pymongo.MongoClient(host=strHost, port=nPort, maxPoolSize=nPoolSize, connect=False)
        if mc is None:
            return None

        if len(strUser) > 0:
            mc[strDbName].authenticate(strUser, strPassword, source='admin')
        return mc[strDbName]
