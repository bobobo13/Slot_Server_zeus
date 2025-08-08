# -*- coding: utf-8 -*-
__author__ = 'NUiLL, duyhsieh'
import sys, traceback
import pymongo
import configparser as ConfigParser
import copy
try:
    import urllib.parse
except:
    from future.standard_library import install_aliases
    install_aliases()
    import urllib.parse

############## 2022/3/5 comment by Duy ##############
############## tested successfully under pymongo 2.8, 2.9 and 3.12.3 ##############
############## for pymongo 2.8 maxPoolSize is not supported with URI, so consodier use MongoClient instead ##############

class DbConnector(object):
    @classmethod
    def GetVersion(cls):
        return DbConnector.VERSION

    VERSION=None
    def __init__(self, 
                 strHost='localhost',
                 strPort='27017',
                 strUser='',
                 strPassword='',
                 nPoolSize=100,
                 nSocketTimeout=1000,
                 nConnectTimeout=2000,
                 nWaitQueueTimeout=4000,
                 nWaitQueueMultiple=4,
                 bRetryWrites=None,
                 extraConnOptions=None,
                 bEnableSsl=False):

        # if DbConnector.VERSION == None:
        #     ver = int(pymongo.get_version_string()[:3].replace('.', ''))
        #     if ver >= 40:
        #         raise Exception('[DbConnector] unsupported pymongo version!{}'.format(pymongo.get_version_string()))
        #     DbConnector.VERSION = ver
            #print("DbConnector version:{}".format(DbConnector.GetVersion()))

        self.Conn = None
        self.hostList= str(strHost).split(',')
        self.portList = str(strPort).split(',')
        if len(self.portList) == 1:         # support set the same port for all VMs in a replica set or sharding cluster
            self.portList = self.portList * len(self.hostList)
        elif len(self.hostList) != len(self.portList):
            raise Exception('Invalid Host Parameters!{}...{}'.format(strHost, strPort))
        self.strUser = strUser
        self.strPassword = strPassword
        self.connOptions = {
            'socketTimeoutMS':nSocketTimeout,
            'connectTimeoutMS':nConnectTimeout,
            'waitQueueTimeoutMS':nWaitQueueTimeout, 
            'waitQueueMultiple':nWaitQueueMultiple,
            'ssl': bEnableSsl,
            'maxPoolSize': nPoolSize
        }

        #  self.connOptions['max_pool_size'] = nPoolSize # not supported when using URI; only MongoClient class supports it.
        # self.connOptions['maxPoolSize'] = nPoolSize ### when using mongo uri, it is maxPoolSize; when using MongoClient, it is max_pool_size
        
        if bRetryWrites is not None:
            self.connOptions['retryWrites'] = 'true' if bRetryWrites is True else 'false'
        
        if extraConnOptions:
            self.connOptions.update(extraConnOptions)
        
        self.Open()

    def Open(self):
        conn = self.__ConnectUri(self.hostList, self.portList, self.strUser, self.strPassword, **self.connOptions)
        self.Conn = conn
        return self.Conn

    def getConn(self):
        return self.Conn

    def getDb(self, db_name):
        if self.Conn:
            return self.Conn[db_name]
        return None

    def getCol(self, db_name, col_name):
        if self.Conn:
            return self.Conn[db_name][col_name]
        return None

    def OpenWith(self, strConfigPath, strConfigSection):
        nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigPath, strConfigSection)
        return self.Open(nam, host, port, user, pwd, ps)

    def __ConnectUri(self, 
                   hostList, 
                   portList, 
                   strUser='', 
                   strPassword='', 
                   **options):
        
        if (len(hostList) <= 0) or (len(portList) <= 0) or (len(hostList)!=len(portList)):
            return None

        uri = self.__MakeMongoUri(hostList, portList, options, strUser, strPassword)
        mc = pymongo.MongoClient(uri)
        return mc

    def __MakeMongoUri(self, hosts, ports, options, strUser='', strPassword=''):
        host_uri=','.join(['{}:{}'.format(host,port) for host,port in zip(hosts, ports)])
        tempOptions = copy.deepcopy(options)
        if len(strUser) > 0:
            encoded_pwd = urllib.parse.quote(strPassword)
            tempOptions['authSource'] = 'admin'
            s = 'mongodb://{}:{}@{}'.format(strUser, strPassword, host_uri)
        else:
            s = 'mongodb://{}'.format(host_uri)
        for k, v in tempOptions.items():
            if type(v) == bool:
                tempOptions[k] = str(v).lower()
        option_uri = urllib.parse.urlencode(tempOptions)
        s = s + '/?' + '{}'.format(option_uri)
        return s

    @staticmethod
    def GetDbConfig(strConfigPath, strConfigSection):
        if (type(strConfigPath) not in [str, bytes]) or (len(strConfigPath) <= 0):
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
    def ConnectWith(strConfigPath, strConfigSection):
        nam, host, port, user, pwd, ps = DbConnector.GetDbInfo(strConfigPath, strConfigSection)
        return DbConnector(str(host), str(port), user, pwd, ps).getDb(nam)

