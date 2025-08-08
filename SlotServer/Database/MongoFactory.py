#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'duyhsieh'

import os
import configparser as ConfigParser
import traceback
from .DbConnectorEx import DbConnector

class MongoConfig(object):
    def __init__(self, env, host_cfg_path=None, db_cfg_path=None):
        self.mongo_host = dict()
        self.database_setting = dict()
        self.conn_parameter = {
            'default': {
                'maxPoolSize': 50,
                'socketTimeoutMS': 2000,
                'connectTimeoutMS': 1000,
                'waitQueueTimeoutMS': 4000,  # basically we do not wish any thread ( when thread # > maxPoolSize) to wait for free connections for too long; if too many threads await, add more EC2 instances.
                'waitQueueMultiple': 4  # allow maxPoolSize * waitQueueMultiple threads to wait for free connection;
            },
            'log': {
                'maxPoolSize': 10,
                'socketTimeoutMS': 4000,
                'connectTimeoutMS': 2000,
                'waitQueueTimeoutMS': 4000,
                'waitQueueMultiple': 4
            },
        }
        self.mongo_client_set = {
            # each db has two MongoClients, with connection parameter default and log
            'default': {
                'mongoclient1': 'default',
                'mongoclient2': 'log',
            },
        }
        if host_cfg_path is not None and db_cfg_path is not None:
            self._add_host_by_config(host_cfg_path)
            self._bind_database_address_by_config(db_cfg_path)
            return


    def get_backend_database_list(self):
        ret = []
        for i in self.database_setting:
            if i.startswith('BackendLog'):
                ret.append(i)
        ret = sorted(ret)  # 保證讓BackendLog1排前面
        return ret

    def get_mongo_client_set(self):
        return self.mongo_client_set

    def get_mongo_host_list(self):
        return self.mongo_host

    def get_database_list(self):
        return self.database_setting

    def get_host_address(self, host):
        return self.mongo_host.get(host)

    def get_database_host_address(self, db_name):
        host_str = self.database_setting[db_name]['host']
        return self.mongo_host[host_str]

    def get_host_conn_param(self, host, mc_key):
        return self.conn_parameter[self.mongo_client_set[host][mc_key]]

    def __add_host(self, host, host_address):
        if host in self.mongo_host:
            raise Exception('[MongoConfig] host already exist!host=%s' % (host))
        self.mongo_host[host] = host_address

    def __bind_database_address(self, db_name, host, mc_key):
        if host not in self.mongo_host:
            raise Exception('[MongoConfig] host does not exist!host=%s' % (host))

        if db_name not in self.database_setting:
            self.database_setting[db_name] = {'host': host, 'mc_key': mc_key}
        else:
            raise Exception('[MongoConfig] db name already used!db name=%s' % (db_name))

    def _add_host_by_config(self, cfg_path):
        if not os.path.exists(cfg_path):
            raise Exception('[MongoConfig] Error! host config file does not exist!,  path=[%s]' % cfg_path)
        configParser = ConfigParser.RawConfigParser()
        configParser.read(cfg_path)
        for s in configParser.sections():
            if s == "DEFAULT":
                continue
            hostId = s
            ip = self.get_host_config_value(configParser, s, 'MongoHost', 'localhost')
            port = self.get_host_config_value(configParser, s, 'MongoPort', 27017)
            user = self.get_host_config_value(configParser, s, 'MongoUser')
            pwd = self.get_host_config_value(configParser, s, 'MongoPwd')
            auth = None
            ssl = self.get_host_config_bool(configParser, s, 'EnableSSL')
            if user is not None:
                auth = [user, pwd]
            host_addr = {'ip': ip, 'port': int(port), 'auth': auth}
            if ssl is not None:
                host_addr.update({'ssl': ssl})
            self.__add_host(hostId, host_addr)
            if hostId not in self.mongo_client_set:
                self.mongo_client_set[hostId] = self.mongo_client_set['default']

    def _bind_database_address_by_config(self, cfg_path):
        if not os.path.exists(cfg_path):
            raise Exception('[MongoConfig] Error! db config file does not exist!,  path=[%s]' % cfg_path)
        configParser = ConfigParser.RawConfigParser()
        configParser.read(cfg_path)
        for s in configParser.sections():
            dbName = configParser.get(s, 'DbName')
            hostId = configParser.get(s, 'HostId')
            self.__bind_database_address(dbName, hostId, "mongoclient1")

    def get_host_config_value(self, configParser, section, option, default=None):
        value = default
        if configParser.has_option(section, option):
            value = configParser.get(section, option)
        return value

    def get_host_config_bool(self, configParser, section, option, default=None):
        value = default
        if configParser.has_option(section, option):
            value = configParser.getboolean(section, option)
        return value

class MongoFactory(object):
    def __init__(self, env, host_cfg_path=None, db_cfg_path=None):
        self.__mongo_client_instance = {}
        self.__mc_instance_map = {}
        self.__wdb_instance_map = {}
        self.__db_config = MongoConfig(env, host_cfg_path, db_cfg_path)

        mc_set = self.__db_config.get_mongo_client_set()
        for host, mc_data in mc_set.items():
            self.__mongo_client_instance[host] = {}
            info = self.__db_config.get_host_address(host)
            if info is None:
                continue
            # print 'host address=', info
            for mc_key in mc_data:
                conn_param = self.__db_config.get_host_conn_param(host, mc_key)
                try:
                    usr = '' if info['auth'] is None else info['auth'][0]
                    pwd = '' if info['auth'] is None else info['auth'][1]
                    conn = DbConnector(info['ip'], 
                                       info['port'],
                                       usr,
                                       pwd,
                                       conn_param.get('maxPoolSize', conn_param.get("max_pool_size")),
                                       conn_param['socketTimeoutMS'], 
                                       conn_param['connectTimeoutMS'], 
                                       conn_param['waitQueueTimeoutMS'], 
                                       conn_param['waitQueueMultiple'], 
                                       True,
                                       bEnableSsl=info.get("ssl", False))
                    
                    self.__mongo_client_instance[host][mc_key] = conn.getConn()

                except:
                    s = '[MongoService] init error! DB address={},callstack={}'.format(info, traceback.format_exc())
                    raise Exception(s)  # stops launching

        for db, host_and_mc_key in self.__db_config.get_database_list().items():
            if db in self.__mc_instance_map:
                raise Exception('[MongoService] DB name already exists!{}'.format(db))

            host = host_and_mc_key['host']
            mc_key = host_and_mc_key['mc_key']
            # print db, host, mc_key
            self.__mc_instance_map[db] = self.__mongo_client_instance[host][mc_key]
            # self.__wdb_instance_map[db] = PyMongoDBWrapper(self.__mongo_client_instance[host][mc_key][db])
            self.__wdb_instance_map[db] = self.__mongo_client_instance[host][mc_key][db]
            # print 'binding db:{} to :{}'.format(db,id(self.__mongo_client_instance[host][mc_key]))

    # interface to get DB , so you don't have to care about whether you have only 1 host(especially local VM ),
    # or N hosts (test / prod) environment.
    # most importantly, this is designed so that user does not have to specify real machine ip addresses
    # Restriction: you cannot have same database names , even they are on different host IPs
    def get_database(self, database_name):
        if database_name not in self.__mc_instance_map:
            raise Exception('[MongoService] Error! database [%s] does not exist!' % database_name)
        return self.__mc_instance_map[database_name][database_name]

    def get_wrap_database(self, database_name):
        if database_name not in self.__wdb_instance_map:
            raise Exception('[MongoService] Error! wrap database [%s] does not exist!' % database_name)
        return self.__wdb_instance_map[database_name]

    def get_backend_database_list(self, wrap=False):
        lst = []
        for i in self.get_backend_database_name_list():
            if wrap:
                lst.append(self.get_wrap_database(i))
            else:
                lst.append(self.get_database(i))
        return lst

    def get_backend_database_name_list(self):
        return self.__db_config.get_backend_database_list()

