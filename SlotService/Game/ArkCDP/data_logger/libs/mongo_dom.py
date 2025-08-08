# -*- coding: utf-8 -*-

import pymongo
from pytz import timezone


class MongoDOM(object):
    MongoClientDict = {}

    @staticmethod
    def get_mongo_client(host, port):
        link_key = "%s:%s" % (host, port)
        if link_key in MongoDOM.MongoClientDict:
            return MongoDOM.MongoClientDict[link_key]

    @staticmethod
    def set_mongo_client(host, port, mongo_client):
        link_key = "%s:%s" % (host, port)
        MongoDOM.MongoClientDict[link_key] = mongo_client

    @staticmethod
    def get_mongo_db(config_obj, section):
        mongo_client = MongoDOM._mongo_client_config_parse(config_obj, section)
        database_name, mongo_user, mongo_pwd, auth_source = MongoDOM.get_db_auth_info(config_obj, section)
        db_object = MongoDOM.get_db_object(mongo_client, database_name, mongo_user, mongo_pwd, auth_source)
        return mongo_client, db_object

    @staticmethod
    def get_db_object(mongo_client, database_name, mongo_user, mongo_pwd, auth_source):
        db_object = mongo_client[database_name]
        if mongo_user is not None:
            db_object.authenticate(mongo_user, mongo_pwd, source=auth_source)
        return db_object

    @staticmethod
    def get_db_info_by_config(config_obj, section):
        default_database_name, mongo_user, mongo_pwd, auth_source = MongoDOM.get_db_auth_info(config_obj, section)
        gmt_time = MongoDOM._get_gmt_time(config_obj, section)
        return default_database_name, mongo_user, mongo_pwd, gmt_time, auth_source

    @staticmethod
    def _get_db_info_by_config_parser(config_obj, section):
        default_database_name, mongo_user, mongo_pwd, auth_source = MongoDOM.get_db_auth_info(config_obj, section)
        gmt_time = MongoDOM._get_gmt_time(config_obj, section)
        return default_database_name, mongo_user, mongo_pwd, gmt_time

    @staticmethod
    def _get_gmt_time(config_obj, section):
        option = 'GMTTime'
        gmt_time = 8
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            try:
                gmt_time = config_obj.getint(section, option)
            except:
                gmt_time = timezone(config_obj.get(section, option))
        return gmt_time

    @staticmethod
    def get_db_auth_info(config_obj, section):
        option = 'DatabaseName'
        default_database_name = ""
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            default_database_name = config_obj.get(section, option)
        option = 'MongoUser'
        mongo_user = None
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            mongo_user = config_obj.get(section, option)
        if mongo_user is not None:
            if mongo_user == '0' or mongo_user == '':
                mongo_user = None
        option = 'MongoPwd'
        mongo_pwd = None
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            mongo_pwd = config_obj.get(section, option)

        option = 'AuthSource'
        auth_source = 'admin'
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            auth_source = config_obj.get(section, option)

        return default_database_name, mongo_user, mongo_pwd, auth_source

    @staticmethod
    def create_mongo_client_by_config(config_obj, section):
        return MongoDOM._mongo_client_config_parse(config_obj, section)

    @staticmethod
    def _mongo_client_config_parse(config_obj, section):
        option = 'MongoHost'
        default_mongo_host = '127.0.0.1'
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            default_mongo_host = config_obj.get(section, option)
        option = 'MongoPort'
        default_mongo_port = 27017
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            default_mongo_port = config_obj.getint(section, option)
        option = 'MongoPoolSize'
        max_pool_size = 30
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            max_pool_size = config_obj.getint(section, option)
        option = 'MongoSocketTimeoutMS'
        socket_timeout_ms = 5000
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            socket_timeout_ms = config_obj.getint(section, option)
        option = 'MongoConnectTimeoutMS'
        connect_timeout_ms = 1000
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            connect_timeout_ms = config_obj.getint(section, option)
        option = 'MongoWaitQueueTimeoutMS'
        wait_queue_timeout_ms = 2000
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            wait_queue_timeout_ms = config_obj.getint(section, option)
        option = 'MongoWaitMultiple'
        wait_queue_multiple = None
        if config_obj.has_section(section) \
                and config_obj.has_option(section, option):
            wait_queue_multiple = config_obj.getint(section, option)
        mongo_client = MongoDOM.get_mongo_client(default_mongo_host, default_mongo_port)
        if mongo_client is None:
            if pymongo.get_version_string().startswith("2."):
                ver = int(pymongo.get_version_string()[:3].replace('.', ''))
                if ver >= 26:
                    mongo_client = pymongo.MongoClient(host=default_mongo_host,
                                                       port=default_mongo_port,
                                                       max_pool_size=max_pool_size,
                                                       socketTimeoutMS=socket_timeout_ms,
                                                       connectTimeoutMS=connect_timeout_ms,
                                                       waitQueueTimeoutMS=wait_queue_timeout_ms,
                                                       waitQueueMultiple=wait_queue_multiple)
                else:
                    mongo_client = pymongo.MongoClient(host=default_mongo_host,
                                                       port=default_mongo_port,
                                                       max_pool_size=max_pool_size,
                                                       socketTimeoutMS=socket_timeout_ms,
                                                       connectTimeoutMS=connect_timeout_ms)
            else:
                mongo_client = pymongo.MongoClient(host=default_mongo_host,
                                                   port=default_mongo_port,
                                                   maxPoolSize=max_pool_size,
                                                   socketTimeoutMS=socket_timeout_ms,
                                                   connectTimeoutMS=connect_timeout_ms,
                                                   waitQueueTimeoutMS=wait_queue_timeout_ms,
                                                   waitQueueMultiple=wait_queue_multiple)
            MongoDOM.set_mongo_client(default_mongo_host, default_mongo_port, mongo_client)
        return mongo_client
