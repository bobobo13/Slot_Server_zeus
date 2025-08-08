import json
import six
if six.PY3:
    import configparser as ConfigParser
else:
    import ConfigParser

from kafka.errors import NoBrokersAvailable

from .libs.mongo_dom import MongoDOM
from .modules.kafka_logger import KafkaLogger
from .modules.mongo_backend_logger import MongoBackendLogger
from .modules.mongo_logger import MongoLogger


def get_ark_data_logger(cdp_cfg_path, kafka_cfg_section,
                        error_logger_cfg_section, env, code_name, sys_logger=None):
    producer, topic, error_logger = get_producer_n_topic_from_json_config(
        cdp_cfg_path, kafka_cfg_section, error_logger_cfg_section, sys_logger)
    topic_prefix = str.format('{}_{}', env, code_name)
    return KafkaLogger(producer, topic, None, error_logger, topic_prefix=topic_prefix)


# MongoLogger
def get_mongo_db_by_config(config_path, section):
    mongo_config = ConfigParser.RawConfigParser()
    mongo_config.read(config_path)
    _, db = MongoDOM.get_mongo_db(mongo_config, section)
    logger_name = mongo_config.get(section, "LoggerName")
    coll_name = mongo_config.get(section, "CollName")
    expire = None
    if mongo_config.has_option(section, "Expire"):
        expire = mongo_config.get(section, "Expire")
    return db, logger_name, coll_name, expire


def get_mongo_error_logger(config_path, section, sys_logger):
    db, logger_name, coll_name, expire = get_mongo_db_by_config(config_path, section)
    mongo_logger = MongoLogger(logger_name, db, coll_name, expire=expire, sys_logger=sys_logger)
    return mongo_logger


def get_mongo_backend_logger(config_path, section, sys_logger, error_log_section=None, **kwargs):
    gmt_time_zone = kwargs.get('gmt_time_zone', None)
    if error_log_section is not None:
        mongo_error_logger = get_mongo_error_logger(config_path, error_log_section, sys_logger)
    else:
        mongo_error_logger = None
    db, logger_name, coll_name, expire = get_mongo_db_by_config(config_path, section)
    mongo_logger = MongoBackendLogger(logger_name, db, sys_logger=sys_logger, gmt_time_zone=gmt_time_zone,
                                      error_logger=mongo_error_logger)
    return mongo_logger


def get_producer_n_topic_from_json_config(cdp_cfg_path, kafka_section,
                                          error_log_section,
                                          sys_logger=None):
    cdp_cfg = ConfigParser.RawConfigParser()
    cdp_cfg.read(cdp_cfg_path)
    topic = cdp_cfg.get(kafka_section, 'topic')
    enable = cdp_cfg.getboolean(kafka_section, 'enable')
    if enable:
        from .libs.kafka_producer import KafkaProducer
        from .libs.zoo_keeper import ZooKeeper
        producer_connect_info = {'bootstrap_servers': ZooKeeper.get_bootstrap_servers(cdp_cfg_path, kafka_section)}
        if cdp_cfg.has_option(kafka_section, 'producer_config'):
            producer_config = json.loads(cdp_cfg.get(kafka_section, 'producer_config'))
            producer_connect_info.update(producer_config)

        admin_client_connect_info = {'bootstrap_servers': ZooKeeper.get_bootstrap_servers(cdp_cfg_path, kafka_section)}
        if cdp_cfg.has_option(kafka_section, 'admin_client_config'):
            admin_client_config = json.loads(cdp_cfg.get(kafka_section, 'admin_client_config'))
            admin_client_connect_info.update(admin_client_config)
        try:
            producer = KafkaProducer(producer_connect_info, admin_client_connect_info, sys_logger)
        except NoBrokersAvailable:
            producer = None
        error_logger = get_mongo_error_logger(cdp_cfg_path, error_log_section, sys_logger)
    else:
        producer = None
        error_logger = None

    return producer, topic, error_logger


def get_json_config(path):
    with open(path) as fh:
        json_config = json.load(fh)
    return json_config
