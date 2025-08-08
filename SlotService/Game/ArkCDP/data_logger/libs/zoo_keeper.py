# coding=utf-8
import copy
import json
import six
if six.PY3:
    import configparser as ConfigParser
else:
    import ConfigParser
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError


class ZooKeeper:
    def __init__(self, _hosts, _timeout=5.0):
        self.zk = KazooClient(
            hosts=_hosts,
            timeout=_timeout
        )

    @staticmethod
    def get_bootstrap_servers(cfg_path, section, _timeout=5):
        config = ConfigParser.RawConfigParser()
        config.read(cfg_path)
        bootstrap_servers = config.get(section, 'bootstrap_servers')
        is_internal = config.getboolean(section, 'isInternal')
        if config.has_option(section, 'zookeeper_hosts'):
            zookeeper_hosts = config.get(section, 'zookeeper_hosts')
            zookeeper_client = ZooKeeper(zookeeper_hosts)
            kafka_hosts = zookeeper_client.get_kafka_hosts_str(timeout=_timeout, is_internal=is_internal)
            if len(kafka_hosts) > 0:
                bootstrap_servers = kafka_hosts
        return bootstrap_servers

    def get_kafka_hosts_str(self, path='/brokers/ids', timeout=5, is_internal=False):
        hosts_info = self.get_kafka_hosts(path, timeout)
        hosts_ip_port = []
        if is_internal:
            key = 'INTERNAL://'
        else:
            key = 'EXTERNAL://'
        for i in hosts_info:
            endpoints = i.get('endpoints')
            for j in endpoints:
                if key in j:
                    s = j.replace(key, '')
                    hosts_ip_port.append(s)
        return ','.join(hosts_ip_port)

    def get_kafka_hosts(self, path='/brokers/ids', timeout=5):
        host_list = []
        try:
            self.zk.start(timeout)
            self._hosts_recursive_to_list(path, host_list)
            self.zk.stop()
            self.zk.close()
        except KazooTimeoutError as e:
            print(e)
        except Exception as e:
            print(e)
        return host_list

    def _hosts_recursive_to_list(self, path, host_list):
        data, stat = self.zk.get(path)
        children = self.zk.get_children(path)
        if len(children) > 0:
            for sub in children:
                if path != '/':
                    sub_node = path + '/' + sub
                else:
                    sub_node = '/' + sub
                self._hosts_recursive_to_list(sub_node, host_list)
        else:
            host_list.append(json.loads(data))
