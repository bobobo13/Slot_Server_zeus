# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class DataLoggerInterface:
    __metaclass__ = ABCMeta

    def setup(self, name, config):
        pass

    @abstractmethod
    def send(self, type_name, log_data, timestamp, *args, **kwargs):
        pass

    def close(self):
        pass
