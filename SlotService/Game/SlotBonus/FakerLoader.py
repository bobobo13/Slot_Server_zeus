#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import os
from system.loader.module_manager import ModuleManager
from system.loader.module_list import module_list_data
from system.lib.env import Env

class FakerLoader:
    module_list_data = module_list_data
    def __init__(self, os_path, ConfigPath, module_list):
        self.loader = ModuleManager(False)
        module_list_data = [i for i in FakerLoader.module_list_data if i[0] in module_list]
        # module_list_data=module_list_data
        for i in module_list_data:
            self.loader.create_class_instance(i[0], i[1], i[2], i[3])

        Env.init('local')
        env = Env()
        self.loader.class_list['env'] = env
        self.loader.order_class_list.append(env)
        self.loader.class_list['config_manager'].set_config_path(ConfigPath)
        self.loader.init_all_instance()

    def get_loader(self):
        return self.loader

