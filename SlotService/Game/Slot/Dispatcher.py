#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import importlib

class Dispatcher(object):
    """
    +----------------------+
    |                      |
    |                      |
    |  * START_GAME        |
    |  * SPIN              |     +-----------------+      +------------+
    |  * ENTER_BONUS_GAME  |  +--| Game1Calculator |--+-->| Calculator |
    |  * ENTER_NEXT_LEVEL  |  |  +-----------------+  |   +------------+
    |                      |  |                       |
    |  +--------------+    |  |  +-----------------+  |
    |  |  Dispatcher  |----+--+--| Game2Calculator |--+
    |  +--------------+    |  |  +-----------------+  |
    +----------------------+  |                       |
                              |  +-----------------+  |
                              +--| Game3Calculator |--+
                              |  +-----------------+  |
                              |                       |
                              +-- ......more....... --+
    """

    def __init__(self, logger, strCodeName=None, **kwargs):
        self.codeName = strCodeName
        self.logger = logger
        self._slot_machine = {}
        self._Delegate = kwargs
        self._load_module(**kwargs)

    def _load_module(self, **kwargs):
        if sys.platform == "win32":
            folder_path = ""
            dirsplit = "\\"
        else:
            folder_path = "/"
            dirsplit = "/"
        folder_path += dirsplit.join((os.path.abspath(__file__).split(dirsplit)[:-2]))
        game_list = [("LionDanceLegi", "LionDance")]
        for pkg, game_name in game_list:
            slot_sys_cls = importlib.import_module(".Game.Slot.SlotMachine", package=pkg).SlotMachine
            self._slot_machine[game_name] = slot_sys_cls(self.logger, **kwargs)

    def get_slot_machine(self, game_name):
        if game_name not in self._slot_machine:
            return None
        return self._slot_machine[game_name]



