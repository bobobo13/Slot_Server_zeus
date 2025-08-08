#!/usr/bin/python
# -*- coding: utf-8 -*-

# Importing the necessary modules
from gevent import monkey
monkey.patch_all()

import os
from SlotServer.Network import Engine
from SlotServer.Network.BaseServer import BaseServer

from .Slot.SlotApi import SlotApi
from .Slot.FakeMongo import FakeMongoDB

class GameService(BaseServer):
    def __init__(self, code_name='Zeus', env='local', role='Game', channel=None):
        version = env if channel is None else (env + '-' + channel)
        version = version if version is not None else 'local'
        super(GameService, self).__init__(code_name, version, global_code_name=None)
        self.addController(SlotApi("SlotMachine", self, DataSource=FakeMongoDB()))


if __name__ == '__main__':
    globalCodeName = os.environ.get("GLOBAL_CODE_NAME")
    env = os.environ.get("PROJ_ENV")
    channel = os.environ.get("PROJ_CHANNEL")
    role = "Game"

    gameService = GameService(globalCodeName, env=env, role=role, channel=channel)

    Engine.instance().start(gameService)
    Engine.instance().startHTTPWSGI(gameService.app, host='0.0.0.0', port=5288)
    Engine.instance().run()


