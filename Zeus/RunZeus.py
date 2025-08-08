
import os
from gevent import monkey
monkey.patch_all()

from SlotServer.Network import Engine
from Zeus.Game.Zeus import GameService


codename = "Zeus"
globalCodeName = os.environ.get("GLOBAL_CODE_NAME")
env = os.environ.get("PROJ_ENV")
channel = os.environ.get("PROJ_CHANNEL")

gameService = GameService(globalCodeName, env=env, role="Game", channel=channel)

Engine.instance().start(gameService)
httpApp = gameService.app

if __name__ == '__main__':
    Engine.instance().startHTTPWSGI(httpApp, host='0.0.0.0', port=8084)
    Engine.instance().run()