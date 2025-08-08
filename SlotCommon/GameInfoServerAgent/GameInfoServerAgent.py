from SlotCommon.Util.RequestAgent import RequestAgent
import json

"""
This module is responsible for interacting with the GameInfoServer API.

"""


class GameInfoServerAgent(RequestAgent):
    def __init__(self, logger, server_url, GameName):
        """
        Initialize the GameInfoServerAgent with the server URL and game name.

        :param server_url: The URL of the GameInfoServer.
        :param GameName: The name of the game.
        """
        super(GameInfoServerAgent, self).__init__(logger, server_url)
        self.GameName = GameName


    def GetGameInfo(self, ChanceKey):
        url = f"{self.server_url}/getGameInfo?GameName={self.GameName}&ChanceKey={ChanceKey}"
        return self.Get(url, callback=json.loads)





