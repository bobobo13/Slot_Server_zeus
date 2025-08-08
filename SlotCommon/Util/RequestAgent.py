import logging
import time
from SlotCommon.Util.RoutineProc import RoutineProc
import requests
import traceback
from requests.adapters import HTTPAdapter

class RequestAgent(object):
    REGENERATE_SESSION_INTERVAL = 500   # max=610
    """
    A class to handle interactions with the GameInfoServer API.
    """

    def __init__(self, logger, server_url):
        """
        Initialize the GameInfoServerAgent with the server URL.

        :param server_url: The URL of the GameInfoServer.
        """
        self.logger = logger
        self.server_url = server_url
        RoutineProc("RequestAgentRegenerateSession", RequestAgent.REGENERATE_SESSION_INTERVAL, func=self.RegenerateSession, logger=self.logger)
        self._ApiSession = self.create_api_session()

    def create_api_session(self):
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=200, pool_block=True)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def RegenerateSession(self, bForce=True):
        self._ApiSession = self.create_api_session()

    def Get(self, url, *, callback=None, **kwargs):
        """
        Perform a GET request to the specified URL with optional parameters.

        :param url: The URL to send the GET request to.
        :param callback: Optional callback function to process the response.
        :return: The response from the server.
        """
        session = self._ApiSession
        resp = None
        try:
            resp = session.get(url, **kwargs)
            self.logger.log(logging.INFO, f"[{self.__class__.__name__}] GET request to {url}, response: {resp.text}")
            # resp.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"[{self.__class__.__name__}] Request failed: {traceback.format_exc()}")
            return None
        if resp is None:
            self.logger.error(f"[{self.__class__.__name__}] No response received.")
            return None
        if resp.status_code != 200:
            self.logger.error(f"[{self.__class__.__name__}] GET request failed: {resp.status_code}, response: {resp.text}")
            return None
        if callback:
            try:
                return callback(resp.text)
            except Exception as e:
                self.logger.error(f"[{self.__class__.__name__}] Callback failed: {traceback.format_exc()}")
                return None
        return resp.text

    def Post(self, url, *, callback=None, **kwargs):
        """
        Perform a POST request to the specified URL with optional data.
        :param url: The URL to send the POST request to.
        :param callback: Optional callback function to process the response.
        :return: The response from the server.
        """
        session = self._ApiSession
        try:
            resp = session.post(url, **kwargs)
            self.logger.log(logging.INFO, f"[{self.__class__.__name__}] POST request to {url} , response: {resp.text}")
            resp.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"[{self.__class__.__name__}] Request failed: {traceback.format_exc()}")
            return None
        if callback:
            try:
                return callback(resp.text)
            except Exception as e:
                self.logger.error(f"[{self.__class__.__name__}] Callback failed: {traceback.format_exc()}")
                return None
        return resp.text

