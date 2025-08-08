#!/usr/bin/python
# -*- coding: utf-8 -*-

import falcon
import json
import requests
import traceback
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from ..Network.Web import Controller
from ..Module.FunctionSwitch import FunctionSwitch
from .SlotManager import SlotManager

class SlotApi(Controller):
    def __init__(self, name, webServer, **kwargs):
        Controller.__init__(self, name, webServer)
        self.DataSource = kwargs.get('DataSource', None)
        self.logger = webServer.log_manager.getLogger(name)
        self.strName = name
        self.server = webServer
        self.function_switch = FunctionSwitch(self.logger, self.DataSource, bInitDb=True)
        kwargs["CallSlotMachineFunc"] = self._CallSlotMachine
        self._Core = SlotManager(self.logger, **kwargs)

        self.addRoute('start_game', self.on_start_game)
        self.addRoute('spin', self.on_spin)
        self.addRoute('next_fever', self.on_next_fever)
        self.addRoute('get_log_game_result', self.get_log_game_result)
        self.addRoute('recover_game_state', self.recover_game_state)
        self._SlotMachineSession = dict()

    def on_start_game(self, req, resp, kwargs):
        self._InitResponse(resp)
        self.logger.info("[SlotService][{}] req:{}, resp:{}".format("on_start_game", req, resp))
        if not self._IsPost('start_game', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, platform_data, gn_data, game_data = self._get_data(data)
        gn_function_switch = gn_data.get('gn_function_switch', "")
        # platform_fs_data = platform_data.get('fs_data', {})
        fs_setting = self.function_switch.get_fs_setting(game_name, platform_data, gn_function_switch)
        r = self._Core.start_game(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def on_spin(self, req, resp, kwargs):
        self._InitResponse(resp)
        self.logger.info("[SlotService][{}] req:{}, resp:{}".format("on_spin", req, resp))
        if not self._IsPost('spin', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, platform_data, gn_data, game_data = self._get_data(data)
        gn_function_switch = gn_data.get('gn_function_switch', "")
        platform_fs_data = platform_data.get('fs_data', {})
        fs_setting = self.function_switch.get_fs_setting(game_name, platform_fs_data, gn_function_switch)
        r = self._Core.spin(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def on_next_fever(self, req, resp, kwargs):
        self._InitResponse(resp)
        self.logger.info("[SlotService][{}] req:{}, resp:{}".format("on_next_fever", req, resp))
        if not self._IsPost('next_fever', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, platform_data, gn_data, game_data = self._get_data(data)
        gn_function_switch = gn_data.get('gn_function_switch', "")
        platform_fs_data = platform_data.get('fs_data', {})
        fs_setting = self.function_switch.get_fs_setting(game_name, platform_fs_data, gn_function_switch)
        r = self._Core.next_fever(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def get_log_game_result(self, req, resp, kwargs):
        self._InitResponse(resp)
        self.logger.info("[SlotService][{}] req:{}, resp:{}".format("get_log_game_result", req, resp))
        if not self._IsPost('get_log_game_result', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, platform_data, gn_data, game_data = self._get_data(data)
        gn_function_switch = gn_data.get('gn_function_switch', "")
        platform_fs_data = platform_data.get('fs_data', {})
        fs_setting = self.function_switch.get_fs_setting(game_name, platform_fs_data, gn_function_switch)

        game_no = game_data.get('GameNo')
        game_sn = game_data.get('GameSn')
        r = self._Core.get_game_result(game_no, game_sn)
        self._InitResponse(resp, Body=r)

    def recover_game_state(self, req, resp, kwargs):
        self._InitResponse(resp)
        self.logger.info("[SlotService][{}] req:{}, resp:{}".format("recover_game_state", req, resp))
        if not self._IsPost('recover_game_state', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, platform_data, gn_data, game_data = self._get_data(data)
        gn_function_switch = gn_data.get('gn_function_switch', "")
        platform_fs_data = platform_data.get('fs_data', {})
        fs_setting = self.function_switch.get_fs_setting(game_name, platform_fs_data, gn_function_switch)

        game_sn = game_data.get('GameSn')
        r = self._Core.recover_game_state(ark_id, game_name, game_sn)
        self._InitResponse(resp, Body=r)

    def _get_data(self, data):
        ark_id = data.get('ark_id')
        game_name = data.get('game_name')
        platform_data = data.get('platform_data', {})
        gn_data = data.get('gn_data', {})
        game_data = data.get('game_data', {})
        return ark_id, game_name, platform_data, gn_data, game_data

    def _GetParams(self, req):
        # if (req.method != 'POST') or (len(req.params) > 0): # 一般 form的格式
        if req.method != 'POST':
            return req.params
        r = {}
        try:
            r = json.loads(req.stream.read())
        except:
            pass
        return r

    def _IsPost(self, strName, req, resp):
        if req.method == 'POST':
            return True
        resp.body = strName + ':hello world!'
        return False

    def _InitResponse(self, resp, status=falcon.HTTP_OK, Body=None):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.set_header('Content-Type', 'application/json')
        resp.status = status
        if Body is not None:
            # resp.body = json.dumps(Body, encoding='utf-8')
            resp.text = json.dumps(Body).encode('utf8')


    def _CallSlotMachine(self, url, ark_id, game_name, fs_setting, gn_data, platform_data, game_data, **kwargs):
        # url = "http://" + self._SlotMachineUrlMap[game_name] + cmd
        data = {}
        data["ark_id"] = ark_id
        data["game_name"] = game_name
        data["fs_setting"] = fs_setting
        data["gn_data"] = gn_data
        data["platform_data"] = platform_data
        data["game_data"] = game_data
        data.update(kwargs)

        resp, elapsed = None, None
        if game_name not in self._SlotMachineSession:
            sission = requests.Session()
            retry = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
            adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry, pool_block=True)
            sission.mount('http://', adapter)
            sission.mount('https://', adapter)
            self._SlotMachineSession[game_name] = sission
        try:
            r = self._SlotMachineSession[game_name].post(url, json=data)
            elapsed = r.elapsed.microseconds / 1000
            self.logger.info("[SlotService] url:{}, reqData:{}, resp:{}, elapsed:{}".format(url, data, resp, elapsed))
            resp = r.json()
        except:
            self.logger.error("[SlotService] url:{}, reqData:{}, e:{}".format(url, data, traceback.format_exc()))
        return resp


