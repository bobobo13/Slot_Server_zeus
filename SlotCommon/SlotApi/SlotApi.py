#!/usr/bin/python
# -*- coding: utf-8 -*-

import falcon
import json
from SlotServer.Network.Web import Controller
import importlib
# from .SlotMachine import SlotMachine

class SlotApi(Controller):
    def __init__(self, name, webServer, CodeName, **kwargs):
        Controller.__init__(self, name, webServer)
        self.DataSource = kwargs.get('DataSource', None)
        self.logger = webServer.log_manager.getLogger(name)
        self.strName = name
        self.server = webServer
        SlotMachineModule = importlib.import_module(f"{CodeName}.Game.Slot.{CodeName}SlotMachine", package=CodeName)
        SlotMachineClass = SlotMachineModule.__getattribute__(f"{CodeName}SlotMachine")
        self._Core = SlotMachineClass(self.logger, CodeName=CodeName, **kwargs)

        self.addRoute('start_game', self.on_start_game)
        self.addRoute('spin', self.on_spin)
        self.addRoute('next_fever', self.on_next_fever)
        self.logger.info("[SlotApi][__init__] strName:{}, webServer:{}".format(name, webServer))

    def on_start_game(self, req, resp, kwargs):
        self._InitResponse(resp)
        if not self._IsPost('start_game', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, fs_setting, platform_data, gn_data, game_data, client_action = self._get_data(data)
        r = self._Core.start_game(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def on_spin(self, req, resp, kwargs):
        self._InitResponse(resp)
        if not self._IsPost('spin', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, fs_setting, platform_data, gn_data, game_data, client_action = self._get_data(data)
        r = self._Core.spin(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def on_next_fever(self, req, resp, kwargs):
        self._InitResponse(resp)
        if not self._IsPost('next_fever', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, fs_setting, platform_data, gn_data, game_data, client_action = self._get_data(data)
        r = self._Core.next_fever(ark_id, game_name, fs_setting, gn_data, platform_data, game_data, client_action=client_action)
        self._InitResponse(resp, Body=r)

    def _get_data(self, data):
        ark_id = data.get('ark_id')
        game_name = data.get('game_name')
        fs_setting = data.get('fs_setting', {})
        platform_data = data.get('platform_data', {})
        gn_data = data.get('gn_data', {})
        game_data = data.get('game_data', {})
        client_action = game_data.get('client_action_data', {})
        self.logger.info("[SlotApi Init][{}] ark_id:{}, game_data:{}, client_action:{}".format("get_data:", ark_id, game_data, client_action))
        return ark_id, game_name, fs_setting, platform_data, gn_data, game_data, client_action

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



