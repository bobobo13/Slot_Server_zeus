#!/usr/bin/python
# -*- coding: utf-8 -*-

import falcon
import json
from SlotServer.Network.Web import Controller
from .SlotMachine import SlotMachine

class SlotApi(Controller):
    def __init__(self, name, webServer, **kwargs):
        Controller.__init__(self, name, webServer)
        self.DataSource = kwargs.get('DataSource', None)
        self.logger = webServer.log_manager.getLogger(name)
        self.strName = name
        self.server = webServer
        self._Core = SlotMachine(self.logger, **kwargs)

        self.addRoute('start_game', self.on_start_game)
        self.addRoute('spin', self.on_spin)
        self.addRoute('next_fever', self.next_fever)

    def on_start_game(self, req, resp, kwargs):
        self._InitResponse(resp)
        if not self._IsPost('start_game', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, fs_setting, platform_data, gn_data, game_data = self._get_data(data)
        r = self._Core.start_game(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def on_spin(self, req, resp, kwargs):
        self._InitResponse(resp)
        if not self._IsPost('spin', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, fs_setting, platform_data, gn_data, game_data = self._get_data(data)
        r = self._Core.spin(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def next_fever(self, req, resp, kwargs):
        self._InitResponse(resp)
        if not self._IsPost('next_fever', req, resp):
            return
        data = self._GetParams(req)
        ark_id, game_name, fs_setting, platform_data, gn_data, game_data = self._get_data(data)
        r = self._Core.next_fever(ark_id, game_name, fs_setting, gn_data, platform_data, game_data)
        self._InitResponse(resp, Body=r)

    def _get_data(self, data):
        ark_id = data.get('ark_id')
        game_name = data.get('game_name')
        fs_setting = data.get('fs_setting', {})
        platform_data = data.get('platform_data', {})
        gn_data = data.get('gn_data', {})
        game_data = data.get('game_data', {})
        return ark_id, game_name, fs_setting, platform_data, gn_data, game_data

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



