#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import base64


class WebSocket(object):
    def __init__(self, server):
        self.server = server
        self.counter = 0

    def __call__(self, environ, start_response):
        if environ["PATH_INFO"] == '/command':
            ws = environ["wsgi.websocket"]
            message = ws.receive()
            login_data = json.loads(base64.standard_b64decode(message), encoding='utf-8')
            self.server._connect(login_data, ws)
            bConnected = True
            while (bConnected):
                message = ws.receive()
                if (message == None):
                    bConnected = False
                else:
                    ark_data = json.loads(base64.standard_b64decode(message), encoding='utf-8')
                    self.server._receive(ark_data)
            self.server._disconnect(login_data)

        # if environ['PATH_INFO'] == '/gmtool':
        #     ws = environ["wsgi.websocket"]
        #     from ...ArkBackend.System.Terminal import Terminal
        #     self.terminal = Terminal(ws)

    def app(self):
        return self