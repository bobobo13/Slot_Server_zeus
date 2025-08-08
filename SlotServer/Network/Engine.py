#!/usr/bin/python
# -*- coding: utf-8 -*-
import gevent

from gevent.pool import Group
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler

from . import WebHTTP
from . import WebSocket
from ..Log import LogManager


engine = None
class Engine():
    def __init__(self):
        self.name = 'Engine'
        self.arkGroup = Group()
        self.logger = LogManager.sys_log

    def start(self, server):
        self.server = server
        self.webHTTP = WebHTTP.WebHTTP(server)
        self.webSocket = WebSocket.WebSocket(server)
        if not self.server.started:
            self.server.start()

    def startHTTPWSGI(self, app, host='', port=8080):
        webHTTPApp = WSGIServer((host, port), app)
        http = gevent.spawn(webHTTPApp.serve_forever)
        self.arkGroup.add(http)
        self.logger.info('Serving HTTP({})'.format(port))

    def startWebSocketWSGI(self, app, host='', port=8081):
        webSocketApp = WSGIServer((host, port), app,
                                  handler_class=WebSocketHandler)
        webSocket = gevent.spawn(webSocketApp.serve_forever)
        self.arkGroup.add(webSocket)
        self.logger.info('Serving WebSocket({})'.format(port))

    def run(self):
        self.arkGroup.join()
        while True:
            gevent.sleep(1)

    def httpApp(self):
        return self.webHTTP.app()

    def webSocketApp(self):
        return self.webSocket.app()

    def app(self):
        return self.server

def instance():
    global engine
    if engine is None:
        engine = Engine()
    return engine
