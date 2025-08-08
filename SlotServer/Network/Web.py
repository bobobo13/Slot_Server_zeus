#!/usr/bin/python
# -*- coding: utf-8 -*-

import falcon
import traceback
from ..Log import LogManager

class Controller():
    def __init__(self, name, webServer):
        self.webServer = webServer
        self.name = name

    def getName(self):
        return self.name

    def addCommonRoute(self, route, controller):
        webAPI = WebAPI(controller)
        url = '/' + route
        self.webServer.addWebAPI(url, webAPI)

    def addMenu(self, menu_link, menu_link_name):
        url = '/' + self.name + '/' + menu_link
        self.webServer.addMenu(self.name, url, menu_link_name)

    def addRoute(self, route, controller):
        webAPI = WebAPI(controller)
        url = '/' + self.name + '/' + route
        self.webServer.addWebAPI(url, webAPI)

    def addSink(self, route, controller):
        webAPI = WebAPI(controller)
        url = '/' + self.name + '/' + route
        self.webServer.addWebSinkAPI(url, webAPI)

class WebAPI(object):
    def __init__(self, controller):
        self.controller = controller

    def on_get(self, req, resp, **kwargs):
        try:
            resp.set_header('Access-Control-Allow-Origin', '*')
            resp.set_header('Content-Type', 'text/html; charset=utf-8')
            resp.status = falcon.HTTP_200
            self.controller(req, resp, kwargs)
        except Exception as e:
            resp.status = falcon.HTTP_500
            resp.body = None
            LogManager.sys_log.error("WebAPI:on_get Exception : " + str(traceback.format_exc()))
            raise Exception("WebAPI:on_get Exception : " + str(traceback.format_exc()))

    def on_post(self, req, resp, **kwargs):
        try:
            resp.set_header('Access-Control-Allow-Origin', '*')
            resp.set_header('Content-Type', 'text/html; charset=utf-8')
            resp.status = falcon.HTTP_200
            self.controller(req, resp, kwargs)
        except Exception as e:
            resp.status = falcon.HTTP_500
            resp.body = None
            LogManager.sys_log.error("WebAPI:on_post Exception : " + str(traceback.format_exc()))
            raise Exception("WebAPI:on_post Exception : " + str(traceback.format_exc()))