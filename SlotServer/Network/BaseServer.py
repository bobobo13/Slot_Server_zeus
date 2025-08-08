#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import gevent
from gevent import Greenlet
import traceback
import configparser
import platform as pyplatform
from sys import platform as _platform
from ..Log.LogManager import LogManager
from .WebHTTP import WebHTTP

class BaseServer(Greenlet):
    def __init__(self, code_name='', version='dev', passTime=60, global_code_name=None, http_response_handler=None):
        Greenlet.__init__(self)
        self.codeName = code_name
        self.log_manager = LogManager(logPath='./Log')
        self.logger = self.log_manager.logger
        self.passTime = passTime
        self.config = configparser.RawConfigParser()
        self.systemDict = {}
        self.register('server', self)
        self.webHTTP = WebHTTP(self)
        self.app = self.webHTTP.app
        self.controllerDict = {}
        self.menuDict = {}
        self.authController = None

    def getPath(self, function=None):
        function = function or ""
        if _platform.startswith("linux"):
            if pyplatform.dist()[0] == 'redhat' or pyplatform.dist()[0] == 'centos':
                user_name = str(os.environ['HOME'])[6:len(str(os.environ['HOME']))]
                return os.path.join('/home', user_name, self.codeName, function)
            return os.path.join('/etc', 'igs', self.codeName, function)
            # linux
        elif _platform == "darwin":
            # OS X
            return os.getcwd()
        elif _platform == "win32":
            # Windows...
            path = os.getcwd()
            endIndex = path.rfind("\\")
            return os.path.join(path[:endIndex], function)
        return './'

    def register(self, name, system):
        if self.systemDict.get(name) is None:
            self.systemDict[name] = system
            system.start()
        else:
            self.logger.sys_log().error(str(name) + ' is illegal, try another system_name.')
            raise Exception(name + ' is illegal, try another system_name.')

    def update(self, pass_time):
        self.passTime = pass_time

    def receive(self, data):
        print(data)
        self.logger.sys_log().debug('GameServer:receive :' + str(data))

    # update controller function with 2015.03.05
    def app(self):
        return self.app

    def addController(self, controller):
        name = controller.getName()
        self.controllerDict[name] = controller

    def addWebAPI(self, url, controller):
        self.app.add_route(url, controller)

    def addWebSinkAPI(self, url, controller):
        self.app.add_sink(controller, url)


    def _run(self):
        while self.started:
            pass_time = self.passTime
            gevent.sleep(pass_time)
            self.update(pass_time)

    def init(self, path='./', configPath='config/dev/server.cfg', version='dev'):
        try:
            # config
            self.path = path
            self.version = version
            self.config.read_file(open(configPath, 'r'));
            codeName = self.config.get('Server', 'CodeName')
            redisHost = self.config.get('Server', 'RedisHost', fallback=None)
            redisPort = self.config.getint('Server', 'RedisPort', fallback=None)
            mongoHost = self.config.get('Server', 'MongoHost', fallback=None)
            mongoPort = self.config.getint('Server', 'MongoPort', fallback=None)
            self.gmtTime = self.config.get('Server', 'GMTTime', fallback=None)
            if self.gmtTime is None:
                self.gmtTime = 8


        except IOError:
            self.logger.info("Project config directory not found.")
        except Exception as e:
            self.logger.info(str(traceback.format_exc()))
            raise Exception("ArkServer:start Exception : " + str(traceback.format_exc()));