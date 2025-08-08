# -*- coding: utf-8 -*-
from builtins import str
from builtins import object
import logging
import logging.handlers
from sys import platform as _platform
import platform
import os

loggerMap = {}
sys_log = logging.getLogger('Engine')

class LogManager():
    FORMAT = '%(name)s[%(process)d]: %(asctime)s - %(levelname)s - %(message)s'
    NOCONSOLE=False

    @staticmethod
    def change_format(logger):
        for h in logger.handlers:
            if isinstance(h, logging.StreamHandler):
                logger.removeHandler(h)
                break
        ch = logging.StreamHandler()
        if LogManager.NOCONSOLE:
            ch.setLevel(99)
        ch.setFormatter(logging.Formatter(LogManager.FORMAT))
        logger.addHandler(ch)


    def __init__(self, code_name='Engine', filename='', logPath='', maxBytes=104857600, backupCount=10, loglevel='DEBUG'):
        global loggerMap
        if code_name in loggerMap:
            raise Exception('%s is aready used in SysLog',code_name)
        self.code_name=code_name
        LogManager.change_format(logging.root)
        self.loglevel = self.getloglevel(loglevel)
        self.logger = logging.getLogger(self.code_name)
        self.logger.setLevel(self.loglevel)
        loggerMap['Engine'] = self.logger

        if _platform.startswith("linux"):
            # linux
            try:
                #handler = logging.handlers.SysLogHandler(address='/dev/log')
                if platform.dist()[0] == 'redhat' or platform.dist()[0] == 'centos':
                    if logPath == '':
                        filename = str(os.environ['HOME'])+'/log/'+str(code_name)+'.log'
                    else:
                        filename = logPath+"/"+str(code_name)+'.log'
                    #handler = logging.handlers.RotatingFileHandler(filename, maxBytes=maxBytes, backupCount=backupCount)
                    from cloghandler import ConcurrentRotatingFileHandler
                    handler = ConcurrentRotatingFileHandler(filename, "a", maxBytes=maxBytes, backupCount=backupCount)
                else:
                    if str(code_name) == 'Engine':
                        #filename = '/var/log/arklog/' + str(code_name) + '.log'
                        #handler = logging.handlers.RotatingFileHandler(filename, maxBytes=maxBytes,
                        #                                               backupCount=backupCount)
                        handler = logging.handlers.SysLogHandler(address='/dev/log')
                    else:
                        handler = logging.handlers.SysLogHandler(address='/dev/log')
                        # handler.setLevel(self.loglevel)
            except:
                pass
            else:
                handler.setLevel(self.loglevel)
                # set formatter to handler
                handler.setFormatter(logging.Formatter(LogManager.FORMAT))
                # add handler to logger
                self.logger.addHandler(handler)
        elif _platform == "darwin":
            # OS X
            try:
                handler = logging.handlers.SysLogHandler(address='/dev/log')
            except:
                pass
            else:
                handler.setLevel(self.loglevel)
                # set formatter to handler
                handler.setFormatter(logging.Formatter(LogManager.FORMAT))
                # add handler to logger
                self.logger.addHandler(handler)
        elif _platform == "win32":
            # Windows...
            if logPath == '':
                filename = '../Log/'+str(code_name)+'.log'
            else:
                filename = logPath+"/"+str(code_name)+'.log'
            # create file handler and set level to loglevel
            handler = logging.handlers.RotatingFileHandler(filename, maxBytes=maxBytes, backupCount=backupCount)
            handler.setLevel(self.loglevel)

            # set formatter to handler
            handler.setFormatter(logging.Formatter(LogManager.FORMAT))
            # add handler to logger
            self.logger.addHandler(handler)

    def sys_log(self):
        return logging.getLogger('Engine')

    def getLogger(self, name):
        if name in loggerMap:
            return loggerMap[name]
        logger = logging.getLogger('name')
        logger.setLevel(self.loglevel)
        loggerMap[name]=logger
        return logger

    #回傳log level
    def getloglevel(self, level='INFO'):
        if level == 'DEBUG':
            return logging.DEBUG
        elif level == 'WARNING':
            return logging.WARNING
        elif level == 'ERROR':
            return logging.ERROR
        elif level == 'CRITICAL':
            return logging.CRITICAL
        else:
            return logging.INFO

# ArkSysLog('Backend')
# ArkSysLog('Frontend')