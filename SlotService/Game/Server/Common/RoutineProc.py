import gevent
from gevent import Greenlet
import logging
import sys, traceback

class RoutineProc(Greenlet):
    def __init__(self, name, passTime=1, func=None, start=True, logger=None, args=(), kwargs=None):
        if not callable(func):
            if logger is not None:
                logger.warn("{} func={} is not callable".format(self.__class__.__name__, func))
            if type(self) == RoutineProc:
                raise Exception("RoutineProc func=None")
        self._name = name
        Greenlet.__init__(self)
        self.passTime = passTime
        self.logger = logger or logging.getLogger(self._name)
        self._func = func
        self._args = args
        self._kwargs = kwargs or {}
        if start:
            self.start()

    def _run(self):
        while self.started:
            try:
                self.update(self.passTime)
            except:
                self.logger.error("%s.%s \n%s"%(str(self.__class__.__name__),sys._getframe().f_code.co_name,traceback.format_exc()))
            gevent.sleep(self.passTime)

    def update(self, passTime):
        self._func(*self._args, **self._kwargs)