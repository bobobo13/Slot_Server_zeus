# -*- coding: utf-8 -*-
__author__ = 'allenlin'
import gevent
import traceback
import logging
import sys
from gevent.queue import Queue
from gevent.pool import Group
# from ..ArkUtilities import get_frame

get_frame = getattr(sys, '_getframe')


class IOEventQueue(object):
    def __init__(self, concurrent=1, logger=None, max_queue=None, isMaxQueueContinue=False,
                 continueSleep=0.001, pendingSleep=0.02):
        '''
        @concurrent: 並行處理數量，需大於0
        @logger: logging物件
        @max_queue: Queue上限，需大於0或None，預設為不限制(None)
        @isMaxQueueContinue: 到達上限後是否繼續處理事件，預設為不處理(False)
        '''
        if logger is None:
            logging.basicConfig()
            self.logger = logging.getLogger(str(self.__class__.__name__))
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger=logger
        self._event_queue=Queue(maxsize=max_queue)
        if concurrent <= 0:
            raise Exception('concurrent must great than 0')
        if max_queue is not None and max_queue<=0:  raise Exception('max_queue must great than 0 or None(no limit)')
        self._concurrent=concurrent
        self._max_queue=max_queue
        self._isProcessRunable=True
        self._isMaxQueueContinue=isMaxQueueContinue
        self._continueSleep=continueSleep
        self._pendingSleep=pendingSleep
        if self._max_queue is None:
            self._append=self._append_direct
        else:
            self._append=self._append_block
        self._processGroup=Group()
        for i in range(concurrent):
            self._processGroup.add(gevent.spawn(self._process))
        pass

    def append(self,event_func,event_callback,*args,**kwargs):
        return self._append(event_func,event_callback,*args,**kwargs)

    def _append_direct(self,event_func,event_callback,*args,**kwargs):
        if self._isProcessRunable:
            event_info=(event_func,event_callback,args,kwargs)
            self._event_queue.put_nowait(event_info)
            return True
        else:
            self.logger.debug("%s.%s not ProcessRunable:%s" % (self.__class__.__name__, get_frame().f_code.co_name,
                                                               (event_func, event_callback, args, kwargs)))

    def _append_block(self,event_func,event_callback,*args,**kwargs):
        if self._isProcessRunable:
            if not self._event_queue.full():
                event_info=(event_func,event_callback,args,kwargs)
                self._event_queue.put_nowait(event_info)
                return True
            else:
                self._isProcessRunable = self._isMaxQueueContinue
                self.logger.error("%s.%s event_queue size over max_queue:%s"%(self.__class__.__name__,get_frame().f_code.co_name,self._max_queue))
        if self._isProcessRunable:
            self.logger.debug("%s.%s over max_queue no append:%s"%(self.__class__.__name__,get_frame().f_code.co_name,(event_func,event_callback,args,kwargs)))
        else:
            self.logger.debug("%s.%s not ProcessRunable:%s"%(self.__class__.__name__,get_frame().f_code.co_name,(event_func,event_callback,args,kwargs)))
        pass

    def _process(self):
        while self._isProcessRunable:
            event_info=None
            try:
                event_info=self._event_queue.get()
            except:
                pass
            if event_info:
                ret = None
                event_callback = None
                try:
                    event_func,event_callback,args,kwargs=event_info
                    ret = event_func(*args,**kwargs)
                except gevent.GreenletExit:
                    pass
                except Exception as ex:
                    ret = ex
                    self.logger.error("%s.%s event_info:%s\n%s"%(self.__class__.__name__,get_frame().f_code.co_name,event_info,traceback.format_exc()))

                try:
                    if event_callback:
                        event_callback(ret)
                except gevent.GreenletExit:
                    pass
                except:
                    self.logger.error("%s.%s event_info:%s\n%s"%(self.__class__.__name__,get_frame().f_code.co_name,event_info,traceback.format_exc()))
                try:
                    gevent.sleep(self._continueSleep)
                except gevent.GreenletExit:
                    pass
            else:
                try:
                    gevent.sleep(self._pendingSleep)
                except gevent.GreenletExit:
                    pass
        pass

    def processAll(self):
        while not self._event_queue.empty():
            event_info=None
            try:
                event_info=self._event_queue.get_nowait()
            except:
                pass
            if event_info:
                if self._isProcessRunable:
                    ret = None
                    event_callback = None
                    try:
                        event_func,event_callback,args,kwargs=event_info
                        ret = event_func(*args,**kwargs)
                    except Exception as ex:
                        ret = ex
                        self.logger.error("%s.%s event_info:%s\n%s"%(self.__class__.__name__,get_frame().f_code.co_name,event_info,traceback.format_exc()))

                    try:
                        if event_callback:
                            event_callback(ret)
                    except:
                        self.logger.error("%s.%s event_info:%s\n%s"%(self.__class__.__name__,get_frame().f_code.co_name,event_info,traceback.format_exc()))
                else:
                    self.logger.debug("%s.%s not ProcessRunable:%s"%(self.__class__.__name__,get_frame().f_code.co_name,event_info))

    def stop(self):
        self._isProcessRunable=False
        self._processGroup.kill()