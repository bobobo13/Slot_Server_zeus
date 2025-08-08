#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from builtins import range
from builtins import object
from past.utils import old_div
__author__ = 'duyhsieh'

import time
import gevent
from gevent import queue
import os 
import pymongo
import pymongo.errors
import traceback


class JobQueueService(object):
    def __init__(self, name, logger, workers=1):
        if workers <= 0:
            raise Exception("This is not funny!!!!!!!!!!!")
        self.pid = os.getpid()
        self.name = name
        self.QUEUE_SIZE = 5000
        self.RETRY_QUEUE_SIZE = 1000
        self.put_timeout = 1
        self.logger = logger
        self.workers = workers
        self._working_count = 0
        self._retry_working_count = 0
        self.__queue = queue.Queue(self.QUEUE_SIZE)
        self.__retry_queue = queue.Queue(self.RETRY_QUEUE_SIZE)
        self.__begin()

    def __begin(self):
        for i in range(0, self.workers):
            gevent.spawn(self.__worker_loop)
        gevent.spawn(self.__retry_worker_loop)
        gevent.spawn(self.__update_loop)

    def push_job(self, function, *args, **kwargs):
        #data = (function, args, kwargs, None)
        if "_SrcPath" in kwargs: # for queue retry: keep the original callstack, instead of retry's callstack
            callstack = kwargs["_SrcPath"]
            kwargs.pop("_SrcPath") # prevent calling pymongo function with invalid keyword
        else:
            callstack = "->".join(repr(x) for x in traceback.extract_stack()[:-1] )
        data = (function, args, kwargs, callstack)
        try:
            self.__queue.put(data, block=True, timeout=self.put_timeout)
        except gevent.queue.Full:
            self.logger.error("[JobQueueService] queue Full!name={}, data={},callstack={}".format(self.name, data, traceback.format_exc()))
        except:
            self.logger.error("[JobQueueService] put queue error!name={}, data={},callstack={}".format(self.name, data, traceback.format_exc()))

    def __push_retry_job(self, function, *args, **kwargs):
        if "_SrcPath" in kwargs: 
            callstack = kwargs["_SrcPath"]
            kwargs.pop("_SrcPath") 
        else:
            callstack = "->".join(repr(x) for x in traceback.extract_stack()[:-1] )

        data = (function, args, kwargs, callstack)
        try:
            self.__retry_queue.put(data, block=True, timeout=self.put_timeout)
        except gevent.queue.Full:
            self.logger.error("[JobQueueService] Retry queue Full! name={}, data={},callstack={}".format(self.name, data, traceback.format_exc()))
        except:
            self.logger.error("[JobQueueService] put Retry queue error! name={}, data={},callstack={}".format(self.name, data, traceback.format_exc()))
        pass

    def get_job_count(self):
        return self.__queue.qsize()
    
    def get_retry_job_count(self):
        return self.__retry_queue.qsize()

    def __update_loop(self):
        while True:
            x = self.get_job_count()
            y = self.get_retry_job_count()
            if x > old_div(self.QUEUE_SIZE,100) or y > 0:
                self.logger.info("[JobQueueService]Name={},PID={},Jobs={},RetryJobs={}".format(
                    self.name, self.pid, x, y))
            gevent.sleep(60)

    def __worker_loop(self):
        while True:
            data = self.__queue.get(block=True, timeout=None)
            self._working_count += 1
            try:
                function = data[0]
                args = data[1]
                kwargs = data[2]
                src_call_path = data[3]
                function(*args, **kwargs)
            except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.NetworkTimeout): # includes AutoReconnect, which is child of ConnectionFailure
                self.logger.error('[JobQueueService]wait to retry: name={}, hnd={},args={},kwargs={},srcpath={},__callstack={}'.format(
                    self.name, function, args, kwargs, src_call_path, traceback.format_exc()))
                kwargs["_SrcPath"] = src_call_path
                gevent.sleep(0.5)
                self.__push_retry_job(function, *args, **kwargs)
            except: # prevent handler from throwing exception and break remaining iteration loop
                self.logger.error('[JobQueueService]Err! name={}, hnd={},args={},kwargs={},srcpath={},__callstack={}'.format(
                    self.name, function, args, kwargs, src_call_path, traceback.format_exc()))
            finally:
                self._working_count -= 1
    
    def __retry_worker_loop(self):
        while True:
            data = self.__retry_queue.get(block=True, timeout=None)
            self._retry_working_count += 1
            try:
                function = data[0]
                args = data[1]
                kwargs = data[2]
                src_call_path = data[3]
                function(*args, **kwargs)
            except: # prevent handler from throwing exception and break remaining iteration loop
                self.logger.error('[JobQueueService]Retry Fail!name={}, hnd={},args={},kwargs={},srcpath={},__callstack={}'.format(
                    self.name, function, args, kwargs, src_call_path, traceback.format_exc()))
            finally:
                self._retry_working_count -= 1

    def wait_queue_empty(self, timeout=None):
        quit_time = time.time() + timeout if timeout else None
        while True:
            x = self.get_job_count()
            y = self.get_retry_job_count()
            if x <= 0 and y <= 0 and self._working_count <= 0 and self._retry_working_count <= 0:
                self.logger.info("[JobQueueService]All done. Name={}, PID={}".format(self.name, self.pid))
                break
            elif quit_time is not None and time.time() > quit_time:
                self.logger.error("[JobQueueService]wait_queue_empty timeout quit Name={},PID={},Jobs={},RetryJobs={} Workings={} RetryWorkings={}".format(self.name, self.pid, x, y, self._working_count, self._retry_working_count))
                break
            gevent.sleep(1)

'''
Exception hierarchy:

Exception <- PymongoError <- ConnectionFailure <- AutoReconnect
                        |<- OperationFailure <- BulkWriteError
                                           |<- DuplicateKeyError

        |<- ExceededMaxWaiters
'''

if __name__ == "__main__":
    def TestJobSleep(name, time, fail=False):
        print("Job {} sleeping".format(name))
        gevent.sleep(time)
        if fail:
            raise pymongo.errors.ConnectionFailure()
        print("Job {} done".format(name))

    import logging
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    jb = JobQueueService("TestJobs", logger, workers=2)
    jb.push_job(TestJobSleep, 1, 3)
    jb.push_job(TestJobSleep, 2, 4)
    jb.push_job(TestJobSleep, 3, 5)
    jb.push_job(TestJobSleep, 4, 5, True)
    jb.push_job(TestJobSleep, 5, 2)

    print("waiting all done")
    jb.wait_queue_empty(10)
