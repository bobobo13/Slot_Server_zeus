# -*- coding: utf-8 -*-
__author__ = "eitherwang"

from Common.RoutineProc import RoutineProc
import time
import pymongo
import sys, traceback


class MemTimerTrigger(RoutineProc):
    """
    This is an example for timer trigger.
    It stores data in memory, only can be used in single process env.
    DO NOT use in production environment!
    """

    def __init__(self, logger=None):
        super(MemTimerTrigger, self).__init__("TimerTrigger", 5, func=self._refresh, start=False, logger=logger)
        self._data = {}
        self._callback = {}

    def _refresh(self):
        tNow = time.time()
        for key, data in self._GetAllTimer():
            if tNow > data["Time"]:
                RegName = data["Name"]
                if RegName not in self._callback:
                    if self.logger is not None:
                        self.logger.warn("[TimerTrigger] {} not in callback functions!")
                    continue
                success = self.Unset(key, RegName)
                if success:
                    self._callback[RegName](key, data)

    def ForceTrigger(self, key, RegName):
        data = self._GetTimer(key, RegName)
        if data is None:
            return
        RegName = data["Name"]
        success = self.Unset(key, RegName)
        if success:
            self._callback[RegName](key, data)

    def _GetTimer(self, key, RegName):
        if key not in self._data:
            return None
        return self._data[key]

    def _GetAllTimer(self):
        return self._data.items()

    def Register(self, RegName, callback):
        self._callback[RegName] = callback

    def Set(self, key, RegName, TriggerAfterSeconds, data=None):
        if data is None:
            data = {}
        data["Name"] = RegName
        data["Time"] = time.time() + TriggerAfterSeconds
        self._data[key] = data

    def Unset(self, key, RegName):
        r = self._data.pop(key, None)
        if r is None:
            return False
        return True


class MongoTimerTrigger(MemTimerTrigger):
    def __init__(self, DataSource, logger=None):
        super(MongoTimerTrigger, self).__init__(logger)
        self._Collection = DataSource["TimerTrigger"]
        self._Collection.create_index([("Id", pymongo.ASCENDING), ("Name", pymongo.ASCENDING)], unique=True)
        self._Collection.create_index([("Time", pymongo.DESCENDING)])

    def Set(self, key, RegName, TriggerAfterSeconds, data=None):
        if data is None:
            data = {}
        qry = {
            "Id": key,
            "Name": RegName,
        }
        data["Time"] = time.time() + TriggerAfterSeconds
        try:
            self._Collection.find_and_modify(qry, update={"$set": data}, upsert=True)
        except:
            self.logger.error(
                "%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))

    def Unset(self, key, RegName):
        try:
            r = self._Collection.find_and_modify({"Id": key, "Name": RegName}, remove=True)
        except:
            self.logger.error(
                "%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return False
        if r is None:
            return False
        return True

    def _GetAllTimer(self):
        try:
            cursor = self._Collection.find({}, sort=[("Time", pymongo.DESCENDING)], limit=1000)
        except:
            self.logger.error(
                "%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return []
        return [(doc["Id"], doc) for doc in cursor]

    def _GetTimer(self, key, RegName):
        qry = {"Id": key, "Name": RegName}
        try:
            doc = self._Collection.find_one(qry, sort=[("Time", pymongo.DESCENDING)], limit=1000)
        except:
            self.logger.error(
                "%s.%s\n%s" % (str(self.__class__.__name__), sys._getframe().f_code.co_name, traceback.format_exc()))
            return {}
        return doc
