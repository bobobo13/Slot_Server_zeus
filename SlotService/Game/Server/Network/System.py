# -*- coding: utf-8 -*-
from builtins import str
import traceback
import gevent
from gevent import Greenlet
import inspect

class System(Greenlet):
    def __init__(self, server,passTime=60):
        Greenlet.__init__(self)
        self.logger = server.logger
        self.function_dict = {}
        self.drt_function_dict = {}
        self.function_dict_arg_info = {}
        self.server = server
        self.passTime = passTime

    def register(self, name, function):
        self.function_dict[name] = function
        argspec = inspect.getfullargspec(function)
        self.function_dict_arg_info[name] = [len(argspec[0]), argspec.varargs]

    def regDirectCommand(self, name, function):
        self.drt_function_dict[name] = function
    def _run(self):
        while self.started:
            gevent.sleep(self.passTime)
            try:
                self.update(self.passTime)
            except:
                self.logger.error("%s.%s \n%s"%(str(self.__class__.__name__), get_function_name(),traceback.format_exc()))

    def update(self, passTime):
        return

    def getServer(self):
        return self.server

    def _command(self, data, *args, **kwargs):
        try:
            # print 'ArkSystem:_command:',data
            ark_id = data['ark_id']
            cmd_name = data['cmd_name']
            cmd_data = data['cmd_data']
            func = self.function_dict[cmd_name]
            if self.function_dict_arg_info[cmd_name][0] > 3:
                # 相容舊版
                if self.function_dict_arg_info[cmd_name][1]:
                    return func(ark_id, cmd_data, data, *args, **kwargs)
                else:
                    return func(ark_id, cmd_data, data)
            else:
                return func(ark_id, cmd_data)
        except StoreReceiveException as e:
            self.logger.error('System:_command : ' + str(data) + str(traceback.format_exc()))
            raise StoreReceiveException(str(e))

        except Exception as e:
            # print 'ArkSystem:_command : ',data;
            self.logger.error('System:_command : ' + str(data) + str(traceback.format_exc()))
            raise Exception('System:_command() :' + str(traceback.format_exc()))

    def _direct_command(self, data,user_ip,*args,**kwargs):
        try:
            cmd_name = data.get('cmd_name')
            cmd_data = data.get('cmd_data')
            if cmd_name in self.drt_function_dict:
                return self.drt_function_dict[cmd_name](cmd_data,user_ip,data,*args,**kwargs)
            else:
                self.logger.warning("%s.%s cmd_id not in arkDrtFunctionDict:%s, ark_data:%s"%(self.__class__.__name__, get_function_name().f_code.co_name,cmd_name,data))
        except Exception as e:
            # print 'System:_command : ',data;
            self.logger.error('System:_command : ' + str(data) + str(traceback.format_exc()))
            raise Exception('System:_command() :' + str(traceback.format_exc()))

    # def findUser(self, ark_id):
    #     result = None
    #     try:
    #         skip_data = {'_id': False, 'ark_id': False, 'create': False};
    #         user_data = self.getServer().gameUser.find_one({'ark_id': ark_id}, skip_data);
    #         if (user_data != None):
    #             result = user_data['player_data'];
    #         return result;
    #     except Exception as e:
    #         # print result;
    #         ArkSysLog.sys_log.error('ArkSystem:findUser : ' + str(result) + str(traceback.format_exc()))
    #         raise Exception('ArkSystem:findUser() :' + str(result) + str(traceback.format_exc()));
    #
    # def updateUser(self, ark_id, data):
    #     result = None
    #     try:
    #         date_time = self.getServer().getLocalDateTime()
    #         query_key = {'ark_id': ark_id}
    #         update_data = {'player_data': data, 'update': date_time}
    #         update_field = {'$set': update_data}
    #         if pymongo.get_version_string().startswith("3."):
    #             result = self.getServer().gameUser.update_one(filter=query_key, update=update_field, upsert=False)
    #             return result.raw_result
    #         else:
    #             result = self.getServer().gameUser.update(spec=query_key, document=update_field, upsert=False)
    #             return result
    #     except Exception as e:
    #         # print result;
    #         ArkSysLog.sys_log.error('ArkSystem:updateUser : ' + str(result) + str(traceback.format_exc()))
    #         raise Exception('ArkSystem:updateUser() :' + +str(result) + str(traceback.format_exc()))
    #
    # def getPlayerData(self, key):
    #     return self.getServer().getPlayerData('player_' + key)
    #
    # def setPlayerData(self, key, value):
    #     return self.getServer().setPlayerData('player_' + key, value)
    #
    # def deletePlayerData(self, key):
    #     return self.getServer().deletePlayerData('player_' + key)
    #
    # def getLogger(self):
    #     return self.getServer().getLogger()
    #
    # def initCollection(self, name):
    #     database = self.getServer().getDatabase()
    #     collection = database[name]
    #     if pymongo.get_version_string().startswith("3."):
    #         collection.create_index([('ark_id', pymongo.DESCENDING)], unique=True)
    #     else:
    #         collection.ensure_index([('ark_id', pymongo.DESCENDING)], unique=True)
    #     return collection
    #
    # def getRootPath(self):
    #     return self.getServer().getArkPath('Game')
    #
    # def getSystem(self, name):
    #     return self.getServer().getSystem(name)
    #
    # def logout(self, id):
    #     self.getServer()._logout(id)
    #
    # def isUserInWhiteList(self,ark_id):
    #     try:
    #         return True if self.getServer().getIsTestAcc(ark_id) else False
    #     except:
    #         return False

def get_function_name():
    frame = inspect.currentframe()
    return frame.f_code.co_name if frame else "Unknown"

class StoreReceiveException(Exception):
    pass