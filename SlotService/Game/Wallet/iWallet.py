# -*- coding: utf-8 -*-
from builtins import object
__author__ = 'eitherwang'

class iWallet(object):

    def InitPlayer(self, ark_id):
        pass

    def GetCurrencyType(self):
        return None

    def IsCredit(self, strType, **kwargs):
        return False

    def GetCredit(self, ark_id, TypeList, **kwargs):
        return None

    def AddCredit(self, ark_id, strType, nAmount, **kwargs):
        return None

    def SubCredit(self, ark_id, strType, nAmount, **kwargs):
        return None

    def Transaction(self, ark_id, SubDict, AddDict, **kwargs):
        return None

    def AcquireLock(self, ark_id, PlayerData=None, ttl=None):
        return True

    def ReleaseLock(self, ark_id, PlayerData=None):
        pass

    def CheckLock(self, ark_id, PlayerData=None):
        return True

    def RefreshLock(self, ark_id, PlayerData=None, ttl=None):
        pass
