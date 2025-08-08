#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import traceback
import hmac
import hashlib
import base64

def getData(self, name, key, body, logger=None, fixKey=None):
    data = None
    timestamp = None
    try:
        data = json.loads(decode_base64(body), encoding='utf-8')
        sign = data['check_code']
        sdata = data['Data']
        timestamp = data["Ts"]
        key = fixKey(key, sdata) if fixKey is not None else key
        if key is None:
            self.logger.warn("{}:getData : {}\n data:{} key is None".format(self.strName, str(traceback.format_exc()), data))
            return -107, None
    except:
        if not data:
            data = body
        if data is not None:
            self.logger.warn("{}:getData : {}\n data:{}".format(self.strName, str(traceback.format_exc()), data))
        else:
            self.logger.warn("{}:getData : {}\n data:{} data None".format(self.strName, str(traceback.format_exc()), data))
        return -102, None

    # noinspection PyTypeChecker
    if sign != hmac.new(str(key), str(sdata), hashlib.sha1).hexdigest():
        self.logger.warn("{}:getData : Signature error \n data:{} sign:{} ".format(self.strName, data, sign))
        return -101, None
    try:
        data = json.loads(sdata)
    except UnicodeDecodeError:
        data = json.loads(sdata, encoding='utf-8')
    except:
        if sdata is not None:
            self.logger.warn("{}:getData : {}\n data:{}".format(self.strName, str(traceback.format_exc()), data))
        else:
            self.logger.warn("{}:getData : {}\n data:{} data None".format(self.strName, str(traceback.format_exc()), data))
        return -103, None

    self.logger.info("{}:getData:{}, ts:{}".format(name, data, timestamp))
    return 0, data


def _getSignKey(self, name, groupName):
    hashKey = self._Core.GetSignKey(groupName)
    if hashKey is None:
        self.logger.error('[{}][_getSignKey] groupName:{}, hashKey:{}'.format(name, groupName, hashKey))
        return None
    self.logger.debug('[{}][_getSignKey] groupName:{}, hashKey:{}'.format(name, groupName, hashKey))
    return hashKey

def decode_base64(data):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    missing_padding = len(data) % 4
    if missing_padding != 0:
        data += b'='* (4 - missing_padding)
    return base64.decodestring(data)