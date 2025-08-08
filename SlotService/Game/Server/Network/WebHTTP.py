#!/usr/bin/python
# -*- coding: utf-8 -*-

import falcon
from falcon import RequestOptions
import datetime
try:
    from simplejson import JSONEncoder
    import simplejson as json
except ImportError:
    from json import JSONEncoder
    import json

class WebHTTP():
    def __init__(self, server):
        self.server = server
        self.app = falcon.App()
        if 'auto_parse_form_urlencoded' in RequestOptions.__slots__:
            self.app.req_options.auto_parse_form_urlencoded = True

    def app(self):
        return self.app

class DatesToStrings(JSONEncoder):
    def _encode(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {self._encode(k): self._encode(v) for k, v in list(obj.items())}
        elif isinstance(obj, list):
            return [self._encode(v) for v in obj]
        else:
            return obj

    def encode(self, obj):
        return super(DatesToStrings, self).encode(self._encode(obj))
