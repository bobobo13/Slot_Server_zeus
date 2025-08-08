# -*- coding: utf-8 -*-

import json
from datetime import datetime


class JsonEncoder(json.JSONEncoder):

    @classmethod
    def represent_object(cls, obj):
        if isinstance(obj, (int, float, str, bool)) or obj is None:
            return obj
        elif isinstance(obj, (list, dict, tuple)):
            return cls.represent_iterable(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, JsonSerializable):
            return obj.json_encoder.default(obj)
        else:
            return hash(obj)

    @classmethod
    def represent_iterable(cls, iterable):
        if isinstance(iterable, (list, tuple)):
            return [cls.represent_object(value) for value in iterable]
        elif isinstance(iterable, dict):
            return {cls.represent_object(key): cls.represent_object(value) for key, value in iterable.items()}

    def default(self, obj):
        return self.represent_object(obj)
        # result = {}
        # for attr, value in obj.__dict__.items():
        #     result[attr] = self.represent_object(value)
        # return result


class JsonDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        super(JsonDecoder, self).__init__(*args, **kwargs)

    def decode(self, data, object_class):
        if isinstance(data, str):
            data = super(JsonDecoder, self).decode(data)
        data = {key: value for key, value in data.items() if not key.endswith("_id")}
        return object_class(**data)


class JsonSerializable(object):
    json_encoder = JsonEncoder()
    json_decoder = JsonDecoder()

    def to_json(self):
        return self.json_encoder.encode(self)

    def to_dict(self):
        return self.json_encoder.default(self)

    @classmethod
    def from_json(cls, data):
        return cls.json_decoder.decode(data, cls)
