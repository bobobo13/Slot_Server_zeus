# -*- coding: utf-8 -*-
__author__ = 'duyhsieh'
import sys, traceback
import pymongo
import copy
import abc
'''
module is tested under python 2.7.18 with both pymongo 2.8, 2.9 and pymongo 3.12.3
best solution: pymongo 2.x users should upgrade to >=2.9 and pymongo 3.x users should upgrade to >=3.12.3
'''

# Return Document Definition Wrapper
# for pymongo version under 2.9, there is no pymongo.ReturnDocument definition.
# So for legacy compatibility with 2.8, we leave this wrapper definition.

# pymongo 2.x and 3.x use same syntax to reference read reference (ex. pymongo.ReadReference.PRIMARY)
# but they are different objects in 2.x and 3.x.
# note: pymongo 3.x pymongo.read_preference.ReadReference is equal to pymongo.ReadReference


## copied from pymongo 3.x for 2.x compatibility
class ReturnDocument(object):
    """An enum used with
    :meth:`~pymongo.collection.Collection.find_one_and_replace` and
    :meth:`~pymongo.collection.Collection.find_one_and_update`.
    """
    BEFORE = False
    """Return the original document before it was updated/replaced, or
    ``None`` if no document matches the query.
    """
    AFTER = True


def patch_pymongo_enum_compatibility():
    pymongo.ReturnDocument = ReturnDocument


class PyMongoColWrapperInterface(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_data_source(self):
        pass

    @abc.abstractmethod
    def find(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def find_one(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def with_options(self, **kwargs):
        pass

    @abc.abstractmethod
    def create_index(self, keys, unique=False, expireAfterSeconds=None, **kwargs):
        pass

    ################# for pymongo 3.x driver #################

    @abc.abstractmethod
    # accepts manipulate
    def insert_one(self, document, **kwargs):
        pass

    @abc.abstractmethod
    def insert_many(self, documents, ordered=False, **kwargs):
        pass

    @abc.abstractmethod
    def update_one(self, query, operation, upsert=False, **kwargs):
        pass

    @abc.abstractmethod
    def update_many(self, query, operation, upsert=False, **kwargs):
        pass

    @abc.abstractmethod
    def delete_one(self, query, **kwargs):
        pass

    @abc.abstractmethod
    def delete_many(self, query, **kwargs):
        pass

    @abc.abstractmethod
    def find_one_and_replace(self, query, replacement, upsert=False, **kwargs):
        pass

    @abc.abstractmethod
    def find_one_and_update(self, filter,
        update,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        # array_filters=None,
        # hint=None,
        # session=None,
        **kwargs):
        pass

    # legacy API: if you system pymongo is 3.x but code style is pymongo 2.x, these legacy apis just direct command to same apis.
    @abc.abstractmethod
    def update(self, spec, document, upsert=False, manipulate=False, multi=False, **kwargs):
        pass

    @abc.abstractmethod
    def find_and_modify(self, query={}, update=None, upsert=False, new=True, **kwargs):
        pass

    @abc.abstractmethod
    def ensure_index(self, key_or_list, **kwargs):
        pass

    @abc.abstractmethod
    def insert(self, doc_or_docs, manipulate=True, **kwargs):
        pass

    @abc.abstractmethod
    def remove(self, spec_or_id=None, multi=True, **kwargs):
        pass

    @abc.abstractmethod
    def save(self, to_save, **kwargs):
        pass

    @abc.abstractmethod
    def initialize_unordered_bulk_op(self, bypass_document_validation=False):
        pass

    @abc.abstractmethod
    def initialize_ordered_bulk_op(self, bypass_document_validation=False):
        pass

    @abc.abstractmethod
    def aggregate(self, pipeline, **kwargs):
        pass


class PyMongoColV3Wrapper(PyMongoColWrapperInterface):
    def __init__(self, col):
        assert isinstance(col, pymongo.collection.Collection)
        self.col = col

    def get_data_source(self):
        return self.col

    # skip and limit can be cascaded called on the return cursor

    def with_options(self, **kwargs):
        return self.col.with_options(**kwargs)

    def find(self, *args, **kwargs):
        if 'fields' in kwargs: # pymongo 2.x
            kwargs['projection'] = kwargs.pop('fields')

        if 'read_preference' in kwargs: # pymongo 3.x running 2.x style code
            pfr = kwargs.pop('read_preference')
            cursor = self.col.with_options(read_preference=pfr).find(*args, **kwargs)
        else:
            cursor = self.col.find(*args, **kwargs)
        return cursor

    def find_one(self, *args, **kwargs):
        if 'fields' in kwargs: # pymongo 2.x
            kwargs['projection'] = kwargs.pop('fields')

        if 'read_preference' in kwargs:  # pymongo 3.x running 2.x style code
            pfr = kwargs.pop('read_preference')
            result = self.col.with_options(read_preference=pfr).find_one(*args, **kwargs)
        else:
            result = self.col.find_one(*args, **kwargs)
        return result

    # keys example: [('key_a', pymongo.DESCENDING), ('key_b', pymongo.DESCENDING)]
    def create_index(self, keys, unique=False, expireAfterSeconds=None, **kwargs):
        if expireAfterSeconds is not None:
            if unique is not None:
                return self.col.create_index(keys, unique=unique, expireAfterSeconds=expireAfterSeconds, **kwargs)
            else:
                return self.col.create_index(keys, expireAfterSeconds=expireAfterSeconds, **kwargs)
        else:
            if unique is not None:
                return self.col.create_index(keys, unique=unique, **kwargs)
            else:
                return self.col.create_index(keys, **kwargs)

    def insert_one(self, document, **kwargs):
        '''
        pymongo.results.InsertOneResult structure
        ['_InsertOneResult__acknowledged', '_InsertOneResult__inserted_id', '_WriteResult__acknowledged', '__class__', '__delattr__', '__doc__', '__format__', '__getattribute__', '__hash__', '__init__', '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__slots__', '__str__', '__subclasshook__', '_raise_if_unacknowledged', 'acknowledged', 'inserted_id']
        '''
        return self.col.insert_one(document, **kwargs).inserted_id

    def insert_many(self, documents, ordered=False, **kwargs):
    #def insert_many(self, documents, oid=True, ordered=False, **kwargs):
        '''
        pymongo.results.InsertManyResult structure
        ['_InsertManyResult__acknowledged', '_InsertManyResult__inserted_ids', '_WriteResult__acknowledged', '__class__', '__delattr__', '__doc__', '__format__', '__getattribute__', '__hash__', '__init__', '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__slots__', '__str__', '__subclasshook__', '_raise_if_unacknowledged', 'acknowledged', 'inserted_ids']

        - `ordered` (optional): If ``True`` (the default) documents will be
            inserted on the server serially, in the order provided. If an error
            occurs all remaining inserts are aborted. If ``False``, documents
            will be inserted on the server in arbitrary order, possibly in
            parallel, and all document inserts will be attempted.
        '''
        return self.col.insert_many(documents, ordered=ordered, **kwargs).inserted_ids

    def update_one(self, query, operation, upsert=False, **kwargs):
        '''
        UpdateResult structure
        ['_UpdateResult__acknowledged', '_UpdateResult__raw_result', '_WriteResult__acknowledged', '__class__', '__delattr__', '__doc__', '__format__', '__getattribute__', '__hash__', '__init__', '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__slots__', '__str__', '__subclasshook__', '_raise_if_unacknowledged', 'acknowledged', 'matched_count', 'modified_count', 'raw_result', 'upserted_id'])
        '''
        return self.col.update_one(query, operation, upsert=upsert, **kwargs).raw_result

    def update_many(self, query, operation, upsert=False, **kwargs):
        return self.col.update_many(query, operation, upsert=upsert, **kwargs).raw_result

    def delete_one(self, query, **kwargs):
        return self.col.delete_one(query, **kwargs).raw_result

    def delete_many(self, query, **kwargs):
        return self.col.delete_many(query, **kwargs).raw_result

    def drop(self):
        return self.col.drop()

    def find_one_and_replace(self, query, replacement, upsert=False, **kwargs):
        return self.col.find_one_and_replace(query, replacement, upsert=upsert, **kwargs)

    def find_one_and_update(self, filter,
        update,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        # array_filters=None,
        # hint=None,
        # session=None,
        **kwargs):
        return self.col.find_one_and_update(filter,
        update,
        projection,
        sort,
        upsert,
        return_document,
        # array_filters=None,
        # hint=None,
        # session=None,
        **kwargs)

    def distinct(self, key, **kwargs):
        return self.col.distinct(key)

    ##### legacy APIs: since pymongo 3.x still compatible with old APIs, just pass arguments to them #####
    def update(self, spec, document, upsert=False, manipulate=False, multi=False, **kwargs):
        return self.col.update(spec, document, upsert=upsert, manipulate=manipulate, multi=multi, **kwargs)

    def find_and_modify(self, query={}, update=None, upsert=False, new=True, **kwargs):
        if kwargs.get('remove'):
            new = None
        return self.col.find_and_modify(query=query, update=update, upsert=upsert, new=new, **kwargs)

    def ensure_index(self, key_or_list, **kwargs):
        return self.col.ensure_index(key_or_list, **kwargs)

    def insert(self, doc_or_docs, manipulate=True, **kwargs):
        return self.col.insert(doc_or_docs, manipulate=manipulate, **kwargs)

    def remove(self, spec_or_id=None, multi=True, **kwargs):
        return self.col.remove(spec_or_id, multi=multi, **kwargs)

    def save(self, to_save, **kwargs):
        return self.col.save(to_save, **kwargs)

    def initialize_unordered_bulk_op(self, bypass_document_validation=False):
        return pymongo.bulk.BulkOperationBuilder(self.col, False, bypass_document_validation)

    def initialize_ordered_bulk_op(self, bypass_document_validation=False):
        return pymongo.bulk.BulkOperationBuilder(self.col, True, bypass_document_validation)

    def aggregate(self, pipeline, **kwargs):
        return self.col.aggregate(pipeline, **kwargs)


class OptionsFindWrapper(object):
    def __init__(self, col, **kwargs):
        self.col = col
        self.kwargs = kwargs

    def find(self, *args, **kwargs):
        #print("===========merging===========", self.kwargs, "and ======", kwargs)
        kwargs.update(self.kwargs)
        return self.col.find(*args, **kwargs)

    def find_one(self, *args, **kwargs):
        kwargs.update(self.kwargs)
        return self.col.find_one(*args, **kwargs)


class PyMongoColV29Wrapper(PyMongoColV3Wrapper):
    def __init__(self, col):
        assert isinstance(col, pymongo.collection.Collection)
        self.col = col

    def with_options(self, **kwargs):
        return OptionsFindWrapper(self.col, **kwargs)

    def find(self,  *args, **kwargs):
        if 'return_document' in kwargs:
            kwargs['new'] = kwargs.pop('return_document')
        if 'projection' in kwargs:
            kwargs['fields'] = kwargs.pop('projection')
        return self.col.find(*args, **kwargs)

    def find_one(self, *args, **kwargs):
        if 'return_document' in kwargs:
            kwargs['new'] = kwargs.pop('return_document')
        if 'projection' in kwargs:
            kwargs['fields'] = kwargs.pop('projection')
        return self.col.find_one(*args, **kwargs)

    def insert_one(self, document, **kwargs):
        ## if manipulate False is passed into insert, None will be returned and you cannot get object ID, 
        ## so you cannot match your API signature, so we always use True here.
        return self.col.insert(document, **kwargs)

    def insert_many(self, documents, ordered=False, **kwargs):
        r = self.col.insert_many(documents, ordered=ordered, **kwargs)
        return r.inserted_ids

    def update_one(self, query, operation, upsert=False, **kwargs):
        return self.col.update(query, operation, upsert=upsert, multi=False, **kwargs)

    def update_many(self, query, operation, upsert=False, **kwargs):
        return self.col.update(query, operation, upsert=upsert, multi=True, **kwargs)

    def delete_one(self, query, **kwargs):
        return self.col.remove(query, multi=False, **kwargs)

    def delete_many(self, query, **kwargs):
        return self.col.remove(query, multi=True, **kwargs)

    def initialize_unordered_bulk_op(self, bypass_document_validation=False):
        return self.col.initialize_unordered_bulk_op()

    def initialize_ordered_bulk_op(self, bypass_document_validation=False):
        return self.col.initialize_ordered_bulk_op()

    def aggregate(self, pipeline, **kwargs):
        if 'session' in kwargs: # pymongo 3.x only
            kwargs.pop('session')
        return self.col.aggregate(pipeline, **kwargs)


class PyMongoColV28Wrapper(PyMongoColV29Wrapper):
    def insert_many(self, documents, ordered=False, **kwargs):
        # ordered is ignored here in <= pymongo 2.8
        insert_ids = self.col.insert(documents, manipulate=True, **kwargs)
        return insert_ids

    def find_one_and_replace(self, query, replacement, upsert=False, **kwargs):
        if 'return_document' in kwargs:
            kwargs['new'] = kwargs.pop('return_document')
        return self.col.find_and_modify(query, {'$set':replacement}, upsert=upsert, **kwargs)

    def find_one_and_update(self, filter,
        update,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        # array_filters=None,
        # hint=None,
        # session=None,
        **kwargs):
        """Partially Supported in pymongo 2.8"""

        kwargs['new'] = return_document
        if projection:
            kwargs['fields'] = projection

        return self.col.find_and_modify(filter, update, upsert=upsert, sort=sort, **kwargs)


class PyMongoVersion(object):
    VERSION=None

    @classmethod
    def init_version(cls):
        if PyMongoVersion.VERSION is None:
            ver = int(pymongo.get_version_string()[:3].replace('.', ''))
            if ver >= 40:
                raise Exception('[PyMongoClientWrapper] unsupported pymongo version!{}'.format(pymongo.get_version_string()))
            PyMongoVersion.VERSION = ver


class PyMongoDBWrapper(object):
    @staticmethod
    def create_col_wrapper(col):
        PyMongoVersion.init_version()
        if PyMongoVersion.VERSION>=30:
            return PyMongoColV3Wrapper(col)
        elif PyMongoVersion.VERSION>=29:
            return PyMongoColV29Wrapper(col)
        else:
            return PyMongoColV28Wrapper(col)

    def __init__(self, db_source):
        assert isinstance(db_source, pymongo.database.Database)
        self.db_source = db_source

    # override [] operator
    def __getitem__(self, col_name):
        col = self.db_source[col_name]
        return PyMongoDBWrapper.create_col_wrapper(col)

    # override dot operator
    def __getattr__(self, key):
        col = self.db_source[key]
        return PyMongoDBWrapper.create_col_wrapper(col)

    @property
    def connection(self):
        return self.db_source.connection

    @property
    def name(self):
        return self.db_source.name

    def collection_names(self, include_system_collections=True):
        return self.db_source.collection_names(include_system_collections)

    def create_collection(self, name, **kwargs):
        col = self.db_source.create_collection(name, **kwargs)
        return PyMongoDBWrapper.create_col_wrapper(col)

    def get_collection(self, name, **kwargs):
        col = self.db_source.get_collection(name, **kwargs)
        return PyMongoDBWrapper.create_col_wrapper(col)

    def authenticate(self, name=None, password=None,
                     source=None, mechanism='DEFAULT', **kwargs):
        return self.db_source.authenticate(name=name, password=password, source=source, mechanism=mechanism, **kwargs)



class PyMongoClientWrapper(object):
    def __init__(self, client):
        assert isinstance(client, pymongo.mongo_client.MongoClient)
        self.client = client

    # override [] operator
    def __getitem__(self, db_name):
        db = self.client[db_name]
        return PyMongoDBWrapper(db)

    # override dot operator
    def __getattr__(self, db_name):
        db = self.client[db_name]
        return PyMongoDBWrapper(db)


if not hasattr(pymongo, 'ReturnDocument'):
    patch_pymongo_enum_compatibility()