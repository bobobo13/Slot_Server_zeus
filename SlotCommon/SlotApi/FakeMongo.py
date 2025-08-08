import pymongo
import copy


class FakeMongoDB:
    def __init__(self):
        pass
        # self.db = {"SlotSetting": FakeMongoCollection({
        #     "GameName": "Default",
        #     "WinType": [
        #         -1,
        #         0,
        #         1,
        #         5,
        #         10,
        #         25,
        #         50
        #     ]
        # })}

    def __getitem__(self, key):
        pass
        # if key not in self.db:
        #     self.db[key] = FakeMongoCollection()
        # return self.db[key]


class FakeMongoCollection:
    def __init__(self, d=None):
        self.docs = [] if d is None else [d]

    def find(self, query, projection=None):
        r = []
        for doc in self.docs:
            for key, value in query.items():
                if doc.get(key) != value:
                    break
            r.append(doc)
        return r
    def find_one(self, query, projection=None):
        r = self.find(query, projection)
        return r[0] if len(r) > 0 else None

    def delete_one(self, query):
        f = self.find(query)
        if len(f) > 0:
            self.docs.remove(f[0])

    def update_one(self, query, update, upsert=False):
        return self.find_one_and_update(query, update, upsert)

    def find_one_and_update(self, query, update, upsert=False, return_document=None):
        docs = self.find(query)
        if len(docs) == 0:
            if upsert:
                if '$setOnInsert' in update:
                    doc = update['$setOnInsert']
                    doc.update(query)
                    self.docs.append(doc)
                    return doc
                elif '$set' in update:
                    doc = update['$set']
                    doc.update(query)
                    self.docs.append(doc)
                    return doc
            return None
        doc = docs[0]
        original_doc = copy.deepcopy(doc)
        if '$set' in update:
            for k, v in update['$set'].items():
                doc[k] = v

        return original_doc if return_document == pymongo.ReturnDocument.BEFORE else doc

    def create_index(self, keys, unique):
        pass

    def with_options(self, read_preference):
        return self
