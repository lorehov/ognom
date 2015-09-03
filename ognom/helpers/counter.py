from __future__ import unicode_literals


class MongoCounterError(Exception):
    pass


class MongoCounter(object):
    def __init__(self, collection, counter_name):
        self.collection = collection
        self.counter_name = counter_name

    def get_value(self):
        doc = self.collection.find_one(
            {'_id': self.counter_name}, {'value': 1})
        if not doc:
            raise MongoCounterError(
                'No such counter "{}" in {}'.format(
                    self.counter_name, self.collection.full_name))
        return doc['value']

    def next_value(self):
        doc = self.collection.find_and_modify(
            {'_id': self.counter_name},
            {'$inc': {'value': 1}},
            new=True,
            upsert=True,
        )
        return doc['value']
