from __future__ import unicode_literals
from unittest import TestCase

from pymongo import MongoClient

from ognom.helpers.counter import MongoCounter


class TestMongoCounter(TestCase):
    def setUp(self):
        self.mongo_client = MongoClient(host='localhost:27017')
        self.collection = self.mongo_client.test_mongo_counters.counters
        self.collection.remove()

    def test_valid(self):
        counter = MongoCounter(self.collection, 'counter1')
        assert counter.next_value() == 1
        assert counter.next_value() == 2
        assert counter.get_value() == 2
        assert counter.next_value() == 3
        assert counter.get_value() == 3
