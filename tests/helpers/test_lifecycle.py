import unittest
from datetime import datetime

from ognom.base import Collection
from ognom.fields import StringField
from ognom.helpers.lifecycle import DocWithLifeCycle


class FooDoc(DocWithLifeCycle):
    objects = Collection(
        db_name='main',
        collection_name='testmodel'
    )

    bar = StringField()


class TestLifeCycleMixin(unittest.TestCase):

    def test_created_at_should_be_inplace(self):
        td = FooDoc.objects.create({'bar': 'baz'})
        assert isinstance(td.created_at, datetime)
        assert td.bar == 'baz'

    def test_updated_at_should_not_be_inplace(self):
        td = FooDoc.objects.create({})
        assert td.updated_at is None

    def test_updated_at_should_present(self):
        td = FooDoc.objects.create({})
        td.updated_at = datetime.utcnow()
        td.save()
        assert isinstance(td.updated_at, datetime)

    def test_deleted_at_should_present(self):
        td = FooDoc.objects.create({})
        td.deleted_at = datetime.utcnow()
        td.save()
        assert isinstance(td.deleted_at, datetime)
