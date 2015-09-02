# coding: utf8
import uuid
import unittest
from decimal import Decimal
from datetime import datetime, timedelta
from collections import namedtuple

import pytest
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
from six import string_types

from ognom.validators import EmailValidator
from ognom.base import (
    Document, Repository, inject_repositories, ConnectionManager)
from ognom.fields import (
    StringField, ObjectIdField, DateTimeField, ValidationError, UUIDField,
    ListField, DictField, DocumentField, BooleanField, URLField, HTTPField,
    IntField, GenericField, DecimalField)


_ContactStatus = namedtuple(
    'ContactStatus', ['Awaiting', 'Done', 'InProcess', 'Failed'])
ContactStatus = _ContactStatus('awaiting', 'done', 'in-process', 'failed')


class BaseDoc(Document):
    db_name = 'main'
    _collection_name = 'testmodel'


class TestDocument(unittest.TestCase):

    def test_attribute_getting(self):
        class TD(BaseDoc):
            field1 = StringField()
        td = TD(field1='test_string')
        assert td.field1 == 'test_string'

    def test_attribute_setting(self):
        class TD(BaseDoc):
            field1 = StringField()
        td = TD()
        td.field1 = 'test_string'
        assert td.field1 == 'test_string'

    def test_validating(self):
        class TD(BaseDoc):
            field1 = ObjectIdField()
        td = TD(field1='test_string')
        self.assertRaises(ValidationError, td.validate)

    def test_none_should_be_treated_as_value_absence(self):
        class TD(BaseDoc):
            field1 = StringField(required=True)
        td = TD(field1=None)
        with pytest.raises(ValidationError):
            td.validate()

    def test_none_should_pass_optional_validators(self):
        class TD(BaseDoc):
            field1 = StringField(validators=[EmailValidator()])
        td = TD(field1=None)
        td.validate()
        assert td.field1 is None

    def test_required_field(self):
        class TD(BaseDoc):
            field1 = StringField(required=True)
        td = TD()
        self.assertRaises(ValidationError, td.validate)

    def test_defaults(self):
        class TD(BaseDoc):
            field1 = StringField(required=True, default='test_string')
            field2 = BooleanField(required=True, default=False)
            field3 = StringField(default='test_string')

        td = TD()
        assert td.field1 == 'test_string'
        assert td.field2 is False
        assert td.field3 == 'test_string'

    def test_to_mongo(self):
        class TD(BaseDoc):
            field1 = StringField(required=True, default='test_string')
            field2 = DateTimeField(default=datetime.now)
            field3 = UUIDField(default=uuid.uuid4)
        td = TD()
        result = td.to_mongo()
        assert result['field1'] == 'test_string'
        assert isinstance(result['field2'], datetime)
        assert isinstance(result['field3'], uuid.UUID)

    def test_jsonify(self):
        class TD(BaseDoc):
            field1 = StringField(required=True, default='test_string')
            field2 = DateTimeField(default=datetime.now)
            field3 = UUIDField(default=uuid.uuid4)
            field4 = ObjectIdField(default=ObjectId)
        td = TD()
        result = td.jsonify()
        assert result['field1'] == 'test_string'
        assert isinstance(result['field2'], string_types)
        assert isinstance(result['field3'], string_types)
        assert isinstance(result['field4'], string_types)

    def test_list_field_to_mongo(self):
        class TD(BaseDoc):
            field1 = ListField(IntField())
        td = TD(field1=[1, 2, 3])
        result = td.to_mongo()
        assert result['field1'], [1, 2 == 3]

    def test_list_field_from_mongo(self):
        class TD(BaseDoc):
            field1 = ListField(StringField())
        result = TD.from_mongo({'field1': ['a', 'b', 3]})
        assert result.field1, ['a', 'b', '3']

    def test_dict_field_to_mongo(self):
        class TD(BaseDoc):
            field1 = DictField(IntField())
        td = TD(field1={'a': 1, 'b': 1})
        result = td.to_mongo()
        assert result['field1'], {'a': 1, 'b': 1}

    def test_document_field_to_mongo(self):
        class ITD(BaseDoc):
            field1 = StringField(default='test_string')

        class TD(BaseDoc):
            field1 = DocumentField(ITD)
        td = TD(field1=ITD())
        result = td.to_mongo()
        assert result['field1'] == {'field1': 'test_string'}

    def test_list_of_document_field_to_mongo(self):
        class ITD(BaseDoc):
            field1 = StringField(default='ts')

        class TD(BaseDoc):
            field1 = ListField(DocumentField(ITD))
        td = TD(field1=[ITD(), ITD(), ITD()])
        result = td.to_mongo()
        self.assertEqual(
            result['field1'],
            [{'field1': 'ts'}, {'field1': 'ts'}, {'field1': 'ts'}])

    def test_dict_of_document_field_to_mongo(self):
        class ITD(BaseDoc):
            field1 = StringField(default='ts')

        class TD(BaseDoc):
            field1 = DictField(DocumentField(ITD))
        td = TD(field1={'a': ITD(), 'b': ITD()})
        result = td.to_mongo()
        self.assertEqual(
            result['field1'],
            {'a': {'field1': 'ts'}, 'b': {'field1': 'ts'}})

    def test_dict_from_mongo(self):
        class TD(BaseDoc):
            field1 = DictField(GenericField())
        result = TD.from_mongo({'field1': {'a': 'b'}})
        assert result.field1['a'] == 'b'

    def test_dict_of_document_field_jsonify(self):
        dt_now = datetime.now()
        dt_now_str = dt_now.strftime('%Y-%m-%dT%H:%M:%S')

        class ITD(BaseDoc):
            field1 = DateTimeField(default=dt_now)

        class TD(BaseDoc):
            field1 = DictField(DocumentField(ITD))

        td = TD(field1={'a': ITD(), 'b': ITD()})
        result = td.jsonify()
        self.assertEqual(
            result['field1'],
            {'a': {'field1': dt_now_str}, 'b': {'field1': dt_now_str}})

    def test_list_of_document_field_jsonify(self):
        dt_now = datetime.now()
        dt_now_str = dt_now.strftime('%Y-%m-%dT%H:%M:%S')

        class ITD(BaseDoc):
            field1 = DateTimeField(default=dt_now)

        class TD(BaseDoc):
            field1 = ListField(DocumentField(ITD))

        td = TD(field1=[ITD(), ITD()])
        result = td.jsonify()
        self.assertEqual(
            result['field1'], [{'field1': dt_now_str}, {'field1': dt_now_str}])

    def test_from_mongo(self):
        class TD(BaseDoc):
            field1 = StringField()
        td = TD.from_mongo({'field1': 'test_string'})
        assert td.field1 == 'test_string'

    def test_from_mongo_hierarchy(self):
        dt_now = datetime.now()

        class ITD(BaseDoc):
            field1 = DateTimeField(default=dt_now)

        class TD(BaseDoc):
            field1 = StringField()
            field2 = DocumentField(ITD)

        td = TD.from_mongo({
            'field1': 'test_string',
            'field2': {'field1': dt_now}})
        assert isinstance(td.field2, ITD)
        assert td.field2.field1 == dt_now

    def test_from_mongo_hierarchy_list(self):
        dt_now = datetime.now()

        class ITD(BaseDoc):
            field1 = DateTimeField(default=dt_now)

        class TD(BaseDoc):
            field1 = StringField()
            field2 = ListField(DocumentField(ITD))

        td = TD.from_mongo({
            'field1': 'test_string',
            'field2': [{'field1': dt_now}]})
        assert isinstance(td.field2, list)
        assert td.field2[0].field1 == dt_now

    def test_from_mongo_hierarchy_dict(self):
        dt_now = datetime.now()

        class ITD(BaseDoc):
            field1 = DateTimeField(default=dt_now)

        class TD(BaseDoc):
            field1 = StringField()
            field2 = DictField(DocumentField(ITD))

        td = TD.from_mongo({
            'field1': 'test_string',
            'field2': {'a': {'field1': dt_now}}})
        assert isinstance(td.field2, dict)
        assert td.field2['a'].field1 == dt_now

    def test_from_mongo_deep_hierarchy(self):
        dt_now = datetime.now()

        class InListTD(BaseDoc):
            field1 = DateTimeField(default=dt_now)

        class InDictTD(BaseDoc):
            field1 = DateTimeField(default=dt_now)
            field2 = ListField(DocumentField(InListTD))

        class TD(BaseDoc):
            field1 = StringField()
            field2 = DictField(DocumentField(InDictTD))

        td = TD.from_mongo({
            'field1': 'test_string',
            'field2': {
                'a': {
                    'field1': dt_now,
                    'field2': [{'field1': dt_now}]}}})
        assert isinstance(td.field2, dict)
        assert isinstance(td.field2['a'].field2, list)
        assert isinstance(td.field2['a'].field2[0], InListTD)
        assert td.field2['a'].field2[0].field1 == dt_now

    def test_copy_in_place(self):
        class ITD(BaseDoc):
            field11 = IntField()

        class TD(BaseDoc):
            field1 = StringField()
            field2 = IntField()
            field3 = DocumentField(ITD)

        itd1 = ITD(field11=11)
        itd2 = ITD(field11=12)
        td1 = TD(field1='a', field2=1, field3=itd1)
        td2 = TD(field1='b', field2=2, field3=itd2)
        td1.copy_in_place(td2)

        # check source instance is unchanged
        assert td2.field1 == 'b'
        assert td2.field2 == 2
        assert td2.field3 is itd2

        # check target instance is a copy of source
        assert td1.field1 == td2.field1
        assert td1.field2 == td2.field2
        assert td1.field3.field11 == td2.field3.field11

    def test_required_fields_should_not_collide(self):
        class TD1(BaseDoc):
            field1 = StringField()
        td1 = TD1()
        td1.validate()
        assert True

    def test_required_fields_should_not_collide_in_one_cls(self):
        class TD(BaseDoc):
            field1 = StringField(required=True)
            field2 = StringField()
            field3 = StringField(required=True)

        td1 = TD(field1='test_string', field3='test_string')
        td1.validate()
        assert True


class TestRepository(unittest.TestCase):
    connection_manager = ConnectionManager()
    connection_manager.connect({'main': {
        'name': 'master_common_test',
        'args': ['127.0.0.1:27017']}})

    def setUp(self):
        class _TestModel(BaseDoc):
            field1 = StringField(required=True)
            field2 = DateTimeField(default=datetime.now)
            meta = {
                'indexes': [
                    {
                        'index': [('field2', 1), ],
                        'background': True,
                    },
                    {
                        'index': [('field1', 1), ('field2', -1), ],
                        'background': True,
                        'unique': True,
                    }
                ]
            }

        class _TestModelRepository(Repository):
            _model_class = _TestModel
        self._TestModel = _TestModel
        self._TestModelRepository = _TestModelRepository
        self._TestModelRepository.remove()
        self._get_collection().drop_indexes()

    def _get_collection(self):
        return self._TestModelRepository.get_collection()

    def create_doc(self, payload):
        return self._TestModelRepository._model_class(**payload)

    def test_save_validate_ok(self):
        md = self._TestModel(field1='asasddsd')
        self._TestModelRepository.save(md)
        assert hasattr(md, '_id')

    def test_right_collection_should_be_used(self):
        col_name = self._TestModelRepository._model_class.collection_name
        assert col_name == 'testmodel'

    def test_save_validate_failed(self):
        md = self._TestModel()
        self.assertRaises(ValidationError, self._TestModelRepository.save, md)

    def test_create(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        count = self._TestModelRepository.count()
        assert count == 1
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        count = self._TestModelRepository.count()
        assert count == 2

    def test_save_existing_doc(self):
        document = self._TestModelRepository.create({
            'field1': 'test_string'
        })
        document.field1 = 'test_string2'
        count = self._TestModelRepository.count()
        assert count == 1
        self._TestModelRepository.save(document)
        count = self._TestModelRepository.count()
        assert count == 1
        assert document.field1 == 'test_string2'
        stored_doc = self._TestModelRepository.get(document.id)
        assert stored_doc.field1 == 'test_string2'

    def test_insert(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        count = self._TestModelRepository.count()
        assert count == 1

        self._TestModelRepository.insert(self.create_doc({
            'field1': 'test_string'}))
        assert self._TestModelRepository.count() == 2

        self._TestModelRepository.insert([
            self.create_doc({'field1': 'test_string2'}),
            self.create_doc({'field1': 'test_string3'}),
        ])
        assert self._TestModelRepository.count() == 4

    def test_find(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        result = self._TestModelRepository.find().as_list()
        assert len(result) == 3

    def test_find_with_projection(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        result = self._TestModelRepository.find(fields={'field2': 1})
        assert hasattr(result[0], 'field2')
        self.assertIsNone(result[0].field1)

    def test_find_slice(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        result = self._TestModelRepository.find(skip=1, limit=2).as_list()
        assert len(result) == 2

    def test_slice(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        result = self._TestModelRepository.find()
        assert len(result[1:3]) == 2

    def test_find_and_modify_found(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        result = self._TestModelRepository.find_and_modify({
            'field1': 'test_string'}, {'$set': {'field1': 'updated'}})
        assert result.field1 == 'test_string'
        object_in_db = self._TestModelRepository.get({'field1': 'updated'})
        assert object_in_db.field1 == 'updated'

    def test_find_and_modify_not_found(self):
        result = self._TestModelRepository.find_and_modify({
            'field1': 'test_string'}, {'$set': {'field1': 'updated'}})
        assert result is None

    def test_find_and_modify_new_true(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        result = self._TestModelRepository.find_and_modify({
            'field1': 'test_string'}, {'$set': {'field1': 'updated'}},
            new=True)
        assert result.field1 == 'updated'
        object_in_db = self._TestModelRepository.get({'field1': 'updated'})
        assert object_in_db.field1 == 'updated'

    def test_remove_by_doc(self):
        document = self._TestModelRepository.create({
            'field1': 'test_string'
        })
        count = self._TestModelRepository.count()
        assert count == 1
        self._TestModelRepository.remove(document.id)
        count = self._TestModelRepository.count()
        assert count == 0

    def test_remove_by_id(self):
        document = self._TestModelRepository.create({
            'field1': 'test_string'
        })
        count = self._TestModelRepository.count()
        assert count == 1
        self._TestModelRepository.remove(document)
        count = self._TestModelRepository.count()
        assert count == 0

    def test_remove_by_spec(self):
        self._TestModelRepository.create({
            'field1': 'test_string'
        })
        self._TestModelRepository.create({
            'field1': 'test_string2'
        })
        count = self._TestModelRepository.count()
        assert count == 2
        self._TestModelRepository.remove({'field1': 'test_string'})
        count = self._TestModelRepository.count()
        assert count == 1

    def test_synchronization_index(self):
        def get_index_infos():
            indexes = self._get_collection().index_information()
            indexes.pop('_id_')
            return tuple(
                (tuple(indexes[i]['key']),
                 indexes[i].get('background', False),
                 indexes[i].get('expireAfterSeconds'))
                for i in indexes.keys()
            )

        def get_indexes():
            indexes = self._TestModel.meta['indexes']
            return tuple(
                (tuple(i['index']),
                 i['background'],
                 i.get('expire_after_seconds'))
                for i in indexes
            )

        def get_indexes_to_change():
            indexes = self._TestModelRepository.synchronize_indexes()
            return (tuple(indexes['indexes_to_drop']),
                    tuple(indexes['indexes_to_ensure']))

        change = get_indexes_to_change()
        expected = ((), ('field2_1', 'field1_1_field2_-1'))
        to_set = lambda tpls: tuple(set(tpl) for tpl in tpls)

        self.assertEqual(
            to_set(change),
            to_set(expected)
        )
        assert set(get_index_infos()) == set(get_indexes())

        self._get_collection().drop_index(
            'field1_1_field2_-1')
        change = get_indexes_to_change()
        assert change, ((), ('field1_1_field2_-1', ))

        assert set(get_index_infos()) == set(get_indexes())

        self._TestModel.meta['indexes'][1]['index'][0] = ('field13', -1)
        self._TestModel.meta['indexes'][1]['background'] = False
        change = get_indexes_to_change()
        assert change == (('field1_1_field2_-1',), ('field13_-1_field2_-1',))
        assert set(get_index_infos()) == set(get_indexes())

    def test_ttl(self):
        self._TestModelRepository.synchronize_indexes()

        self._TestModel.field1 = DateTimeField
        self._TestModel.meta['indexes'][1]['expire_after_seconds'] = 1
        try:
            self._TestModelRepository.synchronize_indexes()
            raise Exception
        except Exception as ex:
            assert type(ex) == TypeError

        self._TestModel.meta['indexes'][1].pop('expire_after_seconds')
        self._TestModelRepository.synchronize_indexes()

        self._TestModel.meta['indexes'][0]['expire_after_seconds'] = '1'
        try:
            self._TestModelRepository.synchronize_indexes()
            raise Exception
        except Exception as ex:
            assert type(ex) == TypeError

        self._TestModel.meta['indexes'][0]['expire_after_seconds'] = 1
        self._TestModel.field2 = StringField

        try:
            self._TestModelRepository.synchronize_indexes()
            raise Exception
        except Exception as ex:
            assert type(ex) == TypeError

    def test_unique(self):
        self._TestModelRepository.synchronize_indexes()

        dt = datetime.utcnow()
        dt2 = dt + timedelta(days=1)

        self._TestModelRepository.create(dict(
            field1='1', field2=dt
        ))
        self._TestModelRepository.create(dict(
            field1='2', field2=dt
        ))
        self._TestModelRepository.create(dict(
            field1='1', field2=dt2
        ))
        self._TestModelRepository.create(dict(
            field1='2', field2=dt2
        ))

        with pytest.raises(DuplicateKeyError):
            self._TestModelRepository.create(dict(
                field1='1', field2=dt
            ))

        self._TestModel.meta['indexes'][1]['unique'] = False
        self._TestModelRepository.synchronize_indexes()

        self._TestModelRepository.create(dict(
            field1='3', field2=dt
        ))

        self._TestModelRepository.create(dict(
            field1='3', field2=dt
        ))

    def test_create_document(self):

        class ITD(BaseDoc):
            int_field = IntField()
            string_field = StringField()

        class TD(BaseDoc):
            dict_field = DictField(DocumentField(ITD))
            list_field = ListField(DocumentField(ITD))
            model_field = DocumentField(ITD)

        class TDRepository(Repository):
            _model_class = TD

        td = TDRepository.create({
            'dict_field': {'1': {'int_field': 1, 'string_field': '2'}},
            'list_field': [{'int_field': 3, 'string_field': '4'}],
            'model_field': {'int_field': 5, 'string_field': '6'}
        })

        assert isinstance(td.dict_field['1'], ITD)
        assert isinstance(td.list_field[0], ITD)
        assert isinstance(td.model_field, ITD)


class TestInjectRepository(unittest.TestCase):

    def setUp(self):
        class _TestModel(BaseDoc):
            field1 = StringField(required=True)
            field2 = DateTimeField(default=datetime.now)

        class _TestModelRepository(Repository):
            _model_class = _TestModel

        class _TestModelRepository2(Repository):
            _model_class = _TestModel

        self._TestModel = _TestModel
        self._TestModelRepository = _TestModelRepository
        self._TestModelRepository2 = _TestModelRepository2

    def test_injected(self):
        inject_repositories([self._TestModelRepository])
        assert self._TestModel.objects == self._TestModelRepository

    def test_reinjected(self):
        inject_repositories([self._TestModelRepository])
        inject_repositories([self._TestModelRepository2])
        assert self._TestModel.objects == self._TestModelRepository2


class TestURLField(unittest.TestCase):

    def setUp(self):
        self.field = URLField()

    def test_validate_wrong_urls(self):
        wrong_urls = [
            'aaaaaaaa',
            'some.url.domain',
            '://somestring'
        ]
        for url in wrong_urls:
            self.assertRaises(ValidationError, self.field.validate, url)

    def test_validate_regular_url(self):
        valid_urls = [
            'http://some.domain.ru',
            'ftp://some.domain.ru',
            'http://some.domain.ru:8080',
            'http://some.domain.ru:8011/path',
            'http://some.domain.ru:9001/path&param1=value1',
        ]
        for url in valid_urls:
            self.assertIsNone(self.field.validate(url))


class TestHTTPField(unittest.TestCase):

    def setUp(self):
        self.field = HTTPField()

    def test_validate_wrong_urls(self):
        wrong_urls = [
            'aaaaaaaa',
            'some.url.domain',
            '://somestring',
            'ftp://some.domain.ru',
            'ftps://some.domain.ru'
        ]
        for url in wrong_urls:
            self.assertRaises(ValidationError, self.field.validate, url)

    def test_validate_regular_url(self):
        valid_urls = [
            'http://some.domain.ru',
            'https://some.domain.ru:8080',
            'http://some.domain.ru:8011/path',
            'https://some.domain.ru:9001/path&param1=value1',
        ]
        for url in valid_urls:
            self.assertIsNone(self.field.validate(url))


class TestDecimalField(unittest.TestCase):
    def setUp(self):
        self.field = DecimalField()

    def test_should_convert_to_string_when_storing(self):
        value = self.field.to_mongo(Decimal('3.14'))
        assert value == '3.14'
        value = self.field.to_mongo(Decimal('3.1415926535897932384'))
        assert value == '3.1415926535897932384'

    def test_should_restore_to_decimal(self):
        value = self.field.from_mongo('3.14')
        assert value == Decimal('3.14')
        value = self.field.from_mongo('3.1415926535897932384')
        assert value == Decimal('3.1415926535897932384')

    def test_should_accept_only_decimal(self):
        with pytest.raises(ValidationError):
            self.field.validate(10)


class TestInheritance(unittest.TestCase):
    def setUp(self):
        class TD1(BaseDoc):
            required_field1 = StringField(required=True)
            default_field1 = StringField(default='df1')
            choices_field1 = StringField(choices=ContactStatus)
            simple_field1 = StringField()

        class TD2(BaseDoc):
            required_field2 = StringField(required=True)
            default_field2 = StringField(default='df2')
            choices_field2 = StringField(choices=ContactStatus)
            simple_field2 = StringField()

        class TD3(TD1, TD2):
            required_field3 = StringField(required=True)
            default_field3 = StringField(default='df3')
            choices_field3 = StringField(choices=ContactStatus)
            simple_field3 = StringField()

        class TD3Repository(Repository):
            _model_class = TD3

        self.TD3 = TD3
        self.TD3Repository = TD3Repository
        self.TD3Repository.remove()
        self.TD3Repository.get_collection().drop_indexes()

    def test_correct_save_required_fields(self):
        self.TD3Repository.create({
            'required_field1': 'test_string1',
            'required_field2': 'test_string2',
            'required_field3': 'test_string3'
        })
        td = self.TD3Repository.get()
        assert td.required_field1 == 'test_string1'
        assert td.required_field2 == 'test_string2'
        assert td.required_field3 == 'test_string3'

    def test_incorrect_save_required_fields(self):
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({})
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({'required_field1': 'test_string1'})
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({
                'required_field1': 'test_string1',
                'required_field2': 'test_string2'})
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({
                'required_field2': 'test_string2',
                'required_field3': 'test_string3'})
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({
                'required_field1': 'test_string1',
                'required_field3': 'test_string3'})

    def test_correct_save_default_value_to_default_fields(self):
        self.TD3Repository.create({
            'required_field1': 'test_string1',
            'required_field2': 'test_string2',
            'required_field3': 'test_string3'
        })
        td = self.TD3Repository.get()
        assert td.default_field1 == 'df1'
        assert td.default_field2 == 'df2'
        assert td.default_field3 == 'df3'

    def test_correct_save_random_value_to_default_fields(self):
        self.TD3Repository.create({
            'required_field1': 'test_string1',
            'required_field2': 'test_string2',
            'required_field3': 'test_string3',
            'default_field1': 'ty1',
            'default_field2': 'ty2',
            'default_field3': 'ty3',
        })
        td = self.TD3Repository.get()
        assert td.default_field1 == 'ty1'
        assert td.default_field2 == 'ty2'
        assert td.default_field3 == 'ty3'

    def test_correct_save_choices_fields(self):
        self.TD3Repository.create({
            'required_field1': 'test_string1',
            'required_field2': 'test_string2',
            'required_field3': 'test_string3',
            'choices_field1': ContactStatus.Done,
            'choices_field2': ContactStatus.Failed,
            'choices_field3': ContactStatus.Awaiting,
        })
        td = self.TD3Repository.get()
        assert td.choices_field1 == ContactStatus.Done
        assert td.choices_field2 == ContactStatus.Failed
        assert td.choices_field3 == ContactStatus.Awaiting

    def test_incorrect_save_choices_fields(self):
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({
                'required_field1': 'test_string1',
                'required_field2': 'test_string2',
                'required_field3': 'test_string3',
                'choices_field1': 'r3',
                'choices_field2': ContactStatus.Failed,
                'choices_field3': ContactStatus.Awaiting,
            })
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({
                'required_field1': 'test_string1',
                'required_field2': 'test_string2',
                'required_field3': 'test_string3',
                'choices_field1': ContactStatus.Failed,
                'choices_field2': '43',
                'choices_field3': ContactStatus.Awaiting,
            })
        with self.assertRaises(ValidationError):
            self.TD3Repository.create({
                'required_field1': 'test_string1',
                'required_field2': 'test_string2',
                'required_field3': 'test_string3',
                'choices_field1': ContactStatus.Failed,
                'choices_field2': ContactStatus.Failed,
                'choices_field3': '43',
            })

    def test_correct_save_simple_fields(self):
        self.TD3Repository.create({
            'required_field1': 'test_string1',
            'required_field2': 'test_string2',
            'required_field3': 'test_string3',
            'simple_field1': 'ty1',
            'simple_field2': 'ty2',
            'simple_field3': 'ty3',
        })
        td = self.TD3Repository.get()
        assert td.simple_field1 == 'ty1'
        assert td.simple_field2 == 'ty2'
        assert td.simple_field3 == 'ty3'


class TestEmailValidator(unittest.TestCase):
    def setUp(self):
        self.validator = EmailValidator()

    def test_valid_emails(self):
        valid_emails = [
            'some@email.r',
            '"Abc\@def"@example.com',
            '"Fred Bloggs"@example.com',
            '"Joe\\Blow"@example.com',
            '"Abc@def"@example.com',
            'customer/department=shipping@example.com',
            '\$A12345@example.com',
            '!def!xyz%abc@example.com',
            '_somename@example.com',
            'test+label@example.com',
            'mimimi@президент.рф'
        ]
        for email in valid_emails:
            assert self.validator.validate(email)

    def test_invalid_emails(self):
        invalid_emails = ['email.r', '&&&abir@valg']
        for email in invalid_emails:
            assert not self.validator.validate(email)
