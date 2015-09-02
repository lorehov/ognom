# coding: utf8
import copy
import logging
from time import sleep
from collections import namedtuple

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import MongoReplicaSetClient, MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import ConnectionFailure
from six import string_types, with_metaclass

from ognom.fields import (
    GenericField, ObjectIdField, DateTimeField, ValidationError)

LOG_ERRORS = True
MAX_ATTEMPTS = 20
CONNECT_SLEEP_TIME = 1

_IndexSpec = namedtuple(
    '_IndexSpec', [
        'name', 'spec', 'background', 'unique', 'expire_after_seconds'])


class CursorWrapper(object):
    def __init__(self, cursor, serialize=None):
        self.cursor = cursor
        self.serialize = serialize

    def __getitem__(self, item):
        item_or_slice = self.cursor[item]
        if not self.serialize:
            return item_or_slice

        if isinstance(item_or_slice, Cursor):
            return [self.serialize(i) for i in item_or_slice]

        return self.serialize(item_or_slice)

    def __getattr__(self, item):
        return getattr(self.cursor, item)

    def __iter__(self):
        return (
            self.serialize(r) if self.serialize else r
            for r in self.cursor
        )

    def skip(self, count):
        self.cursor.skip(count)
        return self

    def limit(self, count):
        self.cursor.limit(count)
        return self

    def next(self):
        next_val = next(self.cursor)
        if self.serialize:
            next_val = self.serialize(next_val)
        return next_val

    def as_list(self):
        return list(self)


class ConnectionManager(object):
    databases = {}
    connections = {}

    @classmethod
    def connect(cls, mongo_settings):
        for db_alias, settings in mongo_settings.items():
            db_name = settings['name']
            connection = cls._get_connection(settings)
            cls.connections[db_alias] = connection
            cls.databases[db_alias] = getattr(connection, db_name)

    @classmethod
    def get_database(cls, db_alias):
        return cls.databases.get(db_alias)

    @classmethod
    def get_databases(cls):
        return cls.databases.items()

    @classmethod
    def get_connections(cls):
        return cls.connections.items()

    @classmethod
    def drop_database(cls, db_alias):
        connection = cls.databases[db_alias]
        cls.connections[db_alias].drop_database(connection.name)

    @classmethod
    def _get_connection(cls, configuration):
        if isinstance(configuration, string_types):
            connection = cls._establish_connection(configuration)
        elif isinstance(configuration, list):
            connection = cls._establish_connection(*configuration)
        elif isinstance(configuration, dict):
            connection = cls._establish_connection(
                *configuration.get('args', []),
                **configuration.get('kwargs', {}))
        else:
            raise ValueError(
                'Unsupported configuration format: %s' % configuration)
        return connection

    @classmethod
    def _establish_connection(cls, *args, **kwargs):
        for i in range(MAX_ATTEMPTS - 1):
            try:
                if 'replicaSet' in kwargs:
                    return MongoReplicaSetClient(*args, **kwargs)
                else:
                    return MongoClient(*args, **kwargs)
            except ConnectionFailure as e:
                if LOG_ERRORS:
                    logging.error(
                        'Fail to connect to %s, %s [%s]', args, kwargs, e)
                sleep(CONNECT_SLEEP_TIME)
        if 'replicaSet' in kwargs:
            return MongoReplicaSetClient(*args, **kwargs)
        else:
            return MongoClient(*args, **kwargs)


class MongoDocumentMeta(type):
    def __new__(cls, class_name, bases, dct):
        result_cls = type.__new__(cls, class_name, bases, dct)
        result_cls._defaults = {}
        result_cls._choices = {}
        result_cls._required = set()

        # Сделано для реализации наследования
        special_attrs = [
            ("_defaults", {}), ("_choices", {}), ("_required", set())]
        for field, default in special_attrs:
            # Копируем у родителей значения специфических полей документа
            for base in bases:
                base_field_value = getattr(base, field, None)
                if base_field_value is not None:
                    current_field_value = getattr(result_cls, field)
                    if isinstance(base_field_value, dict):
                        current_field_value.update(base_field_value)
                    elif isinstance(base_field_value, set):
                        current_field_value |= base_field_value
                    else:
                        raise NotImplementedError(field)

        for name, value in result_cls.__dict__.items():
            if isinstance(value, GenericField):
                if not value.name:
                    value.name = name
                if value.choices:
                    result_cls._choices[name] = set(value.choices)
                if value.required:
                    result_cls._required.add(name)
                if value.default is not None:
                    result_cls._defaults[name] = value.default
        result_cls.collection_name = class_name.lower()

        col_name = result_cls._collection_name
        if col_name and result_cls.collection_name != col_name:
            result_cls.collection_name = col_name
        return result_cls


class Document(with_metaclass(MongoDocumentMeta, object)):
    _id = ObjectIdField()

    _required = None
    _defaults = None
    _choices = None

    db_name = None
    _collection_name = None
    collection_name = None
    meta = {}

    objects = None  # repository can be injected here

    def __new__(cls, *args, **kwargs):
        instance = super(Document, cls).__new__(cls)
        instance._data = {}
        instance.apply_defaults()
        return instance

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    @property
    def id(self):
        return self._id

    def apply_defaults(self):
        for key, value in self._defaults.items():
            if key not in self._data:
                if callable(value):
                    self._data[key] = value()
                else:
                    self._data[key] = value

    def validate(self):
        for required_field in self._required:
            if self._data.get(required_field) is None:
                raise ValidationError(
                    u"Field {} is missing".format(required_field),
                    required_field)

        for name, choices in self._choices.items():
            if (name in self._data and
                    self._data[name] is not None and
                    self._data[name] not in choices):
                raise ValidationError(
                    u"Field {} value {} is not included in {}".format(
                        name, self._data[name], choices))

        for name, value in self._data.items():
            if value is None and name not in self._required:
                continue

            attribute = self.__class__.__dict__.get(name)
            if attribute:
                attribute.validate(value)

    def to_mongo(self):
        result = {}
        for name, value in self._data.items():
            if value is None:
                continue

            attribute = getattr(self.__class__, name, None)
            if attribute:
                result[name] = attribute.to_mongo(value)
            elif name == '_id' and value:
                result[name] = value
        return result

    def _get_prepared_data(self):
        return self._data

    def jsonify(self):
        result = {}
        for name, value in self._get_prepared_data().items():
            if value is None:
                continue
            attribute = self.__class__.__dict__.get(name)
            if attribute and hasattr(attribute, 'jsonify'):
                result[name] = attribute.jsonify(value)
            elif name == '_id' and value:
                result['id'] = str(value)
            else:
                result[name] = value
        return result

    @classmethod
    def from_mongo(cls, payload):
        instance = None
        if payload:
            instance = cls()
            for key, value in payload.items():
                attribute = getattr(instance.__class__, key, None)
                if attribute and hasattr(attribute, 'from_mongo'):
                    setattr(instance, key, attribute.from_mongo(value))
                elif key == '_id' and value:
                    setattr(instance, key, value)
        return instance

    @classmethod
    def from_json(cls, payload):
        payload_ = {}
        if payload:
            for key, value in payload.items():
                attribute = cls.__dict__.get(key)
                if attribute and isinstance(attribute, GenericField):
                    try:
                        payload_[key] = attribute.from_json(value)
                    except (ValueError, TypeError) as ex:
                        raise ValidationError(repr(ex), key)
                elif key in ('id', '_id') and value:
                    payload_['_id'] = ObjectId(value)
        instance = cls(**payload_)
        return instance

    def copy_in_place(self, instance):
        self._data = copy.deepcopy(instance._data)

    def save(self):
        if self.objects:
            return self.objects.save(self)
        raise NotImplemented(u'objects attribute was not specified')

    def remove(self):
        if self.objects:
            return self.objects.remove(self)
        raise NotImplemented(u'objects attribute was not specified')

    def copy(self):
        data = copy.deepcopy(self._data)
        data.pop("_id")
        return self.__class__(**data)

    def __repr__(self):
        return u'{self.__class__.__name__}:{self.id}'.format(self=self)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        if self.id is None:  # can't compare models without id's
            return False
        return self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.id is None:
            raise TypeError('Documents without id are unhashable')
        return hash(self.id)

    class DoesNotExist(Exception):
        pass


class BaseRepository(object):
    _model_class = None
    collection = None

    @classmethod
    def serialize(cls, result):
        if isinstance(result, (tuple, list)):
            return [cls._model_class.from_mongo(v) for v in result]
        return cls._model_class.from_mongo(result)

    @classmethod
    def get_collection(cls):
        if not cls.collection:
            connection = ConnectionManager.get_database(
                cls._model_class.db_name)
            if not connection:
                raise ConnectionFailure(
                    u"No connection for {}".format(cls._model_class.db_name))
            cls.collection = getattr(
                connection, cls._model_class.collection_name)
        return cls.collection

    @classmethod
    def get_model_class(cls):
        return cls._model_class


class Repository(BaseRepository):
    # CRUD
    @classmethod
    def find(cls, spec=None, fields=None, skip=None, limit=None, sort=None,
             as_dict=False, **kwargs):
        find_specs = {
            name: val
            for name, val
            in (('spec', spec), ('fields', fields), ('skip', skip),
                ('limit', limit), ('sort', sort))
            if val
        }
        find_specs.update(kwargs)
        result = cls.get_collection().find(**find_specs)
        if not as_dict:
            result = CursorWrapper(result, cls.serialize)
        return result

    @classmethod
    def get(cls, spec_or_id=None, fields=None):
        if spec_or_id and not isinstance(spec_or_id, dict):
            spec_or_id = ObjectId(spec_or_id)
        get_specs = {
            name: val for name, val
            in (('spec_or_id', spec_or_id), ('fields', fields))
            if val
        }
        return cls.serialize(cls.get_collection().find_one(**get_specs))

    @classmethod
    def get_or_raise(cls, spec_or_id=None, fields=None):
        try:
            result = cls.get(spec_or_id, fields)
        # TypeError is required because of bug in bson lib.
        # It throws TypeError when unicode is passed to ObjectId constructor.
        except (InvalidId, TypeError):
            raise cls._model_class.DoesNotExist()

        if not result:
            raise cls._model_class.DoesNotExist()

        return result

    @classmethod
    def count(cls, spec=None):
        collection = cls.get_collection()
        qs = collection.find(spec)
        count = qs.count()
        return count

    @classmethod
    def create(cls, payload):
        instance = cls._model_class(**payload)
        cls.save(instance)
        return instance

    @classmethod
    def update(cls, spec_or_id, document, multi=False, upsert=False, w=1):
        """
        Important! document here can be either instance of updated document,
        or a "document" in mongo update notation,
        for example {'$set': {'field1': 100}}.
        @param spec_or_id:
        @param document:
        @return:
        """
        is_doc = False
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': ObjectId(spec_or_id)}
        if isinstance(document, Document):
            document_as_dict = document.to_mongo()
            is_doc = True
        else:
            document_as_dict = document
        result = cls.get_collection().update(
            spec_or_id, document_as_dict, upsert=upsert, multi=multi, w=w)
        if is_doc and 'upserted' in result:
            document._id = result['upserted']
        return result

    @classmethod
    def save(cls, doc, w=1):
        doc.validate()
        result = cls.get_collection().save(doc.to_mongo(), w=w)
        if not doc._id:
            doc._id = result
        return result

    @classmethod
    def insert(cls, doc_or_docs, **kwargs):
        if isinstance(doc_or_docs, Document):
            doc_or_docs = [doc_or_docs]
        else:
            doc_or_docs = list(doc_or_docs)

        for doc in doc_or_docs:
            doc.validate()
        entities = [(doc, doc.to_mongo()) for doc in doc_or_docs]
        result = cls.get_collection().insert(
            [doc_as_dict for _, doc_as_dict in entities], **kwargs
        )
        if kwargs.get('manipulate') is not False:
            for doc, doc_as_dict in entities:
                doc._id = doc_as_dict['_id']
        return result

    @classmethod
    def find_and_modify(cls, spec_or_id, document, **kwargs):
        """
        @param spec_or_id: filter for updating (default is ``{}``)
        @param document: either instance of updated document, or a dict in
                mongo update notation, for example {'$set': {'field1': 100}}
        @param kwargs: all kwargs, which supported by
        ``pymongo.Collection.find_and_modify`` method:
            - `upsert`: insert if object doesn't exist (default ``False``)
            - `sort`: a list of (key, direction) pairs specifying the sort
              order for this query. See :meth:`~pymongo.cursor.Cursor.sort`
              for details.
            - `full_response`: return the entire response object from the
              server (default ``False``)
            - `remove`: remove rather than updating (default ``False``)
            - `new`: return updated rather than original object
              (default ``False``)
            - `fields`: see second argument to :meth:`find` (default all)
            - and also all generic pymongo kwargs as w, j and etc...
        @return: instance of some ``Document`` descendant
        """
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': ObjectId(spec_or_id)}
        if isinstance(document, Document):
            document_as_dict = document.to_mongo()
        else:
            document_as_dict = document
        result = cls.get_collection().find_and_modify(
            spec_or_id, document_as_dict, **kwargs)
        if kwargs.get('full_response'):
            return result
        return cls.serialize(result)

    @classmethod
    def remove(cls, spec_or_id=None, w=1):
        if isinstance(spec_or_id, Document):
            spec_or_id = spec_or_id._id
        if (spec_or_id and not isinstance(spec_or_id, dict) and
                isinstance(spec_or_id, string_types)):
            spec_or_id = ObjectId(spec_or_id)
        return cls.get_collection().remove(spec_or_id, w=w)

    @classmethod
    def aggregate(cls, pipeline, **kwargs):
        default_kwargs = {'cursor': {}}
        default_kwargs.update(kwargs)
        return cls.get_collection().aggregate(pipeline, **default_kwargs)

    @classmethod
    def generate_next_id(cls, field):
        result = cls.aggregate([{
            '$group':  {
                '_id': '0',
                'max': {'$max': u'${}'.format(field)}
            }
        }])

        result = result[0]['max'] if result else 0
        max_id = int(result) if result else 0
        return max_id + 1

    @classmethod
    def synchronize_indexes(cls):
        def generate_names(indexes):
            for index in indexes:
                index['name'] = '_'.join(
                    str(i) for j in index['index'] for i in j)
            return indexes

        def new_index_to_set(indexes):
            return set(
                _IndexSpec(
                    index['name'],
                    tuple(index['index']),
                    index.get('background', True),
                    index.get('unique', False),
                    index.get('expire_after_seconds')
                )
                for index in indexes
            )

        def old_index_to_set(indexes):
            return set(
                _IndexSpec(
                    index,
                    tuple(indexes[index]['key']),
                    indexes[index].get('background', True),
                    indexes[index].get('unique', False),
                    indexes[index].get('expireAfterSeconds')
                )
                for index in indexes.keys()
            )

        result = {'indexes_to_ensure': [], 'indexes_to_drop': []}

        if 'indexes' in cls._model_class.meta.keys():
            new_indexes = new_index_to_set(
                generate_names(cls._model_class.meta['indexes']))

            old_indexes = cls.get_collection().index_information()
            old_indexes.pop('_id_', None)
            old_indexes = old_index_to_set(old_indexes)

            indexes_to_ensure = new_indexes - old_indexes
            indexes_to_drop = old_indexes - new_indexes

            for index in indexes_to_drop:
                cls.get_collection().drop_index(index[0])
                result['indexes_to_drop'].append(index[0])

            for index in indexes_to_ensure:
                if index.expire_after_seconds:
                    field_name = index.spec[0][0]
                    if (len(index.spec) > 1 or
                            not isinstance(getattr(getattr(
                                cls, '_model_class'), field_name),
                                DateTimeField) or
                            not isinstance(index.expire_after_seconds, int)):
                        raise TypeError(
                            u"Incorrect expire_after_seconds "
                            u"assignment to collection")
                index_opts = {
                    'name': index.name,
                }
                if index.background:
                    index_opts['background'] = index.background
                if index.unique:
                    index_opts['unique'] = index.unique
                if index.expire_after_seconds:
                    index_opts['expireAfterSeconds'] = \
                        index.expire_after_seconds
                res_ind = cls.get_collection().ensure_index(
                    list(index.spec), **index_opts)
                result['indexes_to_ensure'].append(res_ind)
        return result


def inject_repositories(repositories):
    """
    Injects repositories into there _model_class as "objects" attribute.
    @param repositories:
    """
    for repository in repositories:
        repository.get_model_class().objects = repository
