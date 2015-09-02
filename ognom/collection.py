# coding: utf8
from collections import namedtuple

from bson import ObjectId
from bson.errors import InvalidId
from pymongo.cursor import Cursor
from pymongo.errors import ConnectionFailure
from six import string_types

from ognom.fields import DateTimeField
from ognom.connection import ConnectionManager
from ognom.document import Document

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

