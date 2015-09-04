# coding: utf8
from __future__ import unicode_literals
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


class Collection(object):
    def __init__(self, db_name, collection_name=None, indexes=None):
        """
        Collection class. Stores/retrieves objects from database.
        :param db_name: name of the database;
        :param collection_name: collection name, if empty will be
            populated with pluaralyzed class name;
        :param indexes: list of indexes.
        """
        self.db_name = db_name
        self.collection_name = collection_name
        if indexes is None:
            indexes = []
        self.indexes = indexes

        # will be populated later
        self.model_class = None
        self._collection = None

    def serialize(self, result):
        if isinstance(result, (tuple, list)):
            return [self.model_class.from_mongo(v) for v in result]
        return self.model_class.from_mongo(result)

    @property
    def collection(self):
        if not self.collection_name:
            self.collection_name = '{}'.format(
                self.model_class.__name__.lower())

        if not self._collection:
            db = ConnectionManager.get_database(self.db_name)
            if not db:
                raise ConnectionFailure(
                    'No connection for {}'.format(self.db_name))
            self._collection = getattr(db, self.collection_name)
        return self._collection

    # CRUD
    def find(self, spec=None, fields=None, skip=None, limit=None, sort=None,
             as_dict=False, **kwargs):
        find_specs = {
            name: val
            for name, val
            in (('spec', spec), ('fields', fields), ('skip', skip),
                ('limit', limit), ('sort', sort))
            if val
        }
        find_specs.update(kwargs)
        result = self.collection.find(**find_specs)
        if not as_dict:
            result = CursorWrapper(result, self.serialize)
        return result

    def get(self, spec_or_id=None, fields=None):
        if spec_or_id and not isinstance(spec_or_id, dict):
            spec_or_id = ObjectId(spec_or_id)
        get_specs = {
            name: val for name, val
            in (('spec_or_id', spec_or_id), ('fields', fields))
            if val
        }
        return self.serialize(self.collection.find_one(**get_specs))

    def get_or_raise(self, spec_or_id=None, fields=None):
        try:
            result = self.get(spec_or_id, fields)
        # TypeError is required because of bug in bson lib.
        # It throws TypeError when unicode is passed to ObjectId constructor.
        except (InvalidId, TypeError):
            raise self.model_class.DoesNotExist()

        if not result:
            raise self.model_class.DoesNotExist()

        return result

    def count(self, spec=None):
        qs = self.collection.find(spec)
        count = qs.count()
        return count

    def create(self, payload):
        instance = self.model_class(**payload)
        self.save(instance)
        return instance

    def update(self, spec_or_id, document, multi=False, upsert=False, w=1):
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
        result = self.collection.update(
            spec_or_id, document_as_dict, upsert=upsert, multi=multi, w=w)
        if is_doc and 'upserted' in result:
            document._id = result['upserted']
        return result

    def save(self, doc, w=1):
        doc.validate()
        result = self.collection.save(doc.to_mongo(), w=w)
        if not doc._id:
            doc._id = result
        return result

    def insert(self, doc_or_docs, **kwargs):
        if isinstance(doc_or_docs, Document):
            doc_or_docs = [doc_or_docs]
        else:
            doc_or_docs = list(doc_or_docs)

        for doc in doc_or_docs:
            doc.validate()
        entities = [(doc, doc.to_mongo()) for doc in doc_or_docs]
        result = self.collection.insert(
            [doc_as_dict for _, doc_as_dict in entities], **kwargs
        )
        if kwargs.get('manipulate') is not False:
            for doc, doc_as_dict in entities:
                doc._id = doc_as_dict['_id']
        return result

    def find_and_modify(self, spec_or_id, document, **kwargs):
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
        result = self.collection.find_and_modify(
            spec_or_id, document_as_dict, **kwargs)
        if kwargs.get('full_response'):
            return result
        return self.serialize(result)

    def remove(self, spec_or_id=None, w=1):
        if isinstance(spec_or_id, Document):
            spec_or_id = spec_or_id._id
        if (spec_or_id and not isinstance(spec_or_id, dict) and
                isinstance(spec_or_id, string_types)):
            spec_or_id = ObjectId(spec_or_id)
        return self.collection.remove(spec_or_id, w=w)

    def aggregate(self, pipeline, **kwargs):
        default_kwargs = {'cursor': {}}
        default_kwargs.update(kwargs)
        return self.collection.aggregate(pipeline, **default_kwargs)

    def generate_next_id(self, field):
        result = self.aggregate([{
            '$group':  {
                '_id': '0',
                'max': {'$max': u'${}'.format(field)}
            }
        }])

        result = result[0]['max'] if result else 0
        max_id = int(result) if result else 0
        return max_id + 1

    def synchronize_indexes(self):
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

        if self.indexes:
            new_indexes = new_index_to_set(generate_names(self.indexes))

            old_indexes = self.collection.index_information()
            old_indexes.pop('_id_', None)
            old_indexes = old_index_to_set(old_indexes)

            indexes_to_ensure = new_indexes - old_indexes
            indexes_to_drop = old_indexes - new_indexes

            for index in indexes_to_drop:
                self.collection.drop_index(index[0])
                result['indexes_to_drop'].append(index[0])

            for index in indexes_to_ensure:
                if index.expire_after_seconds:
                    field_name = index.spec[0][0]
                    if (len(index.spec) > 1 or
                            not isinstance(getattr(getattr(
                                self, 'model_class'), field_name),
                                DateTimeField) or
                            not isinstance(index.expire_after_seconds, int)):
                        raise TypeError(
                            'Incorrect expire_after_seconds '
                            'assignment to collection')
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
                res_ind = self.collection.ensure_index(
                    list(index.spec), **index_opts)
                result['indexes_to_ensure'].append(res_ind)
        return result
