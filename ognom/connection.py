from __future__ import unicode_literals
import logging
from time import sleep

from six import string_types
from pymongo import MongoReplicaSetClient, MongoClient
from pymongo.errors import ConnectionFailure


MAX_ATTEMPTS = 20
CONNECT_SLEEP_TIME = 1


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
                logging.error(
                    'Fail to connect to %s, %s [%s]', args, kwargs, e)
                sleep(CONNECT_SLEEP_TIME)
        if 'replicaSet' in kwargs:
            return MongoReplicaSetClient(*args, **kwargs)
        else:
            return MongoClient(*args, **kwargs)
