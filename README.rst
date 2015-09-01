ognom
=====

.. image:: https://travis-ci.org/lorehov/ognom.png
    :target: https://travis-ci.org/lorehov/ognom
    :alt: Build Status
    
Documentation:  #TODO

Ognom is object-to-document mapper for `mongodb <https://www.mongodb.org>`_. Currently ognom uses `pymongo <https://api.mongodb.org/python/current/>`_ as default backend, 
but you can easily implement your own backend based on another driver (for example `asyncio-mongo <https://pypi.python.org/pypi/asyncio_mongo>`_) if needed, 
as serialization and storage logic in ognom are separated.
 
Supports python2.6+, python3.3+, PyPy. 

Documentation:  #TODO


Install
-------

.. code-block:: python

    pip install ognom

Tests
-----

.. code-block:: python

    tox

Features
--------

- Vanilla python, no dependencies;
- From version 1.0 exposes full pymongo API(!);
- Easy to write your own backend with your own API.

Quickstart
----------

.. code-block:: python

    from ognom.base import (ConnectionManager, Document, Repository, 
        inject_repositories)
    from ognom.fields import StringField, IntField
    
    ConnectionManager.connect({
        'main': {  # ognom use aliases for databases to make it possible to use multiple db's per project
            'name': 'birzha_main',
            'args': ['127.0.0.1:27017'],
            'kwargs': {'socketTimeoutMS': 60000}},})


    class Foo(Document):
        db_name = 'main'  # db alias
        _collection_name = 'my_foos'  # collection name (by default 'foos')
    
        meta = {
            'indexes': [
                {
                    'index': [('bar', 1), ('baz', -1)],
                    'background': True,
                },
            ]
        }
        
        bar = StringField(required=True, default='baaar')
        baz = IntField(choices=[10, 20, 30, 40, 50])

    class FooRep(Repository):
        _model_class = Foo
        
    inject_repositories([FooRep])  # weird part, will be removed soon


    foo1 = Foo.objects.create({'bar': 'lalala'})
    assert Foo.objects.get({'bar': 'lalala'}).id == foo1.id
    
    foo2 = Foo(bar='lololo', baz=10)
    assert foo2.id is None
    foo2.save()
    assert foo2.id is not None
    foo2.remove()
    
    foos = Foo.objects.find({'bar': 'lalala'})  # not list but CursorWrapper!
    assert len(list(foos)) == 1
    
