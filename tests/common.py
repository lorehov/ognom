from ognom.document import Document
from ognom.collection import Collection


class BaseDoc(Document):
    objects = Collection(db_name='main', collection_name='test_models')
