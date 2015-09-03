from ognom.document import Document


class BaseDoc(Document):
    db_name = 'main'
    _collection_name = 'testmodel'
