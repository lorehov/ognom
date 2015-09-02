from datetime import datetime

from ognom.base import Document
from ognom.fields import DateTimeField


class Document(Document):
    created_at = DateTimeField(default=datetime.utcnow)
    updated_at = DateTimeField(default=datetime.utcnow)
