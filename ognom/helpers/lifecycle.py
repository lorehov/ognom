from datetime import datetime

from ognom.base import Document
from ognom.fields import DateTimeField


class DocWithLifeCycle(Document):
    created_at = DateTimeField(default=datetime.utcnow)
    updated_at = DateTimeField()
    deleted_at = DateTimeField()
