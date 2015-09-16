import unittest

from ognom.fields import DocumentField
from ognom import _registry
from tests.common import BaseDoc


class TestDocumentField(unittest.TestCase):

    def test_should_accept_document_class_as_model_class(self):
        field = DocumentField(BaseDoc)
        assert field.model_class == BaseDoc

    def test_should_accept_string_as_model_class(self):
        field = DocumentField('tests.common.BaseDoc')
        assert field.model_class == BaseDoc


class TestDocumentFieldDynamicLoad(unittest.TestCase):

    def setUp(self):
        self.documents_registry_orig = _registry.documents_registry
        _registry.documents_registry = {}

    def tearDown(self):
        _registry.documents_registry = self.documents_registry_orig

    def test_should_load_model_class_dynamically(self):
        field = DocumentField('tests.common.BaseDoc')
        assert field.model_class == BaseDoc