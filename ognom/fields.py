from __future__ import unicode_literals
import re
import sys
import uuid
import datetime
import decimal

import six
from bson import ObjectId
from dateutil import parser

from ognom._registry import get_doc_class


class ValidationError(Exception):
    def __init__(self, message, field_name=None, *args, **kwargs):
        super(ValidationError, self).__init__(message)
        self.message = message
        self.field_name = field_name


class GenericField(object):
    def __init__(self, required=False, default=None, choices=None,
                 validators=None):
        self.required = required
        self.default = default
        self.name = None
        self.choices = choices
        self.validators = validators if validators else []

    def __get__(self, instance, owner):
        if not instance:
            return self
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        instance._data[self.name] = self.prepare_to_assign(value)

    def prepare_to_assign(self, value):
        return value

    def validate(self, value):
        if value:
            for validator in self.validators:
                if not validator.validate(value):
                    raise ValidationError(validator.MESSAGE.format(
                        field=self.name, value=value), self.name)

    def to_mongo(self, value):
        return value

    def jsonify(self, value):
        return value

    def from_mongo(self, value):
        return value

    def from_json(self, value):
        """
        Most of the fields use BSON compatible types,
        so we only need to cast string to UUID/ObjectId/DateTime.
        We need smart casting only in case of Complex user defined types.
        """
        return self.to_mongo(value)


class StringField(GenericField):
    def validate(self, value):
        super(StringField, self).validate(value)
        if value and not isinstance(value, six.string_types):
            raise ValidationError(
                '[{}]: String accepts only string values, '
                '{} of {} given!'.format(
                    self.name, value, type(value)), self.name)

    def jsonify(self, value):
        return value


class URLField(StringField):
    _URL_REGEX = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|'
        r'[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    def validate(self, value):
        super(URLField, self).validate(value)
        if value and not self._URL_REGEX.match(value):
            raise ValidationError(
                '[{}] Invalid url {}'.format(self.name, value), self.name)


class HTTPField(URLField):
    _URL_REGEX = re.compile(
        r'^(?:http)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|'
        r'[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)


class IntField(GenericField):
    """An 32-bit integer field."""
    def validate(self, value):
        super(IntField, self).validate(value)
        if value is not None and not isinstance(value, six.integer_types):
            raise ValidationError(
                '[{}] Couldn\'t convert {} to int'.format(
                    self.name, value), self.name)

    def to_mongo(self, value):
        if value is not None:
            return int(value)
        return value


class FloatField(GenericField):
    def validate(self, value):
        super(FloatField, self).validate(value)
        if not isinstance(value, (float, decimal.Decimal, six.integer_types)):
            raise ValidationError(
                '[{}] Can\'t convert {} of type {} to float'.format(
                    self.name, value, type(value)), self.name)

    def to_mongo(self, value):
        return float(value)


class DecimalField(GenericField):
    def validate(self, value):
        super(DecimalField, self).validate(value)
        try:
            decimal.Decimal(value)
        except decimal.InvalidOperation:
            raise ValidationError(
                '[{}] Can\'t convert {} of type {} to Decimal'.format(
                    self.name, value, type(value)), self.name)

    def to_mongo(self, value):
        return six.text_type(value)

    def from_mongo(self, value):
        return decimal.Decimal(value)


class UUIDField(GenericField):
    def to_mongo(self, value):
        if isinstance(value, six.string_types):
            return uuid.UUID(value)
        return value

    def validate(self, value):
        super(UUIDField, self).validate(value)
        if not isinstance(value, uuid.UUID):
            raise ValidationError(
                '[{}] Cannot convert to UUID {}'.format(
                    self.name, value), self.name)

    def jsonify(self, value):
        return str(value)


class ObjectIdField(GenericField):
    def validate(self, value):
        super(ObjectIdField, self).validate(value)
        if not isinstance(value, ObjectId):
            raise ValidationError(
                '[{}] Invalid value for ObjectId {}'.format(
                    self.name, value), self.name)

    def to_mongo(self, value):
        if not isinstance(value, ObjectId):
            try:
                return ObjectId(six.text_type(value))
            except Exception as ex:
                raise ValidationError(
                    '[{}] Invalid value for ObjectId {}. Error: {}'.format(
                        self.name, value, repr(ex)), self.name)
        return value

    def jsonify(self, value):
        return str(value)


class DateTimeField(GenericField):
    def validate(self, value):
        super(DateTimeField, self).validate(value)
        if value and not isinstance(value, datetime.datetime):
            try:
                parser.parse(value)
            except Exception as ex:
                raise ValidationError(
                    '[{}] Unable to convert {} to datetime: {}'.format(
                        self.name, value, repr(ex)))

    def to_mongo(self, value):
        if value is None:
            return value
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)

        if not isinstance(value, six.string_types):
            return None

        try:
            return parser.parse(value)
        except ValueError as ex:
            raise ValidationError(
                '[{}] Unable to convert {} to datetime: {}'.format(
                    self.name, value, repr(ex)))

    def jsonify(self, value):
        return value.strftime('%Y-%m-%dT%H:%M:%S%z')


class BooleanField(GenericField):
    def validate(self, value):
        if not isinstance(value, bool):
            raise ValidationError(
                '[{}] Only bool value can be used'.format(self.name),
                self.name)


class ListField(GenericField):
    def __init__(self, field_type, required=False, default=None,
                 validators=None):
        super(ListField, self).__init__(
            required, default, validators=validators)
        self.field_type = field_type

    def to_mongo(self, value):
        return [self.field_type.to_mongo(item) for item in value]

    def validate(self, value):
        super(ListField, self).validate(value)
        try:
            iter(value)
        except TypeError:
            raise ValidationError(
                u"[{}] Iterable expected {} with value {} was found.".format(
                    self.name, type(value), value),
                self.name)
        for item in value:
            self.field_type.validate(item)

    def jsonify(self, value):
        return [self.field_type.jsonify(item) for item in value]

    def from_mongo(self, value):
        return [self.field_type.from_mongo(v) for v in value]

    def from_json(self, value):
        return [self.field_type.from_json(v) for v in value]

    def prepare_to_assign(self, value):
        if value is not None:
            value = [
                self.field_type.prepare_to_assign(v) for v in value
            ]
        return value


class DictField(GenericField):
    def __init__(self, field_type, required=False, default=None,
                 validators=None):
        super(DictField, self).__init__(
            required, default, validators=validators)
        self.field_type = field_type

    def to_mongo(self, value):
        return {key: self.field_type.to_mongo(item)
                for key, item in value.items()}

    def validate(self, value):
        super(DictField, self).validate(value)
        if value is None:
            value = {}
        if not isinstance(value, dict):
            raise ValidationError(
                u"[{}] Dict expected {} with value {} was found.".format(
                    self.name, type(value), value),
                self.name)
        for item in value.values():
            self.field_type.validate(item)

    def jsonify(self, value):
        return {key: self.field_type.jsonify(item)
                for key, item in value.items()}

    def from_mongo(self, value):
        return {
            k: self.field_type.from_mongo(v)
            for k, v in value.items()
        }

    def from_json(self, value):
        return {
            k: self.field_type.from_json(v)
            for k, v in value.items()
        }

    def prepare_to_assign(self, value):
        if value is not None:
            for key, item in value.items():
                value[key] = self.field_type.prepare_to_assign(item)
        return value


class DocumentField(GenericField):
    def __init__(self, model_class, required=False, default=None,
                 validators=None):
        super(DocumentField, self).__init__(
            required, default, validators=validators)
        if isinstance(model_class, six.string_types):
            self._model_class_name = model_class
            self._model_class = None
        else:
            self._model_class = model_class

    def validate(self, value):
        if not isinstance(value, self.model_class):
            try:
                value = self.model_class(**value)
            except Exception:
                err_msg = u"[{}] {} or dict are accepted, {} given".format(
                    self.name, self.model_class, value
                )
                six.reraise(ValidationError, err_msg, sys.exc_info()[2])
        value.validate()

    def to_mongo(self, value):
        if not isinstance(value, self.model_class):
            value = self.model_class(**value)
        return value.to_mongo()

    def jsonify(self, value):
        if not hasattr(value, 'jsonify'):
            if isinstance(value, dict):
                value = self.model_class(**value)
            else:
                raise ValidationError(
                    u'[{}] Wrong object type passed: {}'.format(
                        self.name, type(value)))
        return value.jsonify()

    def from_mongo(self, payload):
        if payload is None:
            return None
        return self.model_class(**payload)

    def from_json(self, value):
        return self.model_class.from_json(value)

    def prepare_to_assign(self, value):
        if value is not None and not isinstance(value, self.model_class):
            value = self.model_class(**value)
        return value

    @property
    def model_class(self):
        if self._model_class is None:
            self._model_class = get_doc_class(self._model_class_name)
        return self._model_class


class GenericDocumentField(GenericField):
    def __init__(self, attribute, attr_to_class, required=False, default=None):
        super(GenericDocumentField, self).__init__(required, default)
        self.attribute = attribute
        self.attr_to_class = attr_to_class

    def validate(self, value):
        possible_types = tuple(self.attr_to_class.values())
        if not isinstance(value, possible_types):
            value_class = self._get_class(value)
            try:
                value_obj = value_class(**value)
            except Exception:
                err_msg = u"[{}] {} or dict are accepted, {} given".format(
                    self.name, value_class, value
                )
                six.reraise(ValidationError, err_msg, sys.exc_info()[2])
        else:
            value_obj = value
        value_obj.validate()

    def to_mongo(self, value):
        possible_types = tuple(self.attr_to_class.values())
        if not isinstance(value, possible_types):
            value_class = self._get_class(value)
            value_obj = value_class(**value)
        else:
            value_obj = value
        return value_obj.to_mongo()

    def jsonify(self, value):
        if not hasattr(value, 'jsonify'):
            if isinstance(value, dict):
                value_class = self._get_class(value)
                value = value_class(**value)
            else:
                raise ValidationError(
                    u'[{}] Wrong object type was passed: {}'.format(
                        self.name, type(value)))
        return value.jsonify()

    def from_mongo(self, payload):
        if payload:
            value_class = self._get_class(payload)
            return value_class(**payload)
        return None

    def from_json(self, value):
        value_class = self._get_class(value)
        return value_class.from_json(value)

    def prepare_to_assign(self, value):
        possible_types = tuple(self.attr_to_class.values())
        if value is not None and not isinstance(value, possible_types):
            value_class = self._get_class(value)
            value = value_class(**value)
        return value

    def _get_class(self, value):
        attribute_value = value.get(self.attribute)
        value_class = self.attr_to_class.get(attribute_value)
        if not value_class:
            raise ValidationError(
                u'No class for {}:{}'.format(self.attribute, attribute_value))
        return value_class


class IdField(ObjectIdField):
    name = '_id'
