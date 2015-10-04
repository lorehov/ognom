from __future__ import unicode_literals
import copy

from bson import ObjectId
from six import with_metaclass

from ognom.fields import GenericField, ObjectIdField, ValidationError
from ognom._registry import register_doc_class


class ObjectDoesNotExist(Exception):
    pass


class MongoDocumentMeta(type):
    def __new__(cls, class_name, bases, dct):
        result_cls = type.__new__(cls, class_name, bases, dct)
        register_doc_class(result_cls)
        if result_cls.objects is not None:
            result_cls.objects.model_class = result_cls

        result_cls._defaults = {}
        result_cls._choices = {}
        result_cls._required = set()
        result_cls.DoesNotExist = type(
            str('DoesNotExist'), (ObjectDoesNotExist,), {})

        # in order to support inheritance
        special_attrs = [
            ('_defaults', {}), ('_choices', {}), ('_required', set())]
        for field, default in special_attrs:
            # copy values of specific fields from parent
            for base in bases:
                base_field_value = getattr(base, field, None)
                if base_field_value is not None:
                    current_field_value = getattr(result_cls, field)
                    if isinstance(base_field_value, dict):
                        current_field_value.update(base_field_value)
                    elif isinstance(base_field_value, set):
                        current_field_value |= base_field_value
                    else:
                        raise NotImplementedError(field)

        for name, value in result_cls.__dict__.items():
            if isinstance(value, GenericField):
                if not value.name:
                    value.name = name
                if value.choices:
                    result_cls._choices[name] = set(value.choices)
                if value.required:
                    result_cls._required.add(name)
                if value.default is not None:
                    result_cls._defaults[name] = value.default
        return result_cls


class Document(with_metaclass(MongoDocumentMeta, object)):
    _id = ObjectIdField()

    _required = None
    _defaults = None
    _choices = None
    DoesNotExist = None

    objects = None  # repository can be injected here

    def __new__(cls, *args, **kwargs):
        instance = super(Document, cls).__new__(cls)
        instance._data = {}
        instance.apply_defaults()
        return instance

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    @property
    def id(self):
        return self._id

    def apply_defaults(self):
        for key, value in self._defaults.items():
            if key not in self._data:
                if callable(value):
                    self._data[key] = value()
                else:
                    self._data[key] = value

    def validate(self):
        for required_field in self._required:
            if self._data.get(required_field) is None:
                raise ValidationError(
                    'Field {} is missing'.format(required_field),
                    required_field)

        for name, choices in self._choices.items():
            if (name in self._data and
                    self._data[name] is not None and
                    self._data[name] not in choices):
                raise ValidationError(
                    'Field {} value {} is not included in {}'.format(
                        name, self._data[name], choices))

        for name, value in self._data.items():
            if value is None and name not in self._required:
                continue

            attribute = self.__class__.__dict__.get(name)
            if attribute:
                attribute.validate(value)

    def to_mongo(self):
        result = {}
        for name, value in self._data.items():
            if value is None:
                continue

            attribute = getattr(self.__class__, name, None)
            if attribute:
                result[name] = attribute.to_mongo(value)
            elif name == '_id' and value:
                result[name] = value
        return result

    def _get_prepared_data(self):
        return self._data

    def jsonify(self):
        result = {}
        for name, value in self._get_prepared_data().items():
            if value is None:
                continue
            attribute = self.__class__.__dict__.get(name)
            if attribute and hasattr(attribute, 'jsonify'):
                result[name] = attribute.jsonify(value)
            elif name == '_id' and value:
                result['id'] = str(value)
            else:
                result[name] = value
        return result

    @classmethod
    def from_mongo(cls, payload):
        if payload is None:
            return None
        instance = cls()
        for key, value in payload.items():
            attribute = getattr(instance.__class__, key, None)
            if attribute and hasattr(attribute, 'from_mongo'):
                setattr(instance, key, attribute.from_mongo(value))
            elif key == '_id' and value:
                setattr(instance, key, value)
        return instance

    @classmethod
    def from_json(cls, payload):
        payload_ = {}
        if payload:
            for key, value in payload.items():
                attribute = cls.__dict__.get(key)
                if attribute and isinstance(attribute, GenericField):
                    try:
                        payload_[key] = attribute.from_json(value)
                    except (ValueError, TypeError) as ex:
                        raise ValidationError(repr(ex), key)
                elif key in ('id', '_id') and value:
                    payload_['_id'] = ObjectId(value)
        instance = cls(**payload_)
        return instance

    def copy_in_place(self, instance):
        self._data = copy.deepcopy(instance._data)

    def save(self):
        if self.objects:
            return self.objects.save(self)
        raise NotImplemented('objects attribute was not specified')

    def remove(self):
        if self.objects:
            return self.objects.remove(self)
        raise NotImplemented('objects attribute was not specified')

    def copy(self):
        data = copy.deepcopy(self._data)
        data.pop('_id')
        return self.__class__(**data)

    def __repr__(self):
        return '{self.__class__.__name__}:{self.id}'.format(self=self)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        if self.id is None:  # can't compare models without id's
            return False
        return self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.id is None:
            raise TypeError('Documents without id are unhashable')
        return hash(self.id)
