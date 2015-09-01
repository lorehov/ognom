import re


class AbstractValidator(object):
    MESSAGE = u'Abstract message'

    def validate(self, value):
        raise NotImplementedError(
            u'validate method must be implemented for the class {}'.format(
                self.__class__))


class LengthValidator(AbstractValidator):
    MESSAGE = u'Length of value for field "{field}" must be lte then {value}'

    def __init__(self, length):
        self.length = length

    def validate(self, value):
        return len(value) <= self.length


class EmailValidator(AbstractValidator):
    MESSAGE = u'Value of the field "{field}" is not valid email: {value}'
    _EMAIL_REGEXP = re.compile(r'.+@.+\..+$', re.IGNORECASE)

    def validate(self, value):
        return self._EMAIL_REGEXP.match(value) is not None


class TimeValidator(AbstractValidator):
    MESSAGE = u'Value of the field "{field}" is not valid "hh:mm": {value}'
    _TIME_REGEXP = re.compile(r'^(\d{1,2}):(\d{1,2})$', re.IGNORECASE)

    def validate(self, value):
        result = self._TIME_REGEXP.match(value)
        if result is not None:
            hours, minutes = map(int, result.group(1, 2))
            if hours == 24 and minutes == 0:  # special case to define full day
                return True
            else:
                return 0 <= hours <= 23 and 0 <= minutes <= 59
        else:
            return False
