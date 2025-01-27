from .taggable import Taggable

from copy import deepcopy


class Sensor(Taggable):
    """ Representation of a sensor.

    Sensor is defined with a name and a bunch of tags (see |Taggable|).

    Sensor can be constructed from a plain string or from a string with
    placeholders. In the latter case it becomes callable and behaves as
    str.format method, yielding actual sensors upon call.
    """

    def __init__(self, sensor=None, sensor_tag_name="sensor"):
        self.tags = {}

        if sensor is None:
            sensor = "{}"
        self.callable = "{}" in sensor
        self.sensor = sensor
        self.sensor_tag_name = sensor_tag_name

    def value(self, key, value, overwrite=True):
        if not overwrite and key in self.tags:
            return self
        ret = deepcopy(self)
        ret.tags[key] = value
        return ret

    def get_tags(self):
        assert not self.callable
        result = deepcopy(self.tags)
        result[self.sensor_tag_name] = self.sensor
        return result

    def __call__(self, *args, **kwargs):
        assert self.callable
        other = type(self)()
        other.callable = False
        other.sensor_tag_name = self.sensor_tag_name
        other.tags = deepcopy(self.tags)
        other.sensor = self.sensor.format(*args, **kwargs)
        return other


class MultiSensor(Taggable):
    @staticmethod
    def _is_callable(s):
        return getattr(s, "callable", False)

    def __init__(self, *sensors):
        self.sensors = list(sensors)
        self.callable = any(self._is_callable(s) for s in self.sensors)

    def value(self, key, value, overwrite=True):
        ret = deepcopy(self)
        for i, s in enumerate(ret.sensors):
            ret.sensors[i] = s.value(key, value, overwrite)
        return ret

    def get_tags(self):
        result = {}
        for s in self.sensors:
            result.update(s.get_tags())
        return result

    def __call__(self, *args, **kwargs):
        assert self.callable
        ret = deepcopy(self)
        for i, s in enumerate(ret.sensors):
            if self._is_callable(s):
                ret.sensors[i] = s(*args, **kwargs)
        return ret


class Text(Taggable):
    def __init__(self, text):
        self.text = text

    def value(self, *args, **kwargs):
        return self


class Title(Taggable):
    def __init__(self, title):
        self.title = title

    def value(self, *args, **kwargs):
        return self


class EmptyCell(Text):
    def __init__(self):
        super(EmptyCell, self).__init__(text="")
