from enum import Enum, auto
from abc import ABC, abstractmethod


class SystemFields(Enum):
    Top = auto()
    NanAsZero = auto()
    QueryTransformation = auto()
    Stack = auto()
    Aggr = auto()
    All = auto()
    Range = auto()
    LegendFormat = auto()
    SensorStackOverride = auto()
    Axis = auto()
    LeftAxis = auto()
    RightAxis = auto()


ContainerTemplate = "{{container}}"
SensorTemplate = "{{sensor}}"


class NotEquals():
    def __init__(self, value):
        self.value = value


class Taggable(ABC):
    """ Interface for applying tags to generic items.

    Implementations must implement |value| method describing how exactly the
    tags should be applied. This interface provides several handy shortcuts
    that expand to a number of actual tags.

    |aggr| and |all| methods expand to "label=-" and "label=*" tags respectively.
    They expect a list of labels and, possibly, an abbreviation string of kind
    "#ABCD". See supported abbreviations in |ABBREVIATIONS| map.
    """

    ABBREVIATIONS = {
        "B": "tablet_cell_bundle",
        "H": "host",
        "U": "user",
        "A": "account",
    }

    @abstractmethod
    def value(self, key, value):
        raise NotImplementedError()

    def aggr(self, *args):
        ret = self
        for arg in ret._unpack_abbrev(args):
            ret = ret.value(arg, SystemFields.Aggr)
        return ret

    def all(self, *args):
        ret = self
        for arg in ret._unpack_abbrev(args):
            ret = ret.value(arg, SystemFields.All)
        return ret

    def top(self, limit=10, aggregation="max"):
        return self.value(SystemFields.Top, (limit, aggregation))

    def nan_as_zero(self, value=True):
        return self.value(SystemFields.NanAsZero, value)

    def stack(self, value=True):
        return self.value(SystemFields.Stack, value)

    def sensor_stack(self, value=True):
        return self.value(SystemFields.SensorStackOverride, value)

    def axis(self, value):
        assert value in (SystemFields.LeftAxis, SystemFields.RightAxis)
        return self.value(SystemFields.Axis, value)

    def query_transformation(self, transformation):
        return self.value(SystemFields.QueryTransformation, transformation)

    def range(self, min, max, axis=SystemFields.LeftAxis):
        assert axis in (SystemFields.LeftAxis, SystemFields.RightAxis)
        return self.value(SystemFields.Range, (min, max, axis))

    def min(self, min):
        return self.range(min, None)

    def max(self, max):
        return self.range(None, max)

    def legend_format(self, value):
        return self.value(SystemFields.LegendFormat, value)

    def container_legend_format(self):
        return self.value(SystemFields.LegendFormat, ContainerTemplate)

    def sensor_legend_format(self):
        return self.value(SystemFields.LegendFormat, SensorTemplate)

    def _unpack_abbrev(self, args):
        result = []
        for arg in args:
            if type(arg) is str and arg.startswith("#"):
                for c in arg[1:]:
                    if c in self.ABBREVIATIONS:
                        result.append(self.ABBREVIATIONS[c])
                    else:
                        raise RuntimeError('Unknown abbreviation "{}"'.format(c))
            else:
                result.append(arg)
        return result
