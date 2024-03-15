from ..taggable import Taggable
from ..specific_tags.tags import SpecificTag

from copy import deepcopy
from enum import Enum, auto


class MonitoringSystemFields(Enum):
    DownsamplingAggregation = auto()


class DownsamplingAggregation:
    Max = "GRID_AGGREGATION_MAX"
    Min = "GRID_AGGREGATION_MIN"
    Sum = "GRID_AGGREGATION_SUM"
    Avg = "GRID_AGGREGATION_AVG"
    Last = "GRID_AGGREGATION_LAST"
    Count = "GRID_AGGREGATION_COUNT"


class MonitoringExpr(Taggable):
    class NodeType(Enum):
        Terminal = auto()
        BinaryOp = auto()
        Func = auto()

    def __repr__(self):
        return f"Expr: {self.node_type}{self.args}"

    def __init__(self, *args):
        if type(args[0]) is self.NodeType:
            self.node_type, self.args = args[0], args[1:]
        else:
            self.node_type, self.args = self.NodeType.Terminal, args

    def __add__(self, x):
        return self.binary_op("+", self, x)

    def __radd__(self, x):
        return self.binary_op("+", x, self)

    def __sub__(self, x):
        return self.binary_op("-", self, x)

    def __rsub__(self, x):
        return self.binary_op("-", x, self)

    def __mul__(self, x):
        return self.binary_op("*", self, x)

    def __rmul__(self, x):
        return self.binary_op("*", x, self)

    def __truediv__(self, x):
        return self.binary_op("/", self, x)

    def __rtruediv__(self, x):
        return self.binary_op("/", x, self)

    @classmethod
    def func(cls, func, *args):
        return MonitoringExpr(cls.NodeType.Func, func, *args)

    @classmethod
    def binary_op(cls, op, lhs, rhs):
        return MonitoringExpr(cls.NodeType.BinaryOp, op, lhs, rhs)

    def alias(self, param):
        return self.func("alias", self, f'"{param}"')

    def moving_avg(self, param):
        return self.func("moving_avg", self, param)

    def moving_sum(self, param):
        return self.func("moving_sum", self, param)

    def linear_trend(self, from_ts, to_ts):
        return self.func("linear_trend", self, from_ts, to_ts)

    def drop_below(self, value):
        return self.func("drop_below", self, value)

    def group_by_labels(self, label, expr):
        return self.func("group_by_labels", self, f'"{label}"', expr)

    def series_avg(self, label):
        return self.func("series_avg", f'"{label}"', self)

    def series_sum(self, label):
        return self.func("series_sum", f'"{label}"', self)

    def series_min(self, label):
        return self.func("series_min", f'"{label}"', self)

    def series_max(self, *labels):
        labels = [f'"{label}"' for label in labels]
        return self.func("series_max", *labels, self)

    def top_avg(self, k):
        return self.func("top_avg", k, self)

    def top_max(self, k):
        return self.func("top_max", k, self)

    def top_min(self, k):
        return self.func("top_min", k, self)

    def bottom_min(self, k):
        return self.func("bottom_min", k, self)

    def downsampling_aggregation(self, value):
        return self.value(MonitoringSystemFields.DownsamplingAggregation, value)

    def drop_nan(self):
        return self.func("drop_nan", self)

    def replace_nan(self, k):
        return self.func("replace_nan", self, k)

    def sqrt(self):
        return self.func("sqrt", self)

    def value(self, key, value, overwrite=True):
        return MonitoringExpr(
            self.node_type,
            *[arg.value(key, value, overwrite) if hasattr(arg, "value") else arg for arg in self.args])

    @staticmethod
    def _serialize_arg(arg, default_serializer):
        if hasattr(arg, "serialize"):
            return arg.serialize(default_serializer)
        else:
            return default_serializer(arg)

    def serialize(self, default_serializer=lambda x: str(x)):
        if self.node_type == self.NodeType.Terminal:
            arg, = self.args
            return self._serialize_arg(arg, default_serializer)
        elif self.node_type == self.NodeType.BinaryOp:
            op, args = self.args[0], list(self.args[1:])
            for i in range(2):
                serialized = self._serialize_arg(args[i], default_serializer)
                if issubclass(type(args[i]), MonitoringExpr) and args[i].node_type == self.NodeType.BinaryOp:
                    serialized = f'({serialized})'
                args[i] = serialized
            return f"{args[0]} {op} {args[1]}"
        elif self.node_type == self.NodeType.Func:
            args = [self._serialize_arg(arg, default_serializer) for arg in self.args[1:]]
            joined_args = ", ".join(args)
            return f'{self.args[0]}({joined_args})'
        else:
            assert False

    def get_tags(self):
        result = {}
        for s in self.args:
            if hasattr(s, "get_tags"):
                result.update(s.get_tags())
        return result


class PlainMonitoringExpr(MonitoringExpr):
    def __init__(self, s):
        super().__init__(self.NodeType.Terminal, self)
        self.tags = {}
        self.expr = s

    def value(self, key, value, overwrite=True):
        if not overwrite and key in self.tags:
            return self
        ret = deepcopy(self)
        ret.tags[key] = value
        return ret

    def serialize(self, default_serializer=None):
        expr = self.expr
        for k, v in self.tags.items():
            if type(k) in (str, PlainExprVariable):
                expr = expr.replace(f"{{{k}}}", str(v))
        return expr

    def get_tags(self):
        return self.tags


PlainExprVariable = SpecificTag.make_new("PlainExprVariable")
