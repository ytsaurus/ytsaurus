import yt.yson as yson

from .helpers import random_string
import random
import copy
import string
import functools

@functools.total_ordering
class ComparableYsonEntity(yson.YsonEntity):
    def __eq__(self, other):
        return super().__eq__(other)
    def __lt__(self, other):
        if other is None or isinstance(other, yson.YsonEntity):
            return False
        return True


def make_comparable_key(key):
    return [ComparableYsonEntity() if x is None else x for x in key]


class TInt64():
    def random(self):
        return yson.YsonInt64(random.randint(1, 1000000))
        return yson.YsonInt64(random.randint(-2**63, 2**63 - 1))
    def str(self):
        return "int64"
    def comparable(self):
        return True
    def aggregatable(self):
        #return ["sum", "min", "max"]
        return ["min", "max"]
    def aggregate(self, function, lhs, rhs):
        if function == "sum":
            r = lhs + rhs
            if r < -2**63:
                r += 2**64
            elif r > 2**63 - 1:
                r -= 2**64
            return r
        elif function == "max":
            return max(lhs, rhs)
        elif function == "min":
            return min(lhs, rhs)
    def data_weight(self):
        return 8
    def is_string_like(self):
        return False

class TUint64():
    def random(self):
        return yson.YsonUint64(random.randint(0, 2**64 - 1))
    def str(self):
        return "uint64"
    def comparable(self):
        return True
    def aggregatable(self):
        #return ["sum", "min", "max"]
        return ["min", "max"]
    def aggregate(self, function, lhs, rhs):
        if function == "sum":
            return (lhs + rhs) % 2**64
        elif function == "max":
            return max(lhs, rhs)
        elif function == "min":
            return min(lhs, rhs)
    def data_weight(self):
        return 8
    def is_string_like(self):
        return False

class TBoolean():
    def random(self):
        return yson.YsonBoolean(random.randint(0,1))
    def str(self):
        return "boolean"
    def comparable(self):
        return True
    def aggregatable(self):
        return None
    def data_weight(self):
        return 8
    def is_string_like(self):
        return False

class TDouble():
    def random(self):
        return yson.YsonDouble(random.uniform(-2**100,2**100))
    def str(self):
        return "double"
    def comparable(self):
        return True
    def aggregatable(self):
        return None
    def data_weight(self):
        return 8
    def is_string_like(self):
        return False

class RandomStringGenerator():
    def __init__(self):
        self.data = ""
        self.ptr = 0

    def generate(self, n):
        if self.ptr + n > len(self.data):
            self._refill()
        res = self.data[self.ptr:self.ptr + n]
        self.ptr += n
        return res

    def _refill(self):
        self.data = self.data[self.ptr:] + random_string(100000)
        self.ptr = 0

class TString():
    rsg = RandomStringGenerator()

    def random(self):
        length = random.randint(1,100)
        string = "start_{}_end".format(self.rsg.generate(length))
        return yson.YsonString(string.encode())
    def str(self):
        return "string"
    def comparable(self):
        return True
    def aggregatable(self):
        return ["min", "max"]
    def aggregate(self, function, lhs, rhs):
        if function == "max":
            return max(lhs, rhs)
        elif function == "min":
            return min(lhs, rhs)
    def data_weight(self):
        return (100 + 1) // 2 + 10 + 1
    def is_string_like(self):
        return True

class TAny():
    def __init__(self, scalar_types):
        self.scalar_types = scalar_types

    def random(self):
        if random.randint(0, len(self.scalar_types)) == 0:
            return [{},{}]
        return random.choice(self.scalar_types).random()
    def str(self):
        return "any"
    def comparable(self):
        return False
    def aggregatable(self):
        return None
    def data_weight(self):
        return 20
    def is_string_like(self):
        return True

scalar_types = [TInt64(), TUint64(), TBoolean(), TString()]
types = scalar_types + [TAny(scalar_types)]

key_types = [t for t in types if t.comparable()]
types_map = {t.str(): t for t in types + key_types}

class Column():
    def __init__(self, ttype, name, sort_order=None, aggregate=None, max_inline_hunk_size=None):
        self.type = ttype
        self.name = name
        self.sort_order = sort_order
        self.aggregate = aggregate
        if self.type.is_string_like() and random.randint(0, 1):
            self.max_inline_hunk_size = max_inline_hunk_size
        else:
            self.max_inline_hunk_size = None
    def yson(self):
        y = {"name": self.name, "type": self.type.str()}
        if self.sort_order:
            y["sort_order"] = self.sort_order
        if self.aggregate:
            y["aggregate"] = self.aggregate
        if self.max_inline_hunk_size is not None:
            y["max_inline_hunk_size"] = self.max_inline_hunk_size
        return y
    def generate_value(self):
        return self.type.random()
    def do_aggregate(self, lhs, rhs):
        if lhs is None:
            return rhs
        elif rhs is None:
            return lhs
        else:
            return self.type.aggregate(self.aggregate, lhs, rhs)
    def __repr__(self):
        return "Column" + str(self.yson())

class Schema():
    def __init__(self, sorted, spec):
        # QWFP
        self.appearance_probability = 0.95
        self.aggregate_probability = 0.5

        # XXX
        if spec.mode == "stateless_write":
            self.appearance_probability = 1

        if sorted:
            if spec.schema.key_column_count is not None:
                key_column_count = spec.schema.key_column_count
                assert key_column_count > 0
            else:
                key_column_count = random.randint(3, 5)
            max_inline_hunk_size = spec.sorted.max_inline_hunk_size
        else:
            key_column_count = 0
            max_inline_hunk_size = None

        if spec.schema.value_column_count is not None:
            value_column_count = spec.schema.value_column_count
            assert value_column_count > 0
        else:
            value_column_count = random.randint(5, 10)

        def _pick_columns(types, allowed_type_names, count):
            if allowed_type_names is not None:
                types = [t for t in types if t.str() in allowed_type_names]
            return [random.choice(types) for i in range(count)]

        key_columns = _pick_columns(key_types, spec.schema.key_column_types, key_column_count)
        data_columns = _pick_columns(types, spec.schema.value_column_types, value_column_count)
        key_names = ["k%s" % str(i) for i in range(len(key_columns))]
        data_names = ["v%s" % str(i) for i in range(len(data_columns))]

        self.key_columns = [Column(t, n, "ascending") for (t,n) in zip(key_columns, key_names)]

        def aggr(t):
            if not sorted or not spec.schema.allow_aggregates or not t.aggregatable():
                return None
            if random.random() < self.aggregate_probability:
                return random.choice(t.aggregatable())
            else:
                return None
        self.data_columns = [
            Column(t, n, None, aggr(t), max_inline_hunk_size)
            for (t,n) in zip(data_columns, data_names)]

        self.columns = self.key_columns + self.data_columns

    def from_yson(self, yson):
        self.key_columns = []
        self.data_columns = []
        for c in yson:
            column = Column(types_map[c["type"]], c["name"], c.get("sort_order", None), c.get("aggregate", None))
            if c.get("sort_order") == "ascending":
                self.key_columns.append(column)
            else:
                self.data_columns.append(column)
        self.columns = self.key_columns + self.data_columns

    def with_named_columns(self, names, types, sort_order=None):
        new_schema = copy.deepcopy(self)
        new_columns = [Column(type, name, sort_order) for name, type in zip(names, types)]
        if sort_order:
            new_schema.key_columns = new_columns + new_schema.key_columns
        else:
            new_schema.data_columns = new_columns + new_schema.data_columns
        new_schema.columns = new_schema.key_columns + new_schema.data_columns
        return new_schema

    def add_key_column(self):
        type = random.choice(key_types)
        name = "k{}".format(len(self.key_columns))
        self.key_columns.append(Column(type, name, "ascending"))
        self.columns = self.key_columns + self.data_columns

    def get_key_column_names(self):
        return [c.name for c in self.key_columns]
    def get_data_column_names(self):
        return [c.name for c in self.data_columns]
    def get_column_names(self):
        return [c.name for c in self.columns]
    def get_key_columns(self):
        return self.key_columns
    def get_data_columns(self):
        return self.data_columns
    def get_columns(self):
        return self.columns

    def create_pivot_keys(self, tablet_count):
        self.pivot_keys = self._generate_pivot_keys(tablet_count)
        return self.pivot_keys
    def create_pivot_keys_for_tablet_range(self, tablet_count, first_tablet_index, last_tablet_index):
        new_pivots = self._generate_pivot_keys(
            tablet_count,
            self.pivot_keys[first_tablet_index],
            None if last_tablet_index + 1 == len(self.pivot_keys) else self.pivot_keys[last_tablet_index + 1])
        self.pivot_keys[first_tablet_index:last_tablet_index + 1] = new_pivots
        return new_pivots
    def get_pivot_keys(self):
        return self.pivot_keys

    def yson(self):
        return [c.yson() for c in self.columns]

    def yson_keys(self):
        return [c.yson() for c in self.key_columns]

    def yson_with_unique(self):
        return yson.to_yson_type(self.yson(), attributes={"unique_keys": True})

    def yson_keys_with_unique(self):
        return yson.to_yson_type(self.yson_keys(), attributes={"unique_keys": True})

    def flatten_key(self, map_key):
        list_key = []
        for c in self.key_columns:
            list_key.append(map_key.get(c.name, None))
        return list_key

    def generate_key(self):
        return self.generate_row_from_schema(self.key_columns)
    def generate_data(self):
        return self.generate_row_from_schema(self.data_columns)
    def generate_row(self):
        return self.generate_row_from_schema(self.columns)
    def generate_row_from_schema(self, columns):
        while True:
            result = {c.name: c.generate_value() for c in columns if random.random() < self.appearance_probability}
            if len(result) > 0:
                return result

    def _generate_pivot_key(self):
        length = random.randint(1, len(self.key_columns))
        return [
            c.generate_value()
            if random.random() < self.appearance_probability
            else None
            for c in self.key_columns[:length]
        ]

    def _generate_pivot_keys(self, tablet_count, lower_limit=None, upper_limit=None):
        if lower_limit is None:
            lower_limit = []
        if tablet_count <= 1:
            return [lower_limit]

        def generate_acceptable_pivot_key():
            for i in range(10000):
                key = self._generate_pivot_key()
                comparable_key = make_comparable_key(key)
                if lower_limit is not None and comparable_key < make_comparable_key(lower_limit):
                    continue
                if upper_limit is not None and comparable_key >= make_comparable_key(upper_limit):
                    continue
                return key
            else:
                raise RuntimeError(
                        "Failed to generate acceptable pivot key (LowerLimit: {}, UpperLimit: {}".format(
                            lower_limit, upper_limit))

        pivots = [generate_acceptable_pivot_key() for i in range(tablet_count - 1)]
        pivots = sorted(pivots, key=make_comparable_key)
        unique_pivots = [pivots[0]]
        for pivot in pivots[1:]:
            if pivot != unique_pivots[-1]:
                unique_pivots.append(pivot)
        if unique_pivots[0] == lower_limit:
            unique_pivots = unique_pivots[1:]
        return [lower_limit] + unique_pivots

    def data_weight(self):
        return sum(column.type.data_weight() for column in self.columns)
    def key_data_weight(self):
        return sum(column.type.data_weight() for column in self.key_columns)
