import yt.yson as yson

from dataclasses import dataclass
import enum
import random

from typing import List, Tuple, Union, Any

################################################################################

INT = {"type": "int64"}
UINT = {"type": "uint64"}
BOOLEAN = {"type": "boolean"}
STRING = {"type": "string"}
DOUBLE = {"type": "double"}

SCALAR_TYPES = (INT, UINT, BOOLEAN, STRING, DOUBLE)

STRING_LENGTH = 12

ALPHABET = "abcdefghijklmnopqrstuvwxyz"

DEF_NULL_PROB = 0.05

TType = dict
TColumn = dict
TSchema = List[TColumn]
TTypes = Tuple[dict]
TRow = dict

################################################################################

class Grail:
    def drink(self) -> Any:
        assert False, "virtual method"

class NumericGrail(Grail):
    def __init__(self, min_v, max_v, cardinality: int, t: type):
        if issubclass(t, int):
            assert max_v - min_v > 3 * cardinality

        self.pool = set()
        while len(self.pool) < cardinality:
            self.pool.add(t(min_v + (max_v - min_v) * random.random()))

        self.pool = list(self.pool)

    def drink(self):
        return random.choice(self.pool)

class BooleanGrail(Grail):
    def __init__(self, true_weight=0.5, false_weight=0.5):
        self.true_chance = true_weight / (true_weight + false_weight)

    def drink(self):
        return random.random() < self.true_chance

class StringGrail(Grail):
    def __init__(self, seed: str = ALPHABET, min_len: int = 0, max_len: int = 50):
        self.seed = seed
        self.min_len = min_len
        self.max_len = max_len

    def drink(self):
        l = random.randint(self.min_len, self.max_len)
        return "".join(self.seed[random.randint(0, len(self.seed) - 1)] for _ in range(l))

class OptionalGrail(Grail):
    def __init__(self, grail: Grail, null_probability=DEF_NULL_PROB):
        self.null_probability = null_probability
        self.grail = grail

    def drink(self):
        return None if random.random() < self.null_probability else self.grail.drink()

class OptionalShelf():
    def __init__(self, null_probability=DEF_NULL_PROB):
        self.null_probability = null_probability

    def take(self, grail: Grail):
        return OptionalGrail(grail, self.null_probability)

class ListGrail(Grail):
    def __init__(self, grail: Grail, min_list_len:int=0, max_list_len:int=5):
        assert min_list_len <= max_list_len
        self.min_list_len = min_list_len
        self.max_list_len = max_list_len
        self.grail = grail

    def drink(self):
        tuple_len = random.randrange(self.min_list_len, self.max_list_len)
        return [self.grail.drink() for _ in range(tuple_len)]

class ListShelf():
    def __init__(self, min_list_len:int=0, max_list_len:int=5):
        assert min_list_len <= max_list_len
        self.min_list_len = min_list_len
        self.max_list_len = max_list_len

    def take(self, grail: Grail):
        return ListGrail(grail, )

################################################################################

DEFAULT_GRAILS = {
    "int64": NumericGrail(-2**63, 2**63, 1000, yson.YsonInt64),
    "uint64": NumericGrail(0, 2**64 - 1, 1000, yson.YsonUint64),
    "boolean": BooleanGrail(),
    "string": StringGrail(seed=ALPHABET, min_len=0, max_len=2),
    "double": NumericGrail(-100.0, 100.0, 1000, yson.YsonDouble),
}
DEFAULT_SHELVES = {
    "optional": OptionalShelf(),
    "list": ListShelf(),
}

class Treasury:
    def __init__(self, grails:dict=DEFAULT_GRAILS, shelves:dict=DEFAULT_SHELVES):
        self.grails = grails
        self.shelves = shelves

        assert len(grails.keys() & shelves.keys()) == 0

    def raid(self, type: TType) -> Grail:
        if "type_v3" in type:
            return self._get_from_v3(type["type_v3"])
        grail = self.grails[type["type"]]
        required = type.get("required", False)
        if not required:
            return self.shelves["optional"].take(grail)
        return grail

    def _get_from_v3(self, type: Union[TType, str]):
        if isinstance(type, str):
            return self.grails[type]
        type_name = type["type_name"]
        subgrail = self._get_from_v3(type["item"])
        return self.shelves[type_name].take(subgrail)

def get_scalar(type: TType) -> str:
    assert isinstance(type, dict), f"type is {type} here"
    if "type" in type:
        return type["type"]
    v3 = type["type_v3"]
    if isinstance(v3, str):
        return v3
    if v3["type_name"] == "optional":
        return v3["item"]
    return None

def is_compatible(typeA: TType, typeB: TType) -> bool:
    scalar = get_scalar(typeB)
    if get_scalar(typeA) != scalar:
        return False
    if scalar is not None:
        return True
    return typeA["type_v3"] == typeB["type_v3"]

################################################################################

@dataclass
class RegOptions:
    max_depth: int = 5
    forbid_throwing: bool = False
    forbidden_kinds: tuple = tuple()

################################################################################

class IExpression:
    def __init__(self):
        self.type: TType
        assert False, "abstract"
    def eval(self, row: TRow):
        assert False, "abstract"
    def __str__(self) -> str:
        assert False, "abstract"

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions) -> 'IExpression':
        assert False, "abstract"
    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        assert False, "abstract"

################################################################################

class Reference(IExpression):
    def __init__(self, column: TColumn):
        self.column = column
        self.type = {}
        if "type" in self.column:
            self.type["type"] = self.column["type"]
        if "type_v3" in self.column:
            self.type["type_v3"] = self.column["type_v3"]

    def __str__(self):
        return self.column["name"]

    def eval(self, row: TRow):
        return row.get(self.column["name"], None)

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions) -> 'Reference':
        candidates = tuple(col for col in schema if is_compatible(col, desired_type))
        assert candidates, f"Cannot form reference of the desired type {desired_type}"
        return Reference(random.choice(candidates))

    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        return tuple(schema)

################################################################################

class Literal(IExpression):
    def __init__(self, value, ty_pe: TType):
        self.value = value
        self.type = ty_pe
        if value is None:
            return
        if isinstance(value, bool):
            assert is_compatible(ty_pe, BOOLEAN),\
                f"type mismatch {value} does not represent {ty_pe}"
        elif isinstance(value, float):
            assert is_compatible(ty_pe, DOUBLE),\
                f"type mismatch {value} does not represent {ty_pe}"
        elif isinstance(value, str):
            assert is_compatible(ty_pe, STRING),\
                f"type mismatch {value} does not represent {ty_pe}"
        elif isinstance(value, int):
            assert is_compatible(ty_pe, INT) or is_compatible(ty_pe, UINT),\
                f"type mismatch value {value}, {type(value)} does not represent {ty_pe}"
        else:
            assert False, f"unsupported value {value} of {type(value)}"

    def __str__(self):
        if self.value is None:
            return "#"
        if is_compatible(self.type, UINT):
            return str(self.value) + "u"
        if is_compatible(self.type, STRING):
            return f"\"{self.value}\""
        return str(self.value)

    def eval(self, row: TRow):
        return self.value

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions):
        return Literal(Treasury().raid(desired_type).drink(), desired_type)

    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        return SCALAR_TYPES

################################################################################

class UnaryOp(enum.Enum):
    Plus = 1
    Minus = 2
    BitNot = 3
    Not = 4

class Unary(IExpression):
    def __init__(self, op: UnaryOp, operand: IExpression):
        scalar = get_scalar(operand.type)
        if op == UnaryOp.Plus or op == UnaryOp.Minus or op == UnaryOp.BitNot:
            assert scalar == "int64" or scalar == "uint64", f"got {scalar}"
        elif op == UnaryOp.Not:
            assert scalar == "boolean", f"got {scalar}"
        else:
            assert False, "unreachable"
        self.op = op
        self.operand = operand
        self.type = operand.type

    def __str__(self):
        if self.op == UnaryOp.Plus:
            return f"+({self.operand})"
        elif self.op == UnaryOp.Minus:
            return f"-({self.operand})"
        elif self.op == UnaryOp.BitNot:
            return f"~({self.operand})"
        elif self.op == UnaryOp.Not:
            return f"not ({self.operand})"
        else:
            assert False, "unreachable"

    def eval(self, row: TRow):
        op_eval = self.operand.eval(row)
        if op_eval is None:
            return None
        if self.op == UnaryOp.Plus:
            return op_eval
        elif self.op == UnaryOp.Minus:
            return -op_eval
        elif self.op == UnaryOp.BitNot:
            scalar = get_scalar(self.type)
            if scalar == "uint64":
                return 2**64 - op_eval - 1
            elif scalar == "int64":
                return - op_eval - 1
            else:
                assert False, "unreachable"
        elif self.op == UnaryOp.Not:
            return not op_eval
        else:
            assert False, "unreachable"

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions):
        operand = make_random_expression(schema, desired_type, depth + 1, options)
        op = UnaryOp.Not if is_compatible(desired_type, BOOLEAN) else random.choice([UnaryOp.BitNot, UnaryOp.Minus, UnaryOp.Plus])

        return Unary(op, operand)

    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        return (INT, UINT, BOOLEAN)

################################################################################

class BinaryOp(enum.Enum):
    Plus = 1
    Minus = 2
    Multiply = 3
    Divide = 4

    Modulo = 5
    # LeftShift = 6
    # RightShift = 7
    BitOr = 8
    BitAnd = 9

    And = 10
    Or = 11

    Equal = 12
    NotEqual = 13

    Less = 14
    LessOrEqual = 15
    Greater = 16
    GreaterOrEqual = 17

    Concatenate = 18

class Binary(IExpression):
    OP_TO_SIGN = {
        BinaryOp.Plus : "+",
        BinaryOp.Minus : "-",
        BinaryOp.Multiply : "*",
        BinaryOp.Divide : "/",
        BinaryOp.Modulo : "%",
        # BinaryOp.LeftShift : "<<",
        # BinaryOp.RightShift : ">>",
        BinaryOp.BitOr : "|",
        BinaryOp.BitAnd : "&",
        BinaryOp.And : "AND",
        BinaryOp.Or : "OR",
        BinaryOp.Equal : "=",
        BinaryOp.NotEqual : "!=",
        BinaryOp.Less : "<",
        BinaryOp.Greater : ">",
        BinaryOp.LessOrEqual : "<=",
        BinaryOp.GreaterOrEqual : ">=",
        BinaryOp.Concatenate : "||",
    }

    def __init__(self, op: BinaryOp, lhs: IExpression, rhs: IExpression):
        assert is_compatible(lhs.type, rhs.type)
        t = lhs.type
        if op in (
            BinaryOp.Plus,
            BinaryOp.Minus,
            BinaryOp.Multiply,
            BinaryOp.Divide,
        ):
            assert is_compatible(t, INT) or is_compatible(t, UINT) or is_compatible(t, DOUBLE)
            self.type = t
        elif op in (BinaryOp.And, BinaryOp.Or):
            assert is_compatible(t, BOOLEAN)
            self.type = BOOLEAN
        elif op in (BinaryOp.Modulo, BinaryOp.BitAnd, BinaryOp.BitOr,
            # BinaryOp.LeftShift, BinaryOp.RightShift
        ):
            assert is_compatible(t, INT) or is_compatible(t, UINT)
            self.type = t
        elif op in (BinaryOp.Equal, BinaryOp.NotEqual, BinaryOp.Less, BinaryOp.Greater, BinaryOp.LessOrEqual, BinaryOp.GreaterOrEqual):
            assert any([is_compatible(t, x) for x in SCALAR_TYPES])
            self.type = BOOLEAN
        elif op == BinaryOp.Concatenate:
            assert is_compatible(t, STRING)
            self.type = STRING
        else:
            assert False, "unreachable"
        self.op = op
        self.lhs = lhs
        self.rhs = rhs

    def __str__(self):
        return f"({self.lhs}) {Binary.OP_TO_SIGN[self.op]} ({self.rhs})"

    def eval_arithmetic(self, lhs, rhs):
        if lhs is None or rhs is None:
            return None
        res = None
        if self.op == BinaryOp.Plus:
            res = lhs + rhs
        elif self.op == BinaryOp.Minus:
            res = lhs - rhs
        elif self.op == BinaryOp.Multiply:
            res = lhs * rhs
        elif self.op == BinaryOp.Divide:
            if is_compatible(self.type, DOUBLE):
                res = lhs / rhs
            elif (lhs >= 0) != (rhs >= 0) and lhs % rhs:
                return lhs // rhs + 1
            else:
                return lhs // rhs
        else:
            assert False, "unreachable"

        if is_compatible(self.type, INT):
            res = res & 0xFFFFFFFFFFFFFFFF
            res -= 2 ** 63
        elif is_compatible(self.type, UINT):
            res = res & 0xFFFFFFFFFFFFFFFF
        else:
            assert False, "unreachable"

        return res

    def eval_logical(self, lhs, rhs):
        if lhs is None:
            lhs = False
        if rhs is None:
            rhs = False
        if self.op == BinaryOp.And:
            return lhs and rhs
        if self.op == BinaryOp.Or:
            return lhs or rhs
        assert False, "unreachable"

    def eval_equality(self, lhs, rhs):
        if lhs is None:
            if rhs is None:
                return self.op == BinaryOp.Equal
            else:
                return self.op != BinaryOp.Equal
        if rhs is None:
            return self.op != BinaryOp.Equal
        return (lhs == rhs) == (self.op == BinaryOp.Equal)


    def eval_relation(self, lhs, rhs):
        def eval_less(lhs, rhs):
            if lhs is None:
                return rhs is not None
            if rhs is None:
                return False
            return lhs < rhs
        if self.op == BinaryOp.Less:
            return eval_less(lhs, rhs)
        if self.op == BinaryOp.Greater:
            return eval_less(rhs, lhs)
        if self.op == BinaryOp.LessOrEqual:
            return not eval_less(rhs, lhs)
        if self.op == BinaryOp.GreaterOrEqual:
            return not eval_less(lhs, rhs)
        else:
            assert False, "unreachable"

    # def eval_shift(self, lhs, rhs):
    #     if lhs is None or rhs is None:
    #         return None
    #     rhs = rhs & 63
    #     if rhs == 0:
    #         return lhs
    #     negative = lhs < 0
    #     if negative:
    #         lhs += 2**63
    #         if self.op == BinaryOp.RightShift:
    #             lhs += 0xFFFFFFFFFFFFFFFF0000000000000000
    #     res = lhs << rhs
    #     res = res & 0xFFFFFFFFFFFFFFFF
    #     if self.type == TInt64 and res >= 2**63:
    #         res -= 2 ** 64
    #     return res

    def eval_bit_logic(self, lhs, rhs):
        if lhs is None or rhs is None:
            return None
        if self.op == BinaryOp.BitAnd:
            return lhs & rhs
        if self.op == BinaryOp.BitOr:
            return lhs | rhs
        assert False, "unreachable"

    def eval(self, row: TRow):
        lhs_eval = self.lhs.eval(row)
        rhs_eval = self.rhs.eval(row)
        if self.op in (
            BinaryOp.Plus,
            BinaryOp.Minus,
            BinaryOp.Divide,
            BinaryOp.Multiply,
        ):
            return self.eval_arithmetic(lhs_eval, rhs_eval)
        if self.op in (BinaryOp.And, BinaryOp.Or):
            return self.eval_logical(lhs_eval, rhs_eval)
        if self.op in (BinaryOp.Equal, BinaryOp.NotEqual):
            return self.eval_equality(lhs_eval, rhs_eval)
        if self.op in (BinaryOp.Less, BinaryOp.Greater, BinaryOp.LessOrEqual, BinaryOp.GreaterOrEqual):
            return self.eval_relation(lhs_eval, rhs_eval)
        # if self.op in (BinaryOp.LeftShift, BinaryOp.RightShift):
        #     return self.eval_shift(lhs_eval, rhs_eval)
        if self.op in (BinaryOp.BitAnd, BinaryOp.BitOr):
            return self.eval_bit_logic(lhs_eval, rhs_eval)
        if self.op == BinaryOp.Concatenate:
            if lhs_eval is None or rhs_eval is None:
                return None
            return lhs_eval + rhs_eval
        assert False, "unreachable"

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions):
        op = None
        lhs = None
        rhs = None
        arg_type = None
        if is_compatible(desired_type, BOOLEAN):
            fate = random.random()
            if fate < 0.33:
                op = random.choice([BinaryOp.And, BinaryOp.Or])
                arg_type = BOOLEAN
            elif fate < 0.66:
                op = random.choice([BinaryOp.Equal, BinaryOp.NotEqual])
                arg_type = random.choice(SCALAR_TYPES)
            else:
                op = random.choice([BinaryOp.Less, BinaryOp.Greater, BinaryOp.LessOrEqual, BinaryOp.GreaterOrEqual])
                arg_type = random.choice(SCALAR_TYPES)
        elif is_compatible(desired_type, INT) or is_compatible(desired_type, UINT):
            ops = [
                BinaryOp.Plus, BinaryOp.Minus, BinaryOp.Multiply,
                # BinaryOp.LeftShift, BinaryOp.RightShift,
                BinaryOp.BitAnd, BinaryOp.BitOr
            ]
            if not options.forbid_throwing:
                ops.append(BinaryOp.Divide)
            op = random.choice(ops)
            arg_type = desired_type
        elif is_compatible(desired_type, DOUBLE):
            ops = [BinaryOp.Plus, BinaryOp.Minus, BinaryOp.Multiply]
            if not options.forbid_throwing:
                ops.append(BinaryOp.Divide)
            op = random.choice(ops)
            arg_type = desired_type
        elif is_compatible(desired_type, STRING):
            op = BinaryOp.Concatenate
            arg_type = desired_type
        else:
            assert "unreachable"

        lhs = make_random_expression(schema, arg_type, depth + 1, options)
        rhs = make_random_expression(schema, arg_type, depth + 1, options)

        return Binary(op, lhs, rhs)

    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        return SCALAR_TYPES

################################################################################

class In(IExpression):
    def __init__(self, expr: IExpression, values: List[Literal]):
        assert all([expr.type == v.type] for v in values)
        assert values
        self.expr = expr
        self.values = values
        self.type = BOOLEAN

    def __str__(self):
        return f"({self.expr}) IN ({','.join([str(v) for v in self.values])})"

    def eval(self, row: TRow):
        expr_eval = self.expr.eval(row)
        return expr_eval in [v.eval(row) for v in self.values]

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions):
        assert is_compatible(desired_type, BOOLEAN)
        arg_type = random.choice(SCALAR_TYPES)

        expr = make_random_expression(schema, arg_type, depth + 1, options)
        value_count = random.randint(1, 5)
        values = [Literal.make_random(schema, arg_type, depth + 1, options) for _ in range(value_count)]
        return In(expr, values)

    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        return (BOOLEAN, )

################################################################################

class Transform(IExpression):
    def __init__(self, expr: IExpression, start: List[Literal], end: List[Literal], default:Literal = None):
        assert start
        assert end
        assert len(start) == len(end)
        assert all([expr.type == s.type] for s in start)
        for i in range(1, len(end)):
            assert end[0].type == end[i].type
        self.expr = expr
        self.start = start
        self.end = end
        self.type = end[0].type
        if default:
            assert default.type == self.type
        self.default = default

    def __str__(self):
        return ("TRANSFORM(" +
            f"{self.expr}, " +
            f"({', '.join([str(s) for s in self.start])}), "+
            f"({', '.join([str(e) for e in self.end])})" +
            f", {self.default})" if self.default else ")")

    def eval(self, row: TRow):
        expr_eval = self.expr.eval(row)
        match = [i for i in range(len(self.start)) if expr_eval == self.start[i].eval(row)]
        if match:
            return self.end[match[0]].eval(row)
        else:
            return self.default.eval(row)

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions):
        arg_type = random.choice(SCALAR_TYPES)
        expr = make_random_expression(schema, arg_type, depth + 1, options)

        value_count = random.randint(1, 5)
        start = [Literal.make_random(schema, arg_type, depth + 1, options) for _ in range(value_count)]
        end = [Literal.make_random(schema, desired_type, depth + 1, options) for _ in range(value_count)]

        default = make_random_expression(schema, desired_type, depth + 1, options)
        return Transform(expr, start, end, default)

    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        return SCALAR_TYPES

################################################################################

class Function(IExpression):
    SUPPORTED = set((
        "numeric_to_string",
        "int64",
        "uint64",
        "double",
        # Excluded for being prone to throwing
        # "parse_int64",
        # "parse_uint64",
        # "parse_double",
        "if",
    ))

    def __init__(self, args: List[IExpression], name: str, type: TType):
        assert name in Function.SUPPORTED
        self.name = name
        self.args = args
        self.type = type

    def __str__(self):
        return f"{self.name}({', '.join(map(str, self.args))})"

    def eval(self, row: TRow):
        assert len(self.args) >= 1

        arg = self.args[0].eval(row)
        if arg is None:
            return None

        if self.name == "numeric_to_string":
            assert isinstance(arg, float) or isinstance(arg, int)
            return str(arg)
        elif self.name == "int64" or self.name == "uint64":
            assert isinstance(arg, float) or isinstance(arg, int)
            return int(arg)
        elif self.name == "double":
            assert isinstance(arg, float) or isinstance(arg, int)
            return float(arg)
        elif self.name == "if":
            return self.args[1].eval(row) if arg else self.args[2].eval(row)
        else:
            assert False, "unreachable"

    @staticmethod
    def make_random(schema: TSchema, desired_type: TType, depth: int, options: RegOptions):
        if is_compatible(desired_type, INT):
            name = random.choice(["int64", "if"])
        elif is_compatible(desired_type, UINT):
            name = random.choice(["uint64", "if"])
        elif is_compatible(desired_type, DOUBLE):
            name = random.choice(["double", "if"])
        elif is_compatible(desired_type, STRING):
            name = random.choice(["numeric_to_string", "if"])
        elif is_compatible(desired_type, BOOLEAN):
            name = "if"
        args = []
        if name in ("numeric_to_string", "int64", "uint64", "double"):
            args.append(make_random_expression(schema, random.choice([INT, UINT, DOUBLE]), depth + 1, options))
        elif name == "if":
            args = [
                make_random_predicate(schema, depth + 1, options),
                make_random_expression(schema, desired_type, depth + 1, options),
                make_random_expression(schema, desired_type, depth + 1, options),
            ]
        else:
            assert False, "unreachable"
        return Function(args, name, desired_type)

    @staticmethod
    def get_out_types(schema: TSchema) -> TTypes:
        return SCALAR_TYPES

################################################################################

def is_terminal(kind: type):
    return kind in (Reference, Literal)

def get_random_expression_kind(schema: TSchema, desired_type: TType, depth:int, options: RegOptions) -> type:
    if depth >= options.max_depth:
        return random.choice([
            kind for kind in (Literal, Reference)
            if any([is_compatible(desired_type, t) for t in kind.get_out_types(schema)])
        ])
    allowed_kinds = [kind for kind in (
        Reference,
        Literal,
        Unary,
        Binary,
        Function,
        In,
        # Transform
    ) if kind not in options.forbidden_kinds]

    kind_weight_list = [
        (kind, depth / options.max_depth if is_terminal(kind) else 1 - depth / options.max_depth)
        for kind in allowed_kinds
        if any([is_compatible(desired_type, t) for t in kind.get_out_types(schema)])
    ]
    kinds, weights = zip(*kind_weight_list)
    return random.choices(kinds, weights=weights)[0]


def make_random_expression(schema: TSchema, desired_type: type, depth: int=0, options: RegOptions=RegOptions()) -> IExpression:
    kind = get_random_expression_kind(schema, desired_type, depth, options)
    return kind.make_random(schema, desired_type, depth, options)

def make_random_predicate(schema: TSchema, depth: int=0, options: RegOptions=RegOptions()) -> IExpression:
    kind = get_random_expression_kind(schema, BOOLEAN, depth, options)
    return kind.make_random(schema, BOOLEAN, depth, options)

################################################################################

def main():
    schema = [
        {"name": "v1", "type": "int64"},
        {"name": "v2", "type": "double"},
        {"name": "v3", "type": "string"},
        {"name": "v4", "type": "uint64"},
        {"name": "v5", "type": "boolean"},
        {"name": "v6", "type": "any", "type_v3": {
            "type_name": "list",
            "item": {
                "type_name": "optional",
                "item": "uint64",
            },
        }},
        {"name": "v7", "type": "any", "type_v3": {
            "type_name": "optional",
            "item": {
                "type_name": "list",
                "item": "int64",
            },
        }},
    ]

    expression = make_random_expression(schema, {"type_v3": {"type_name": "optional", "item": "int64"}})

    print(str(expression))

if __name__ == "__main__":
    main()
