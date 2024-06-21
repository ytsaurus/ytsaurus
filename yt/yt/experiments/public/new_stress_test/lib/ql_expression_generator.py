from .schema import Column, TInt64, TBoolean, TDouble, TString, TUint64#, TAny

import yt.yson as yson

from enum import Enum
from typing import List, Dict, Any
import random

MAXIMUM_DEPTH = 5
ALL_TYPES = (
    TUint64,
    TInt64,
    TDouble,
    TString,
    # TAny,
    TBoolean,
)

class IExpression:
    def __init__(self):
        self.type = None
        assert False, "abstract"
    def eval(self, _):
        assert False, "abstract"
    def __str__(self):
        assert False, "abstract"
    def make_random(columns, desired_type, depth):
        assert False, "abstract"
    def get_out_types(columns):
        assert False, "abstract"

class Reference(IExpression):
    def __init__(self, column: Column):
        self.column = column
        self.type = column.type

    def __str__(self):
        return self.column.name

    def eval(self, row):
        return row.get(self.column.name, None)

    def make_random(columns: List[Column], desired_type: yson.YsonType, depth: int):
        candidates = tuple(col for col in columns if col.type == desired_type)
        assert candidates
        return Reference(random.choice(candidates))

    def get_out_types(columns: List[Column]):
        return tuple(col.type for col in columns)

class Literal(IExpression):
    def make_null(desired_type):
        lit = Literal(yson.YsonInt64(1))
        lit.type = desired_type
        lit.value = None
        return lit

    def __init__(self, value: yson.YsonType):
        self.value = value
        if isinstance(value, yson.YsonUint64):
            self.type = TUint64
        elif isinstance(value, yson.YsonInt64):
            self.type = TInt64
        elif isinstance(value, yson.YsonDouble):
            self.type = TDouble
        elif isinstance(value, yson.YsonString):
            self.type = TString
        elif isinstance(value, yson.YsonBoolean):
            self.type = TBoolean
        # elif isinstance(value, yson.YsonType):
        #     self.type = TAny
        else:
            assert False, "unsupported value"

    def __str__(self):
        if self.value is None:
            return "#"
        if self.type == TUint64:
            return f"{self.value}u"
        return str(self.value)

    def eval(self, _):
        return self.value

    def make_random(columns: List[Column], desired_type: type, depth: int):
        return Literal(desired_type.random())

    def get_out_types(columns: List[Column]):
        return ALL_TYPES

class UnaryOp(Enum):
    Plus = 1
    Minus = 2
    BitNot = 3
    Not = 4

class Unary(IExpression):
    def __init__(self, op: UnaryOp, operand: IExpression):
        if op == UnaryOp.Plus or op == UnaryOp.Minus or op == UnaryOp.BitNot:
            assert operand.type == TInt64 or operand.type == TUint64
        elif op == UnaryOp.Not:
            assert operand.type == TBoolean
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
            return f"!({self.operand})"
        else:
            assert False, "unreachable"

    def eval(self, row: Dict[str, Any]):
        op_eval = self.operand.eval(row)
        if op_eval is None:
            return None
        if self.op == UnaryOp.Plus:
            return op_eval
        elif self.op == UnaryOp.Minus:
            return -op_eval
        elif self.op == UnaryOp.BitNot:
            if self.type == TUint64:
                return 2**64 - op_eval - 1
            elif self.type == TInt64:
                return - op_eval - 1
            else:
                assert False, "unreachable"
        elif self.op == UnaryOp.Not:
            return not op_eval
        else:
            assert False, "unreachable"

    def make_random(columns: List[Column], desired_type: type, depth: int):
        operand = make_random_expression(columns, desired_type, depth + 1)
        op = UnaryOp.Not if desired_type == TBoolean else random.choice([UnaryOp.BitNot, UnaryOp.Minus, UnaryOp.Plus])

        return Unary(op, operand)

    def get_out_types(columns: List[Column]):
        return (TInt64, TUint64, TBoolean)

class BinaryOp(Enum):
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
        assert lhs.type == rhs.type
        t = lhs.type
        if op in (BinaryOp.Plus, BinaryOp.Minus, BinaryOp.Multiply, BinaryOp.Divide):
            assert t in (TInt64, TUint64, TDouble)
            self.type = t
        elif op in (BinaryOp.And, BinaryOp.Or):
            assert t == TBoolean
            self.type = TBoolean
        elif op in (BinaryOp.Modulo, BinaryOp.BitAnd, BinaryOp.BitOr,
            # BinaryOp.LeftShift, BinaryOp.RightShift
        ):
            assert t in (TInt64, TUint64)
            self.type = t
        elif op in (BinaryOp.Equal, BinaryOp.NotEqual, BinaryOp.Less, BinaryOp.Greater, BinaryOp.LessOrEqual, BinaryOp.GreaterOrEqual):
            assert t in (
                TInt64, TUint64, TDouble, TBoolean, TString,
                # TAny,
            )
            self.type = TBoolean
        elif op == BinaryOp.Concatenate:
            assert t == TString
            self.type = TString
        else:
            assert False, "unreachable"
        self.op = op
        self.lhs = lhs
        self.rhs = rhs

    def __str__(self):
        return f"({self.lhs}) {Binary.OP_TO_SIGN[self.op]} ({self.rhs})"

    def eval_arithmetic(self, lhs: yson.YsonType, rhs: yson.YsonType):
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
            if self.type == TDouble:
                res = lhs / rhs
            elif (lhs >= 0) != (rhs >= 0) and lhs % rhs:
                return lhs // rhs + 1
            else:
                return lhs // rhs
        else:
            assert False, "unreachable"

        if self.type == TInt64:
            res = res & 0xFFFFFFFFFFFFFFFF
            res -= 2 ** 63
        elif self.type == TUint64:
            res = res & 0xFFFFFFFFFFFFFFFF

        return res

    def eval_logical(self, lhs: yson.YsonType, rhs: yson.YsonType):
        if lhs is None:
            lhs = False
        if rhs is None:
            rhs = False
        if self.op == BinaryOp.And:
            return lhs and rhs
        if self.op == BinaryOp.Or:
            return lhs or rhs
        assert False, "unreachable"

    def eval_equality(self, lhs: yson.YsonType, rhs: yson.YsonType):
        if lhs is None:
            if rhs is None:
                return self.op == BinaryOp.Equal
            else:
                return self.op != BinaryOp.Equal
        if rhs is None:
            return self.op != BinaryOp.Equal
        return (lhs == rhs) == (self.op == BinaryOp.Equal)


    def eval_relation(self, lhs, rhs):
        def eval_less(lhs: yson.YsonType, rhs: yson.YsonType):
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

    # def eval_shift(self, lhs: yson.YsonType, rhs: yson.YsonType):
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

    def eval_bit_logic(self, lhs: yson.YsonType, rhs: yson.YsonType):
        if lhs is None or rhs is None:
            return None
        if self.op == BinaryOp.BitAnd:
            return lhs & rhs
        if self.op == BinaryOp.BitOr:
            return lhs | rhs
        assert False, "unreachable"

    def eval(self, row: Dict[str, Any]):
        lhs_eval = self.lhs.eval(row)
        rhs_eval = self.rhs.eval(row)
        if self.op in (BinaryOp.Plus, BinaryOp.Minus, BinaryOp.Divide, BinaryOp.Multiply):
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

    def make_random(columns: List[Column], desired_type: type, depth: int):
        op = None
        lhs = None
        rhs = None
        arg_type = None
        if desired_type == TBoolean:
            fate = random.random()
            if fate < 0.33:
                op = random.choice([BinaryOp.And, BinaryOp.Or])
                arg_type = TBoolean
            elif fate < 0.66:
                op = random.choice([BinaryOp.Equal, BinaryOp.NotEqual])
                arg_type = random.choice([
                    TInt64, TUint64, TDouble, TBoolean, TString,
                    # TAny,
                ])
            else:
                op = random.choice([BinaryOp.Less, BinaryOp.Greater, BinaryOp.LessOrEqual, BinaryOp.GreaterOrEqual])
                arg_type = random.choice([
                    TInt64, TUint64, TDouble, TBoolean, TString,
                    # TAny,
                ])
        elif desired_type in (TInt64, TUint64):
            op = random.choice([
                BinaryOp.Plus, BinaryOp.Minus, BinaryOp.Multiply, BinaryOp.Divide,
                # BinaryOp.LeftShift, BinaryOp.RightShift,
                BinaryOp.BitAnd, BinaryOp.BitOr
            ])
            arg_type = desired_type
        elif desired_type == TDouble:
            op = random.choice([BinaryOp.Plus, BinaryOp.Minus, BinaryOp.Multiply, BinaryOp.Divide])
            arg_type = desired_type
        elif desired_type == TString:
            op = BinaryOp.Concatenate
            arg_type = desired_type
        else:
            assert "unreachable"

        lhs = make_random_expression(columns, arg_type, depth + 1)
        rhs = make_random_expression(columns, arg_type, depth + 1)

        return Binary(op, lhs, rhs)

    def get_out_types(columns: List[Column]):
        return (TInt64, TUint64, TBoolean, TDouble, TString)

class In(IExpression):
    def __init__(self, expr: IExpression, values: List[Literal]):
        assert all([expr.type == v.type] for v in values)
        assert values
        self.expr = expr
        self.values = values
        self.type = TBoolean

    def __str__(self):
        return f"({self.expr}) IN ({", ".join([str(v) for v in self.values])})"

    def eval(self, row: Dict[str, Any]):
        expr_eval = self.expr.eval(row)
        return expr_eval in [v.eval(row) for v in self.values]

    def make_random(columns: List[Column], desired_type: type, depth: int):
        assert desired_type == TBoolean
        arg_type = random.choice([TInt64, TUint64, TBoolean, TString, TDouble])

        expr = make_random_expression(columns, arg_type, depth + 1)
        value_count = random.randint(1, 5)
        values = [Literal.make_random(columns, arg_type, depth + 1) for _ in range(value_count)]
        return In(expr, values)

    def get_out_types(columns: List[Column]):
        return (TBoolean,)

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
        return "TRANSFORM("+\
            f"{self.expr}, "+\
            f"({", ".join([str(s) for s in self.start])}), "+\
            f"({", ".join([str(e) for e in self.end])})" +\
            f", {self.default})" if self.default else ")"

    def eval(self, row: dict):
        expr_eval = self.expr.eval(row)
        match = [i for i in range(len(self.start)) if expr_eval == self.start[i].eval(row)]
        if match:
            return self.end[match[0]].eval(row)
        else:
            return self.default.eval(row)

    def make_random(columns: List[Column], desired_type: type, depth: int):
        arg_type = random.choice([TInt64, TUint64, TBoolean, TString, TDouble])
        expr = make_random_expression(columns, arg_type, depth + 1)

        value_count = random.randint(1, 5)
        start = [Literal.make_random(columns, arg_type, depth + 1) for _ in range(value_count)]
        end = [Literal.make_random(columns, desired_type, depth + 1) for _ in range(value_count)]

        default = make_random_expression(columns, desired_type, depth + 1)
        return Transform(expr, start, end, default)

    def get_out_types(columns: List[Column]):
        return (TBoolean, TInt64, TUint64, TString, TDouble)

def get_random_expression_kind(columns: List[Column], desired_type: type, depth: int) -> type:
    if depth >= MAXIMUM_DEPTH:
        return random.choice([
            kind for kind in (Literal, Reference)
            if desired_type in kind.get_out_types(columns)
        ])
    return random.choice([
        kind for kind in (
            Unary,
            Binary,
            # In,
            # Transform
        )
        if desired_type in kind.get_out_types(columns)
    ])

def make_random_expression(columns: List[Column], desired_type: type, depth:int = 0) -> IExpression:
    kind = get_random_expression_kind(columns, desired_type, depth)
    return kind.make_random(columns, desired_type, depth)
