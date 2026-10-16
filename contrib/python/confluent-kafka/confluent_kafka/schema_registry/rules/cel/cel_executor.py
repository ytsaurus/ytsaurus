# Copyright 2024 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging
import uuid
from threading import Lock
from typing import Any, Dict, List, Optional

import celpy
from celpy import celtypes
from google.protobuf import message

from confluent_kafka.schema_registry import RuleKind, Schema
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.rules.cel.cel_field_presence import InterpretedRunner
from confluent_kafka.schema_registry.rules.cel.constraints import _msg_to_cel, _scalar_field_value_to_cel
from confluent_kafka.schema_registry.rules.cel.extra_func import EXTRA_FUNCS
from confluent_kafka.schema_registry.serde import FieldContext, RuleContext, RuleExecutor

log = logging.getLogger(__name__)

# A date logical type annotates an Avro int, where the int stores the number
# of days from the unix epoch, 1 January 1970 (ISO calendar).
DAYS_SHIFT = datetime.date(1970, 1, 1).toordinal()


class CelExecutor(RuleExecutor):

    def __init__(self):
        self._env = celpy.Environment(runner_class=InterpretedRunner)
        self._funcs = EXTRA_FUNCS
        self._cache = _CelCache()

    def type(self) -> str:
        return "CEL"

    def transform(self, ctx: RuleContext, msg: Any) -> Any:
        args = {"message": msg_to_cel(msg)}
        return self.execute(ctx, msg, args)

    def execute(self, ctx: RuleContext, msg: Any, args: Any) -> Any:
        expr = ctx.rule.expr
        if expr is None:
            log.warning("Expression from rule %s is None", ctx.rule.name)
            return msg
        try:
            index = expr.index(";")
        except ValueError:
            index = -1
        if index >= 0:
            guard = expr[:index]
            if len(guard.strip()) > 0:
                guard_result = self.execute_rule(ctx, guard, args)
                if not guard_result:
                    if ctx.rule.kind == RuleKind.CONDITION:
                        return True
                    return msg
            expr = expr[index + 1 :]

        return self.execute_rule(ctx, expr, args)

    def execute_rule(self, ctx: RuleContext, expr: str, args: Any) -> Any:
        schema = ctx.target
        if schema is None:
            # TODO: check whether we should raise or return fallback
            raise ValueError("Target schema is None")
        script_type = schema.schema_type
        if script_type is None:
            # TODO: check whether we should raise or return fallback
            raise ValueError("Target schema type is None")
        prog = self._cache.get_program(expr, script_type, schema)
        if prog is None:
            ast = self._env.compile(expr)
            prog = self._env.program(ast, functions=self._funcs)
            self._cache.set(expr, script_type, schema, prog)
        result = prog.evaluate(args)
        if isinstance(result, celtypes.BoolType):
            return bool(result)
        return result

    @classmethod
    def register(cls):
        RuleRegistry.register_rule_executor(CelExecutor())


class _CelCache(object):
    def __init__(self):
        self.lock = Lock()
        self.programs = {}

    def set(self, expr: str, script_type: str, schema: Schema, prog: celpy.Runner):
        with self.lock:
            self.programs[(expr, script_type, schema)] = prog

    def get_program(self, expr: str, script_type: str, schema: Schema) -> Optional[celpy.Runner]:
        with self.lock:
            return self.programs.get((expr, script_type, schema), None)

    def clear(self):
        with self.lock:
            self.programs.clear()


def msg_to_cel(msg: Any) -> Any:
    if isinstance(msg, message.Message):
        return _msg_to_cel(msg)
    else:
        return _value_to_cel(msg)


def field_value_to_cel(field_ctx: FieldContext, field_value: Any) -> Any:
    msg = field_ctx.containing_message
    if isinstance(msg, message.Message):
        desc = msg.DESCRIPTOR
        field_desc = desc.fields_by_name[field_ctx.name]
        return _scalar_field_value_to_cel(field_value, field_desc)
    else:
        return _value_to_cel(field_value)


def _value_to_cel(msg: Any) -> Any:
    if isinstance(msg, dict):
        return _dict_to_cel(msg)
    elif isinstance(msg, list):
        return _array_to_cel(msg)
    elif isinstance(msg, str):
        return celtypes.StringType(msg)
    elif isinstance(msg, bytes):
        return celtypes.BytesType(msg)
    elif isinstance(msg, int):
        return celtypes.IntType(msg)
    elif isinstance(msg, float):
        return celtypes.DoubleType(msg)
    elif isinstance(msg, bool):
        return celtypes.BoolType(msg)
    elif isinstance(msg, datetime.datetime):
        # this impl differs from the other clients
        return celtypes.TimestampType(msg)
    elif isinstance(msg, datetime.timedelta):
        # this impl differs from the other clients
        return celtypes.DurationType(msg)
    if isinstance(msg, datetime.date):
        # convert date to int
        return celtypes.IntType(msg.toordinal() - DAYS_SHIFT)
    elif isinstance(msg, uuid.UUID):
        return celtypes.StringType(str(msg))
    else:
        # unsupported: time-millis, time-micros, decimal
        return msg


def _dict_to_cel(val: dict) -> Dict[str, celtypes.Value]:
    result = celtypes.MapType()
    for key, val in val.items():
        result[key] = _value_to_cel(val)
    return result  # type: ignore[return-value]


def _array_to_cel(val: list) -> List[celtypes.Value]:
    result = celtypes.ListType()
    for item in val:
        result.append(_value_to_cel(item))
    return result
