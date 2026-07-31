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

from threading import Lock
from typing import Any, Optional

import jsonata

from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.serde import RuleContext, RuleExecutor


class JsonataExecutor(RuleExecutor):

    def __init__(self):
        self._cache = _JsonataCache()

    def type(self) -> str:
        return "JSONATA"

    def transform(self, ctx: RuleContext, message: Any) -> Any:
        jsonata_expr = self._cache.get_jsonata(ctx.rule.expr)
        if jsonata_expr is None:
            jsonata_expr = jsonata.Jsonata(ctx.rule.expr)
            self._cache.set(ctx.rule.expr, jsonata_expr)
        return jsonata_expr.evaluate(message)

    @classmethod
    def register(cls):
        RuleRegistry.register_rule_executor(JsonataExecutor())


class _JsonataCache(object):
    def __init__(self):
        self.lock = Lock()
        self.exprs = {}

    def set(self, expr: str, jsonata_expr: jsonata.Jsonata):
        with self.lock:
            self.exprs[expr] = jsonata_expr

    def get_jsonata(self, expr: str) -> Optional[jsonata.Jsonata]:
        with self.lock:
            return self.exprs.get(expr, None)

    def clear(self):
        with self.lock:
            self.exprs.clear()
