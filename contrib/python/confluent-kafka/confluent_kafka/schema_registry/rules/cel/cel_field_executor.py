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

from typing import Any

from celpy import celtypes

from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor, field_value_to_cel, msg_to_cel
from confluent_kafka.schema_registry.serde import FieldContext, FieldRuleExecutor, FieldTransform, RuleContext


class CelFieldExecutor(FieldRuleExecutor):

    def __init__(self):
        self._executor = CelExecutor()

    def type(self) -> str:
        return "CEL_FIELD"

    def new_transform(self, ctx: RuleContext) -> FieldTransform:
        return self._field_transform

    def _field_transform(self, ctx: RuleContext, field_ctx: FieldContext, field_value: Any) -> Any:
        if field_value is None:
            return None
        if not field_ctx.is_primitive():
            return field_value
        args = {
            "value": field_value_to_cel(field_ctx, field_value),
            "fullName": field_ctx.full_name,
            "name": field_ctx.name,
            "typeName": field_ctx.type_name(),
            "tags": [celtypes.StringType(tag) for tag in field_ctx.tags],
            "message": msg_to_cel(field_ctx.containing_message),
        }
        return self._executor.execute(ctx, field_value, args)

    @classmethod
    def register(cls):
        RuleRegistry.register_rule_executor(CelFieldExecutor())
