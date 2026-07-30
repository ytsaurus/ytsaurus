#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
#

from typing import List, Optional

from attrs import define as _attrs_define

from confluent_kafka.schema_registry.serde import RuleAction, RuleExecutor


@_attrs_define(frozen=True)
class RuleOverride:
    type: str
    on_success: Optional[str]
    on_failure: Optional[str]
    disabled: Optional[bool]


class RuleRegistry(object):
    __slots__ = ['_rule_executors', '_rule_actions', '_rule_overrides']

    def __init__(self):
        self._rule_executors = {}
        self._rule_actions = {}
        self._rule_overrides = {}

    def register_executor(self, rule_executor: RuleExecutor):
        self._rule_executors[rule_executor.type()] = rule_executor

    def get_executor(self, name: str) -> Optional[RuleExecutor]:
        return self._rule_executors.get(name)

    def get_executors(self) -> List[RuleExecutor]:
        return list(self._rule_executors.values())

    def register_action(self, rule_action: RuleAction):
        self._rule_actions[rule_action.type()] = rule_action

    def get_action(self, name: str) -> Optional[RuleAction]:
        return self._rule_actions.get(name)

    def get_actions(self) -> List[RuleAction]:
        return list(self._rule_actions.values())

    def register_override(self, rule_override: RuleOverride):
        self._rule_overrides[rule_override.type] = rule_override

    def get_override(self, name: str) -> Optional[RuleOverride]:
        return self._rule_overrides.get(name)

    def get_overrides(self) -> List[RuleOverride]:
        return list(self._rule_overrides.values())

    def clear(self):
        self._rule_executors.clear()
        self._rule_actions.clear()
        self._rule_overrides.clear()

    @staticmethod
    def get_global_instance():
        return _global_instance

    @staticmethod
    def register_rule_executor(rule_executor: RuleExecutor):
        _global_instance.register_executor(rule_executor)

    @staticmethod
    def register_rule_action(rule_action: RuleAction):
        _global_instance.register_action(rule_action)

    @staticmethod
    def register_rule_override(rule_override: RuleOverride):
        _global_instance.register_override(rule_override)


_global_instance = RuleRegistry()
