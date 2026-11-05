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

import asyncio as _locks
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from cachetools import LRUCache

from confluent_kafka.schema_registry import (
    AsyncSchemaRegistryClient,
    RegisteredSchema,
    topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.common.schema_registry_client import RulePhase
from confluent_kafka.schema_registry.common.serde import (
    STRATEGY_TYPE_MAP,
    ErrorAction,
    FieldTransformer,
    Migration,
    NoneAction,
    RuleAction,
    RuleConditionError,
    RuleContext,
    RuleError,
    SchemaId,
    SubjectNameStrategyType,
)
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.schema_registry.schema_registry_client import Rule, RuleKind, RuleMode, RuleSet, Schema
from confluent_kafka.serialization import (
    Deserializer,
    MessageField,
    SerializationContext,
    SerializationError,
    Serializer,
)

__all__ = [
    'AsyncAssociatedNameStrategy',
    'AsyncBaseSerde',
    'AsyncBaseSerializer',
    'AsyncBaseDeserializer',
    'KAFKA_CLUSTER_ID',
    'FALLBACK_TYPE',
]

log = logging.getLogger(__name__)


KAFKA_CLUSTER_ID = "subject.name.strategy.kafka.cluster.id"
NAMESPACE_WILDCARD = "-"
FALLBACK_TYPE = "subject.name.strategy.fallback.type"
DEFAULT_CACHE_CAPACITY = 1000


class AsyncAssociatedNameStrategy:
    """
    A subject name strategy that retrieves the associated subject name from schema registry
    by querying associations for the topic.

    This class encapsulates a cache for subject name lookups to avoid repeated API calls.

    Args:
        cache_capacity (int): Maximum number of entries to cache. Defaults to 1000.
    """

    def __init__(self, cache_capacity: int = DEFAULT_CACHE_CAPACITY):
        self._cache: LRUCache = LRUCache(maxsize=cache_capacity)
        self._lock: _locks.Lock = _locks.Lock()

    def _get_cache_key(self, topic: str, is_key: bool, record_name: Optional[str]) -> Tuple[str, bool, Optional[str]]:
        """Create a cache key from topic, is_key, and record_name."""
        return (topic, is_key, record_name)

    async def _load_subject_name(
        self,
        topic: str,
        is_key: bool,
        record_name: Optional[str],
        ctx: SerializationContext,
        schema_registry_client: AsyncSchemaRegistryClient,
        conf: Optional[dict],
    ) -> Optional[str]:
        """Load the subject name from schema registry (not cached)."""
        # Determine resource namespace from config
        kafka_cluster_id = None
        fallback_strategy = SubjectNameStrategyType.TOPIC  # default fallback

        # If no client is available, skip association lookup and use fallback directly
        if schema_registry_client is None:
            return topic_subject_name_strategy(ctx, record_name)

        if conf is not None:
            kafka_cluster_id = conf.get(KAFKA_CLUSTER_ID)
            fallback_config = conf.get(FALLBACK_TYPE)
            if fallback_config is not None:
                if isinstance(fallback_config, SubjectNameStrategyType):
                    fallback_strategy = fallback_config
                else:
                    try:
                        fallback_strategy = SubjectNameStrategyType(str(fallback_config).upper())
                    except ValueError:
                        valid_fallbacks = [
                            e.value for e in SubjectNameStrategyType if e != SubjectNameStrategyType.ASSOCIATED
                        ]
                        raise ValueError(
                            f"Invalid value for {FALLBACK_TYPE}: {fallback_config}. "
                            f"Valid values are: {', '.join(valid_fallbacks)}"
                        )

        resource_namespace = kafka_cluster_id if kafka_cluster_id is not None else NAMESPACE_WILDCARD

        # Determine association type based on whether this is key or value
        association_type = "key" if is_key else "value"

        # Query schema registry for associations
        try:
            associations = await schema_registry_client.get_associations_by_resource_name(
                resource_name=topic,
                resource_namespace=resource_namespace,
                resource_type="topic",
                association_types=[association_type],
                offset=0,
                limit=-1,
            )
        except SchemaRegistryError as e:
            if e.http_status_code == 404:
                # Treat 404 as no associations found and fall through to existing fallback logic
                associations = []
            else:
                raise

        if len(associations) > 1:
            raise SerializationError(f"Multiple associated subjects found for topic {topic}")
        elif len(associations) == 1:
            return associations[0].subject
        else:
            # No associations found, use fallback strategy
            if fallback_strategy == SubjectNameStrategyType.NONE:
                raise SerializationError(f"No associated subject found for topic {topic}")
            elif fallback_strategy == SubjectNameStrategyType.ASSOCIATED:
                raise ValueError(
                    f"Invalid value for {FALLBACK_TYPE}: {fallback_strategy.value}. "
                    f"ASSOCIATED cannot be used as a fallback strategy."
                )

            return STRATEGY_TYPE_MAP[fallback_strategy](ctx, record_name)

    async def __call__(
        self,
        ctx: Optional[SerializationContext],
        record_name: Optional[str],
        schema_registry_client: AsyncSchemaRegistryClient,
        conf: Optional[dict] = None,
    ) -> Optional[str]:
        """
        Retrieves the associated subject name from schema registry by querying
        associations for the topic.

        The topic is passed as the resource name to schema registry. If there is a
        configuration property named "kafka.cluster.id", then its value will be passed
        as the resource namespace; otherwise the value "-" will be passed as the
        resource namespace.

        If more than one subject is returned from the query, a SerializationError
        will be raised. If no subjects are returned from the query, then the behavior
        will fall back to topic_subject_name_strategy, unless the configuration property
        "subject.name.strategy.fallback.type" is set to "RECORD", "TOPIC_RECORD", or "NONE".

        Results are cached using an LRU cache to avoid repeated API calls.

        Args:
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation. **Required** - must contain topic and field information.

            record_name (Optional[str]): Record name (used for fallback strategies).

            schema_registry_client (AsyncSchemaRegistryClient): AsyncSchemaRegistryClient instance.

            conf (Optional[dict]): Configuration dictionary. Supports:
                - "subject.name.strategy.kafka.cluster.id": Kafka cluster ID to use as resource namespace.
                - "subject.name.strategy.fallback.type": Fallback strategy when no
                  associations are found. One of "TOPIC", "RECORD", "TOPIC_RECORD", or "NONE".
                  Defaults to "TOPIC".

        Returns:
            Optional[str]: The subject name from the association, or from the fallback strategy.

        Raises:
            SerializationError: If multiple associated subjects are found for the topic,
                or if no subjects are found and fallback is set to "NONE".
            ValueError: If ctx is None.
        """
        if ctx is None:
            raise ValueError(
                "SerializationContext is required for AsyncAssociatedNameStrategy. "
                "Either provide a SerializationContext or use a different strategy."
            )

        topic = ctx.topic
        if topic is None:
            return None

        is_key = ctx.field == MessageField.KEY
        cache_key = self._get_cache_key(topic, is_key, record_name)

        # Check cache first
        async with self._lock:
            cached_result = self._cache.get(cache_key)
            if cached_result is not None:
                return cached_result

        # Not in cache, load from schema registry
        result = await self._load_subject_name(topic, is_key, record_name, ctx, schema_registry_client, conf)

        # Cache the result
        if result is not None:
            async with self._lock:
                self._cache[cache_key] = result

        return result

    async def clear_cache(self) -> None:
        """Clear the association subject name cache."""
        async with self._lock:
            self._cache.clear()


class AsyncBaseSerde(object):
    __slots__ = [
        '_use_schema_id',
        '_use_latest_version',
        '_use_latest_with_metadata',
        '_registry',
        '_rule_registry',
        '_strategy_accepts_client',
        '_subject_name_conf',
        '_subject_name_func',
        '_field_transformer',
    ]

    _use_schema_id: Optional[int]
    _use_latest_version: bool
    _use_latest_with_metadata: Optional[Dict[str, str]]
    _registry: Any  # AsyncSchemaRegistryClient
    _rule_registry: Any  # RuleRegistry
    _strategy_accepts_client: bool
    _subject_name_conf: Optional[dict]
    _subject_name_func: Callable[..., Any]
    _field_transformer: Optional[FieldTransformer]

    def configure_subject_name_strategy(
        self,
        subject_name_strategy_type: Optional[Union[SubjectNameStrategyType, str]] = None,
        subject_name_strategy_conf: Optional[dict] = None,
        subject_name_strategy: Optional[Callable] = None,
    ) -> None:
        """
        Configure the subject name strategy for this serde.

        This method supports both the legacy callable approach and the new type-based approach.
        If both `subject_name_strategy` (as a callable) and `subject_name_strategy_type` are
        provided, the callable takes precedence.

        Args:
            subject_name_strategy: A callable that implements the subject name strategy.
                Signature: (SerializationContext, str) -> str or
                          (SerializationContext, str, AsyncSchemaRegistryClient, dict) -> str

            subject_name_strategy_type: The type of subject name strategy to use.
                Can be a SubjectNameStrategyType enum value or a string
                ("TOPIC", "RECORD", "TOPIC_RECORD", "ASSOCIATED").

            subject_name_strategy_conf: Configuration dictionary passed to strategies
                that accept extra parameters (like ASSOCIATED).

        Raises:
            ValueError: If the strategy is not callable or the type is invalid.
        """
        self._subject_name_conf = subject_name_strategy_conf

        # If a callable is provided, use it directly (backward compatible)
        if subject_name_strategy is not None:
            if not callable(subject_name_strategy):
                raise ValueError("subject.name.strategy must be callable")
            self._subject_name_func = subject_name_strategy
            self._strategy_accepts_client = isinstance(subject_name_strategy, AsyncAssociatedNameStrategy)
            return

        # If a type is provided, resolve it to a callable
        if subject_name_strategy_type is not None:
            # Convert string to enum if needed
            if isinstance(subject_name_strategy_type, str):
                try:
                    subject_name_strategy_type = SubjectNameStrategyType(subject_name_strategy_type.upper())
                except ValueError:
                    raise ValueError(
                        f"Invalid subject.name.strategy.type: {subject_name_strategy_type}. "
                        f"Valid values are: {[e.value for e in SubjectNameStrategyType]}"
                    )

            # Handle ASSOCIATED specially since it needs schema_registry_client
            if subject_name_strategy_type == SubjectNameStrategyType.ASSOCIATED:
                self._subject_name_func = AsyncAssociatedNameStrategy()
                self._strategy_accepts_client = True
            elif subject_name_strategy_type == SubjectNameStrategyType.NONE:
                raise ValueError(
                    f"Invalid subject.name.strategy.type: {subject_name_strategy_type}. "
                    f"NONE cannot be used as a subject name strategy."
                )
            elif subject_name_strategy_type in STRATEGY_TYPE_MAP:
                self._subject_name_func = STRATEGY_TYPE_MAP[subject_name_strategy_type]
                self._strategy_accepts_client = False
            else:
                raise ValueError(f"Unknown subject.name.strategy.type: {subject_name_strategy_type}")
            return

        # Default to AsyncAssociatedNameStrategy (falls back to TOPIC when no associations found)
        self._subject_name_func = AsyncAssociatedNameStrategy()
        self._strategy_accepts_client = True

    async def _get_reader_schema(self, subject: str, fmt: Optional[str] = None) -> Optional[RegisteredSchema]:
        if self._use_schema_id is not None:
            schema = await self._registry.get_schema(self._use_schema_id, subject, fmt)
            registered_schema = self._registry._cache.get_registered_by_subject_id(subject, self._use_schema_id)
            if registered_schema is not None:
                return registered_schema
            return await self._registry.lookup_schema(subject, schema, normalize_schemas=False, deleted=True)
        if self._use_latest_with_metadata is not None:
            return await self._registry.get_latest_with_metadata(
                subject, self._use_latest_with_metadata, deleted=True, fmt=fmt
            )
        if self._use_latest_version:
            return await self._registry.get_latest_version(subject, fmt)
        return None

    def _execute_rules(
        self,
        ser_ctx: SerializationContext,
        subject: str,
        rule_mode: RuleMode,
        source: Optional[Schema],
        target: Optional[Schema],
        message: Any,
        inline_tags: Optional[Dict[str, Set[str]]],
        field_transformer: Optional[FieldTransformer],
    ) -> Any:
        return self._execute_rules_with_phase(
            ser_ctx, subject, RulePhase.DOMAIN, rule_mode, source, target, message, inline_tags, field_transformer
        )

    def _execute_rules_with_phase(
        self,
        ser_ctx: SerializationContext,
        subject: str,
        rule_phase: RulePhase,
        rule_mode: RuleMode,
        source: Optional[Schema],
        target: Optional[Schema],
        message: Any,
        inline_tags: Optional[Dict[str, Set[str]]],
        field_transformer: Optional[FieldTransformer],
    ) -> Any:
        if message is None or target is None:
            return message
        enabled_env: Optional[str] = None
        rules: Optional[List[Rule]] = None
        if rule_mode == RuleMode.UPGRADE:
            if target is not None and target.rule_set is not None:
                enabled_env = target.rule_set.enable_at
                rules = target.rule_set.migration_rules
        elif rule_mode == RuleMode.DOWNGRADE:
            if source is not None and source.rule_set is not None:
                enabled_env = source.rule_set.enable_at
                rules = source.rule_set.migration_rules
                rules = rules[:] if rules else []
                rules.reverse()
        else:
            if target is not None and target.rule_set is not None:
                enabled_env = target.rule_set.enable_at
                if rule_phase == RulePhase.ENCODING:
                    rules = target.rule_set.encoding_rules
                else:
                    rules = target.rule_set.domain_rules
                if rule_mode == RuleMode.READ:
                    # Execute read rules in reverse order for symmetry
                    rules = rules[:] if rules else []
                    rules.reverse()

        if not rules:
            return message

        for index in range(len(rules)):
            rule = rules[index]
            ctx = RuleContext(
                enabled_env,
                ser_ctx,
                source,
                target,
                subject,
                rule_mode,
                rule,
                index,
                rules,
                inline_tags,
                field_transformer,
            )
            if self._is_disabled(ctx, rule):
                continue
            if rule.mode == RuleMode.WRITEREAD:
                if rule_mode != RuleMode.READ and rule_mode != RuleMode.WRITE:
                    continue
            elif rule.mode == RuleMode.UPDOWN:
                if rule_mode != RuleMode.UPGRADE and rule_mode != RuleMode.DOWNGRADE:
                    continue
            elif rule.mode != rule_mode:
                continue
            if rule.type is None:
                self._run_action(
                    ctx,
                    rule_mode,
                    rule,
                    self._get_on_failure(rule),
                    message,
                    RuleError(f"Rule type is None for rule {rule.name}"),
                    'ERROR',
                )
                return message
            rule_executor = self._rule_registry.get_executor(rule.type.upper())
            if rule_executor is None:
                self._run_action(
                    ctx,
                    rule_mode,
                    rule,
                    self._get_on_failure(rule),
                    message,
                    RuleError(f"Could not find rule executor of type {rule.type}"),
                    'ERROR',
                )
                return message
            try:
                result = rule_executor.transform(ctx, message)
                if rule.kind == RuleKind.CONDITION:
                    if not result:
                        raise RuleConditionError(rule)
                elif rule.kind == RuleKind.TRANSFORM:
                    message = result
                self._run_action(
                    ctx,
                    rule_mode,
                    rule,
                    self._get_on_failure(rule) if message is None else self._get_on_success(rule),
                    message,
                    None,
                    'ERROR' if message is None else 'NONE',
                )
            except SerializationError:
                raise
            except Exception as e:
                self._run_action(ctx, rule_mode, rule, self._get_on_failure(rule), message, e, 'ERROR')
        return message

    def _get_on_success(self, rule: Rule) -> Optional[str]:
        if rule.type is None:
            return rule.on_success
        override = self._rule_registry.get_override(rule.type)
        if override is not None and override.on_success is not None:
            return override.on_success
        return rule.on_success

    def _get_on_failure(self, rule: Rule) -> Optional[str]:
        if rule.type is None:
            return rule.on_failure
        override = self._rule_registry.get_override(rule.type)
        if override is not None and override.on_failure is not None:
            return override.on_failure
        return rule.on_failure

    def _is_disabled(self, ctx: RuleContext, rule: Rule) -> Optional[bool]:
        if rule.type is None:
            return rule.disabled
        override = self._rule_registry.get_override(rule.type)
        if override is not None and override.disabled is not None:
            return override.disabled
        enabled_env = ctx.enabled_env if ctx.enabled_env is not None else "ALL"
        if enabled_env != "ALL" and enabled_env != "CLIENT":
            return True
        return rule.disabled

    def _run_action(
        self,
        ctx: RuleContext,
        rule_mode: RuleMode,
        rule: Rule,
        action: Optional[str],
        message: Any,
        ex: Optional[Exception],
        default_action: str,
    ):
        action_name = self._get_rule_action_name(rule, rule_mode, action)
        if action_name is None:
            action_name = default_action
        rule_action = self._get_rule_action(ctx, action_name)
        if rule_action is None:
            log.error("Could not find rule action of type %s", action_name)
            raise RuleError(f"Could not find rule action of type {action_name}")
        try:
            rule_action.run(ctx, message, ex)
        except SerializationError:
            raise
        except Exception as e:
            log.warning("Could not run post-rule action %s: %s", action_name, e)

    def _get_rule_action_name(self, rule: Rule, rule_mode: RuleMode, action_name: Optional[str]) -> Optional[str]:
        if action_name is None or action_name == "":
            return None
        if rule.mode in (RuleMode.WRITEREAD, RuleMode.UPDOWN) and ',' in action_name:
            parts = action_name.split(',')
            if rule_mode in (RuleMode.WRITE, RuleMode.UPGRADE):
                return parts[0]
            elif rule_mode in (RuleMode.READ, RuleMode.DOWNGRADE):
                return parts[1]
        return action_name

    def _get_rule_action(self, ctx: RuleContext, action_name: str) -> Optional[RuleAction]:
        if action_name == 'ERROR':
            return ErrorAction()
        elif action_name == 'NONE':
            return NoneAction()
        return self._rule_registry.get_action(action_name)


class AsyncBaseSerializer(AsyncBaseSerde, Serializer):
    __slots__ = ['_auto_register', '_normalize_schemas', '_schema_id_serializer']

    _auto_register: bool
    _normalize_schemas: bool
    _schema_id_serializer: Callable[[bytes, Any, Any], bytes]


class AsyncBaseDeserializer(AsyncBaseSerde, Deserializer):
    __slots__ = ['_schema_id_deserializer']

    _schema_id_deserializer: Callable[[bytes, Any, Any], Any]

    async def _get_writer_schema(
        self, schema_id: SchemaId, subject: Optional[str] = None, fmt: Optional[str] = None
    ) -> Schema:
        if schema_id.id is not None:
            return await self._registry.get_schema(schema_id.id, subject, fmt)
        elif schema_id.guid is not None:
            return await self._registry.get_schema_by_guid(str(schema_id.guid), fmt)
        else:
            raise SerializationError("Schema ID or GUID is not set")

    def _has_rules(self, rule_set: RuleSet, phase: RulePhase, mode: RuleMode) -> bool:
        if rule_set is None:
            return False
        if phase == RulePhase.MIGRATION:
            rules = rule_set.migration_rules
        elif phase == RulePhase.DOMAIN:
            rules = rule_set.domain_rules
        elif phase == RulePhase.ENCODING:
            rules = rule_set.encoding_rules
        if mode in (RuleMode.UPGRADE, RuleMode.DOWNGRADE):
            return any(rule.mode == mode or rule.mode == RuleMode.UPDOWN for rule in rules or [])
        elif mode == RuleMode.UPDOWN:
            return any(rule.mode == mode for rule in rules or [])
        elif mode in (RuleMode.WRITE, RuleMode.READ):
            return any(rule.mode == mode or rule.mode == RuleMode.WRITEREAD for rule in rules or [])
        elif mode == RuleMode.WRITEREAD:
            return any(rule.mode == mode for rule in rules or [])
        return False

    async def _get_migrations(
        self, subject: str, source_info: Schema, target: RegisteredSchema, fmt: Optional[str]
    ) -> List[Migration]:
        source = await self._registry.lookup_schema(subject, source_info, normalize_schemas=False, deleted=True)
        migrations: List[Migration] = []
        if source.version < target.version:
            migration_mode = RuleMode.UPGRADE
            first = source
            last = target
        elif source.version > target.version:
            migration_mode = RuleMode.DOWNGRADE
            first = target
            last = source
        else:
            return migrations
        previous: Optional[RegisteredSchema] = None
        versions = await self._get_schemas_between(subject, first, last, fmt)
        for i in range(len(versions)):
            version = versions[i]
            if i == 0:
                previous = version
                continue
            if (
                version.schema is not None
                and version.schema.rule_set is not None
                and self._has_rules(version.schema.rule_set, RulePhase.MIGRATION, migration_mode)
            ):
                if previous is not None:  # previous is always set after first iteration
                    if migration_mode == RuleMode.UPGRADE:
                        migration = Migration(migration_mode, previous, version)
                    else:
                        migration = Migration(migration_mode, version, previous)
                    migrations.append(migration)
            previous = version
        if migration_mode == RuleMode.DOWNGRADE:
            migrations.reverse()
        return migrations

    async def _get_schemas_between(
        self, subject: str, first: RegisteredSchema, last: RegisteredSchema, fmt: Optional[str] = None
    ) -> List[RegisteredSchema]:
        if first.version is None or last.version is None:
            return [first, last]
        if last.version - first.version <= 1:
            return [first, last]
        version1 = first.version
        version2 = last.version
        result = [first]
        for i in range(version1 + 1, version2):
            result.append(await self._registry.get_version(subject, i, True, fmt))
        result.append(last)
        return result

    def _execute_migrations(
        self, ser_ctx: SerializationContext, subject: str, migrations: List[Migration], message: Any
    ) -> Any:
        for migration in migrations:
            if migration.source is not None and migration.target is not None:
                message = self._execute_rules_with_phase(
                    ser_ctx,
                    subject,
                    RulePhase.MIGRATION,
                    migration.rule_mode,
                    migration.source.schema,
                    migration.target.schema,
                    message,
                    None,
                    None,
                )
        return message
