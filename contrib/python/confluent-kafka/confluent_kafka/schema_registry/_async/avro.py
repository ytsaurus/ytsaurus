#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
import io
import json
from typing import Any, Callable, Coroutine, Dict, Optional, Union, cast

from fastavro import schemaless_reader, schemaless_writer
from fastavro.schema import expand_schema

from confluent_kafka.schema_registry import (
    AsyncSchemaRegistryClient,
    RuleMode,
    Schema,
    dual_schema_id_deserializer,
    prefix_schema_id_serializer,
)
from confluent_kafka.schema_registry.common import asyncinit
from confluent_kafka.schema_registry.common.avro import (
    AVRO_TYPE,
    AvroSchema,
    _ContextStringIO,
    _schema_loads,
    get_inline_tags,
    parse_schema_with_repo,
    transform,
)
from confluent_kafka.schema_registry.common.schema_registry_client import RulePhase
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.serde import (
    AsyncBaseDeserializer,
    AsyncBaseSerializer,
    ParsedSchemaCache,
    SchemaId,
)
from confluent_kafka.serialization import SerializationContext, SerializationError

__all__ = [
    '_resolve_named_schema',
    'AsyncAvroSerializer',
    'AsyncAvroDeserializer',
]


async def _resolve_named_schema(
    schema: Schema, schema_registry_client: AsyncSchemaRegistryClient
) -> Dict[str, AvroSchema]:
    """
    Resolves named schemas referenced by the provided schema recursively.
    :param schema: Schema to resolve named schemas for.
    :param schema_registry_client: SchemaRegistryClient to use for retrieval.
    :return: named_schemas dict.
    """
    named_schemas: Dict[str, AvroSchema] = {}
    if schema.references is not None:
        for ref in schema.references:
            if ref.subject is None or ref.version is None:
                raise TypeError("Subject or version cannot be None")
            referenced_schema = await schema_registry_client.get_version(ref.subject, ref.version, True)
            ref_named_schemas = await _resolve_named_schema(referenced_schema.schema, schema_registry_client)
            if referenced_schema.schema.schema_str is None:
                raise TypeError("Schema string cannot be None")
            if ref.name is None:
                raise TypeError("Name cannot be None")
            named_schemas.update(ref_named_schemas)
            # Store the raw (unparsed) schema dict. Pre-parsing here would inline
            # any sub-references inside this schema; if the same sub-reference is
            # also reachable through another sibling reference (a "diamond"
            # dependency), the top-level load_schema would then inject duplicate
            # inline definitions and fail with "redefined named type". Keeping
            # the raw form lets the top-level load_schema resolve every named
            # type exactly once.
            raw_schema = json.loads(referenced_schema.schema.schema_str)
            named_schemas[ref.name] = raw_schema
            # Also store under fully-qualified name so fastavro can resolve
            # namespace-qualified type references
            if isinstance(raw_schema, dict) and 'name' in raw_schema:
                ns = raw_schema.get('namespace')
                name = raw_schema['name']
                fqn = f"{ns}.{name}" if ns and '.' not in name else name
                if fqn != ref.name:
                    named_schemas[fqn] = raw_schema
    return named_schemas


@asyncinit
class AsyncAvroSerializer(AsyncBaseSerializer):
    """
    Serializer that outputs Avro binary encoded data with Confluent Schema Registry framing.

    Configuration properties:

    +-----------------------------------+----------+--------------------------------------------------+
    | Property Name                     | Type     | Description                                      |
    +===================================+==========+==================================================+
    | ``auto.register.schemas``         | bool     | If True, automatically register the configured   |
    |                                   |          | schema with Confluent Schema Registry if it has  |
    |                                   |          | not previously been associated with the relevant |
    |                                   |          | subject (determined via subject.name.strategy).  |
    |                                   |          |                                                  |
    |                                   |          | Defaults to True.                                |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``normalize.schemas``             | bool     | Whether to normalize schemas, which will         |
    |                                   |          | transform schemas to have a consistent format,   |
    |                                   |          | including ordering properties and references.    |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``use.schema.id``                 | int      | Whether to use the given schema ID for           |
    |                                   |          | serialization.                                   |
    |                                   |          |                                                  |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``use.latest.version``            | bool     | Whether to use the latest subject version for    |
    |                                   |          | serialization.                                   |
    |                                   |          |                                                  |
    |                                   |          | WARNING: There is no check that the latest       |
    |                                   |          | schema is backwards compatible with the object   |
    |                                   |          | being serialized.                                |
    |                                   |          |                                                  |
    |                                   |          | Defaults to False.                               |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``use.latest.with.metadata``      | dict     | Whether to use the latest subject version with   |
    |                                   |          | the given metadata.                              |
    |                                   |          |                                                  |
    |                                   |          | WARNING: There is no check that the latest       |
    |                                   |          | schema is backwards compatible with the object   |
    |                                   |          | being serialized.                                |
    |                                   |          |                                                  |
    |                                   |          | Defaults to None.                                |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``subject.name.strategy.type``    | str      | The type of subject name strategy to use.        |
    |                                   |          | Valid values are: TOPIC, RECORD, TOPIC_RECORD,   |
    |                                   |          | ASSOCIATED.                                      |
    |                                   |          |                                                  |
    |                                   |          | Defaults to ASSOCIATED if neither this nor       |
    |                                   |          | subject.name.strategy is specified.              |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``subject.name.strategy.conf``    | dict     | Configuration dictionary passed to strategies    |
    |                                   |          | that require additional configuration, such as   |
    |                                   |          | ASSOCIATED.                                      |
    |                                   |          |                                                  |
    |                                   |          | Defaults to None.                                |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``subject.name.strategy``         | callable | Callable(SerializationContext, str) -> str       |
    |                                   |          |                                                  |
    |                                   |          | Defines how Schema Registry subject names are    |
    |                                   |          | constructed. Standard naming strategies are      |
    |                                   |          | defined in the confluent_kafka.schema_registry   |
    |                                   |          | namespace. Takes precedence over                 |
    |                                   |          | subject.name.strategy.type if both are set.      |
    |                                   |          |                                                  |
    |                                   |          | Defaults to None.                                |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``schema.id.serializer``          | callable | Callable(bytes, SerializationContext, schema_id) |
    |                                   |          |   -> bytes                                       |
    |                                   |          |                                                  |
    |                                   |          | Defines how the schema id/guid is serialized.    |
    |                                   |          | Defaults to prefix_schema_id_serializer.         |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``validate.strict``               | bool     | If set to True, an error will be raised if       |
    |                                   |          | records do not contain exactly the same          |
    |                                   |          | fields that the schema states.                   |
    |                                   |          |                                                  |
    |                                   |          | Defaults to False.                               |
    +-----------------------------------+----------+--------------------------------------------------+
    | ``validate.strict.allow.default`` | bool     | If set to True, an error will be raised          |
    |                                   |          | if records do not contain exactly the same       |
    |                                   |          | fields that the schema states, unless it is a    |
    |                                   |          | missing field that has a default value in the    |
    |                                   |          | schema.                                          |
    |                                   |          |                                                  |
    |                                   |          | Defaults to False.                               |
    +-----------------------------------+----------+--------------------------------------------------+

    Schemas are registered against subject names in Confluent Schema Registry that
    define a scope in which the schemas can be evolved. By default, the subject name
    is formed by concatenating the topic name with the message field (key or value)
    separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    ``subject.name.strategy``.

    Supported subject name strategies:

    +--------------------------------------+------------------------------+
    | Subject Name Strategy                | Output Format                |
    +======================================+==============================+
    | topic_subject_name_strategy(default) | {topic name}-{message field} |
    +--------------------------------------+------------------------------+
    | topic_record_subject_name_strategy   | {topic name}-{record name}   |
    +--------------------------------------+------------------------------+
    | record_subject_name_strategy         | {record name}                |
    +--------------------------------------+------------------------------+

    See `Subject name strategy <https://docs.confluent.io/current/schema-registry/serializer-formatter.html#subject-name-strategy>`_ for additional details.

    Note:
        Prior to serialization, all values must first be converted to
        a dict instance. This may handled manually prior to calling
        :py:func:`Producer.produce()` or by registering a `to_dict`
        callable with AvroSerializer.

        See ``avro_producer.py`` in the examples directory for example usage.

    Note:
       Tuple notation can be used to determine which branch of an ambiguous union to take.

       See `fastavro notation <https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take>`_

    Args:
        schema_registry_client (SchemaRegistryClient): Schema Registry client instance.

        schema_str (str or Schema):
            Avro `Schema Declaration. <https://avro.apache.org/docs/current/spec.html#schemas>`_
            Accepts either a string or a :py:class:`Schema` instance. Note that string
            definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.

        to_dict (callable, optional): Callable(object, SerializationContext) -> dict. Converts object to a dict.

        conf (dict): AvroSerializer configuration.
    """  # noqa: E501

    __slots__ = [
        '_known_subjects',
        '_parsed_schema',
        '_schema',
        '_schema_id',
        '_schema_name',
        '_to_dict',
        '_parsed_schemas',
        '_strict',
        '_strict_allow_default',
    ]

    _default_conf = {
        'auto.register.schemas': True,
        'normalize.schemas': False,
        'use.schema.id': None,
        'use.latest.version': False,
        'use.latest.with.metadata': None,
        'subject.name.strategy.type': None,
        'subject.name.strategy.conf': None,
        'subject.name.strategy': None,
        'schema.id.serializer': prefix_schema_id_serializer,
        'validate.strict': False,
        'validate.strict.allow.default': False,
    }

    async def __init_impl(
        self,
        schema_registry_client: AsyncSchemaRegistryClient,
        schema_str: Union[str, Schema, None] = None,
        to_dict: Optional[Callable[[object, SerializationContext], dict]] = None,
        conf: Optional[dict] = None,
        rule_conf: Optional[dict] = None,
        rule_registry: Optional[RuleRegistry] = None,
    ):
        super().__init__()
        if isinstance(schema_str, str):
            schema = _schema_loads(schema_str)
        elif isinstance(schema_str, Schema):
            schema = schema_str
        else:
            schema = None

        self._registry = schema_registry_client
        self._schema_id: Optional[SchemaId] = None
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
        self._known_subjects: set[str] = set()
        self._parsed_schemas = ParsedSchemaCache()

        if to_dict is not None and not callable(to_dict):
            raise ValueError(
                "to_dict must be callable with the signature " "to_dict(object, SerializationContext)->dict"
            )

        self._to_dict = to_dict

        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = cast(bool, conf_copy.pop('auto.register.schemas'))
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._normalize_schemas = cast(bool, conf_copy.pop('normalize.schemas'))
        if not isinstance(self._normalize_schemas, bool):
            raise ValueError("normalize.schemas must be a boolean value")

        self._use_schema_id = cast(Optional[int], conf_copy.pop('use.schema.id'))
        if self._use_schema_id is not None and not isinstance(self._use_schema_id, int):
            raise ValueError("use.schema.id must be an int value")

        self._use_latest_version = cast(bool, conf_copy.pop('use.latest.version'))
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")
        if self._use_latest_version and self._auto_register:
            raise ValueError("cannot enable both use.latest.version and auto.register.schemas")

        self._use_latest_with_metadata = cast(Optional[dict], conf_copy.pop('use.latest.with.metadata'))
        if self._use_latest_with_metadata is not None and not isinstance(self._use_latest_with_metadata, dict):
            raise ValueError("use.latest.with.metadata must be a dict value")

        self.configure_subject_name_strategy(
            subject_name_strategy_type=cast(Any, conf_copy.pop('subject.name.strategy.type')),
            subject_name_strategy_conf=cast(Any, conf_copy.pop('subject.name.strategy.conf')),
            subject_name_strategy=cast(Any, conf_copy.pop('subject.name.strategy')),
        )

        self._schema_id_serializer = cast(
            Callable[[bytes, Optional[SerializationContext], Any], bytes], conf_copy.pop('schema.id.serializer')
        )
        if not callable(self._schema_id_serializer):
            raise ValueError("schema.id.serializer must be callable")

        self._strict = cast(bool, conf_copy.pop('validate.strict'))
        if not isinstance(self._strict, bool):
            raise ValueError("validate.strict must be a boolean value")

        self._strict_allow_default = cast(bool, conf_copy.pop('validate.strict.allow.default'))
        if not isinstance(self._strict_allow_default, bool):
            raise ValueError("validate.strict.allow.default must be a boolean value")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}".format(", ".join(conf_copy.keys())))

        if schema:
            parsed_schema = await self._get_parsed_schema(schema)

            if isinstance(parsed_schema, list):
                # if parsed_schema is a list, we have an Avro union and there
                # is no valid schema name. This is fine because the only use of
                # schema_name is for supplying the subject name to the registry
                # and union types should use topic_subject_name_strategy, which
                # just discards the schema name anyway
                schema_name = None
            elif isinstance(parsed_schema, dict):
                # The Avro spec states primitives have a name equal to their type
                # i.e. {"type": "string"} has a name of string.
                # This function does not comply.
                # https://github.com/fastavro/fastavro/issues/415
                if schema.schema_str is not None:
                    schema_dict = json.loads(schema.schema_str)
                    schema_name = parsed_schema.get("name", schema_dict.get("type"))
                else:
                    schema_name = None
            else:
                schema_name = None
        else:
            schema_name = None
            parsed_schema = None

        self._schema = schema
        self._schema_name = schema_name
        self._parsed_schema = parsed_schema

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {}, rule_conf if rule_conf else {})

    __init__ = __init_impl

    def __call__(  # type: ignore[override]
        self, obj: object, ctx: Optional[SerializationContext] = None
    ) -> Coroutine[Any, Any, Optional[bytes]]:
        return self.__serialize(obj, ctx)

    async def __serialize(self, obj: object, ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        """
        Serializes an object to Avro binary format, prepending it with Confluent
        Schema Registry framing.

        Args:
            obj (object): The object instance to serialize.

            ctx (SerializationContext): Metadata pertaining to the serialization operation.

        Raises:
            TypeError or ValueError: If any error occurs serializing obj.
            SchemaRegistryError: If there was an error registering the schema with
                                 Schema Registry, or auto.register.schemas is
                                 false and the schema was not registered.

        Returns:
            bytes: Confluent Schema Registry encoded Avro bytes
        """

        if obj is None:
            return None

        subject = (
            await self._subject_name_func(ctx, self._schema_name, self._registry, self._subject_name_conf)
            if self._strategy_accepts_client
            else self._subject_name_func(ctx, self._schema_name)
        )
        latest_schema = await self._get_reader_schema(subject) if subject else None
        if latest_schema is not None:
            self._schema_id = SchemaId(AVRO_TYPE, latest_schema.schema_id, latest_schema.guid)
        elif subject is not None and subject not in self._known_subjects:
            # Check to ensure this schema has been registered under subject_name.
            if self._auto_register:
                # The schema name will always be the same. We can't however register
                # a schema without a subject so we set the schema_id here to handle
                # the initial registration.
                registered_schema = await self._registry.register_schema_full_response(
                    subject, self._schema, normalize_schemas=self._normalize_schemas
                )
                self._schema_id = SchemaId(AVRO_TYPE, registered_schema.schema_id, registered_schema.guid)
            else:
                registered_schema = await self._registry.lookup_schema(
                    subject, self._schema, normalize_schemas=self._normalize_schemas
                )
                self._schema_id = SchemaId(AVRO_TYPE, registered_schema.schema_id, registered_schema.guid)

            self._known_subjects.add(subject)

        value: Any
        parsed_schema: Any
        if self._to_dict is not None:
            if ctx is None:
                raise TypeError("SerializationContext cannot be None")
            value = self._to_dict(obj, ctx)
        else:
            value = obj

        if latest_schema is not None and ctx is not None and subject is not None:
            parsed_schema = await self._get_parsed_schema(latest_schema.schema)

            expanded_parsed_schema = expand_schema(parsed_schema)

            def field_transformer(rule_ctx, field_transform, msg):
                return transform(rule_ctx, expanded_parsed_schema, msg, field_transform)  # noqa: E731

            value = self._execute_rules(
                ctx,
                subject,
                RuleMode.WRITE,
                None,
                latest_schema.schema,
                value,
                get_inline_tags(parsed_schema),
                field_transformer,
            )
        else:
            parsed_schema = self._parsed_schema

        with _ContextStringIO() as fo:
            # Check if it's a simple bytes type
            is_bytes = parsed_schema == "bytes" or (
                isinstance(parsed_schema, dict) and parsed_schema.get("type") == "bytes"
            )
            if is_bytes:
                # For simple bytes type, write value directly
                buffer = value if isinstance(value, bytes) else value.encode()
            else:
                # write the record to the rest of the buffer
                schemaless_writer(
                    fo, parsed_schema, value, strict=self._strict, strict_allow_default=self._strict_allow_default
                )
                buffer = fo.getvalue()

            if latest_schema is not None and ctx is not None and subject is not None:
                buffer = self._execute_rules_with_phase(
                    ctx, subject, RulePhase.ENCODING, RuleMode.WRITE, None, latest_schema.schema, buffer, None, None
                )

            return self._schema_id_serializer(buffer, ctx, self._schema_id)

    async def _get_parsed_schema(self, schema: Schema) -> AvroSchema:
        parsed_schema = self._parsed_schemas.get_parsed_schema(schema)
        if parsed_schema is not None:
            return parsed_schema

        named_schemas = await _resolve_named_schema(schema, self._registry)
        if schema.schema_str is None:
            raise TypeError("Schema string cannot be None")
        prepared_schema = _schema_loads(schema.schema_str)
        if prepared_schema.schema_str is None:
            raise TypeError("Prepared schema string cannot be None")
        parsed_schema = parse_schema_with_repo(prepared_schema.schema_str, named_schemas=named_schemas)

        self._parsed_schemas.set(schema, parsed_schema)
        return parsed_schema


@asyncinit
class AsyncAvroDeserializer(AsyncBaseDeserializer):
    """
    Deserializer for Avro binary encoded data with Confluent Schema Registry
    framing.

    +----------------------------------+----------+--------------------------------------------------+
    | Property Name                    | Type     | Description                                      |
    +----------------------------------+----------+--------------------------------------------------+
    |                                  |          | Whether to use the latest subject version for    |
    | ``use.latest.version``           | bool     | deserialization.                                 |
    |                                  |          |                                                  |
    |                                  |          | Defaults to False.                               |
    +----------------------------------+----------+--------------------------------------------------+
    |                                  |          | Whether to use the latest subject version with   |
    | ``use.latest.with.metadata``     | dict     | the given metadata.                              |
    |                                  |          |                                                  |
    |                                  |          | Defaults to None.                                |
    +----------------------------------+----------+--------------------------------------------------+
    |                                  |          | The type of subject name strategy to use.        |
    | ``subject.name.strategy.type``   | str      | Valid values are: TOPIC, RECORD, TOPIC_RECORD,   |
    |                                  |          | ASSOCIATED.                                      |
    |                                  |          |                                                  |
    |                                  |          | Defaults to ASSOCIATED if neither this nor       |
    |                                  |          | subject.name.strategy is specified.              |
    +----------------------------------+----------+--------------------------------------------------+
    |                                  |          | Configuration dictionary passed to strategies    |
    | ``subject.name.strategy.conf``   | dict     | that require additional configuration, such as   |
    |                                  |          | ASSOCIATED.                                      |
    |                                  |          |                                                  |
    |                                  |          | Defaults to None.                                |
    +----------------------------------+----------+--------------------------------------------------+
    |                                  |          | Callable(SerializationContext, str) -> str       |
    |                                  |          |                                                  |
    | ``subject.name.strategy``        | callable | Defines how Schema Registry subject names are    |
    |                                  |          | constructed. Standard naming strategies are      |
    |                                  |          | defined in the confluent_kafka.schema_registry   |
    |                                  |          | namespace. Takes precedence over                 |
    |                                  |          | subject.name.strategy.type if both are set.      |
    |                                  |          |                                                  |
    |                                  |          | Defaults to None.                                |
    +----------------------------------+----------+--------------------------------------------------+
    |                                  |          | Callable(bytes, SerializationContext, schema_id) |
    |                                  |          |   -> io.BytesIO                                  |
    |                                  |          |                                                  |
    | ``schema.id.deserializer``       | callable | Defines how the schema id/guid is deserialized.  |
    |                                  |          | Defaults to dual_schema_id_deserializer.         |
    +----------------------------------+----------+--------------------------------------------------+

    Note:
        By default, Avro complex types are returned as dicts. This behavior can
        be overridden by registering a callable ``from_dict`` with the deserializer to
        convert the dicts to the desired type.

        See ``avro_consumer.py`` in the examples directory in the examples
        directory for example usage.

    Args:
        schema_registry_client (SchemaRegistryClient): Confluent Schema Registry
            client instance.

        schema_str (str, Schema, optional): Avro reader schema declaration Accepts
            either a string or a :py:class:`Schema` instance. If not provided, the
            writer schema will be used as the reader schema. Note that string
            definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.

        from_dict (callable, optional): Callable(dict, SerializationContext) -> object.
            Converts a dict to an instance of some object.

        return_record_name (bool): If True, when reading a union of records, the result will
                                   be a tuple where the first value is the name of the record and the second value is
                                   the record itself.  Defaults to False.

    See Also:
        `Apache Avro Schema Declaration <https://avro.apache.org/docs/current/spec.html#schemas>`_

        `Apache Avro Schema Resolution <https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution>`_
    """

    __slots__ = ['_reader_schema', '_from_dict', '_return_record_name', '_schema', '_parsed_schemas']

    _default_conf = {
        'use.latest.version': False,
        'use.latest.with.metadata': None,
        'subject.name.strategy.type': None,
        'subject.name.strategy.conf': None,
        'subject.name.strategy': None,
        'schema.id.deserializer': dual_schema_id_deserializer,
    }

    async def __init_impl(
        self,
        schema_registry_client: AsyncSchemaRegistryClient,
        schema_str: Union[str, Schema, None] = None,
        from_dict: Optional[Callable[[dict, SerializationContext], object]] = None,
        return_record_name: bool = False,
        conf: Optional[dict] = None,
        rule_conf: Optional[dict] = None,
        rule_registry: Optional[RuleRegistry] = None,
    ):
        super().__init__()
        schema = None
        if schema_str is not None:
            if isinstance(schema_str, str):
                schema = _schema_loads(schema_str)
            elif isinstance(schema_str, Schema):
                schema = schema_str
            else:
                raise TypeError('You must pass either schema string or schema object')

        self._schema = schema
        self._registry = schema_registry_client
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
        self._parsed_schemas = ParsedSchemaCache()
        self._use_schema_id = None

        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._use_latest_version = cast(bool, conf_copy.pop('use.latest.version'))
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")

        self._use_latest_with_metadata = cast(Optional[dict], conf_copy.pop('use.latest.with.metadata'))
        if self._use_latest_with_metadata is not None and not isinstance(self._use_latest_with_metadata, dict):
            raise ValueError("use.latest.with.metadata must be a dict value")

        self.configure_subject_name_strategy(
            subject_name_strategy_type=cast(Any, conf_copy.pop('subject.name.strategy.type')),
            subject_name_strategy_conf=cast(Any, conf_copy.pop('subject.name.strategy.conf')),
            subject_name_strategy=cast(Any, conf_copy.pop('subject.name.strategy')),
        )

        self._schema_id_deserializer = cast(
            Callable[[bytes, Optional[SerializationContext], Any], io.BytesIO], conf_copy.pop('schema.id.deserializer')
        )
        if not callable(self._schema_id_deserializer):
            raise ValueError("schema.id.deserializer must be callable")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}".format(", ".join(conf_copy.keys())))

        self._reader_schema: Optional[AvroSchema]
        if schema and self._schema is not None:
            self._reader_schema = await self._get_parsed_schema(self._schema)
        else:
            self._reader_schema = None

        if from_dict is not None and not callable(from_dict):
            raise ValueError(
                "from_dict must be callable with the signature " "from_dict(SerializationContext, dict) -> object"
            )
        self._from_dict = from_dict

        self._return_record_name = return_record_name
        if not isinstance(self._return_record_name, bool):
            raise ValueError("return_record_name must be a boolean value")

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {}, rule_conf if rule_conf else {})

    __init__ = __init_impl

    def __call__(
        self, data: Optional[bytes], ctx: Optional[SerializationContext] = None
    ) -> Coroutine[Any, Any, Union[dict, object, None]]:
        return self.__deserialize(data, ctx)

    async def __deserialize(
        self, data: Optional[bytes], ctx: Optional[SerializationContext] = None
    ) -> Union[dict, object, None]:
        """
        Deserialize Avro binary encoded data with Confluent Schema Registry framing to
        a dict, or object instance according to from_dict, if specified.

        Arguments:
            data (bytes): bytes

            ctx (SerializationContext): Metadata relevant to the serialization
                operation.

        Raises:
            SerializerError: if an error occurs parsing data.

        Returns:
            object: If data is None, then None. Else, a dict, or object instance according
                    to from_dict, if specified.
        """  # noqa: E501

        if data is None:
            return None

        if len(data) <= 5:
            raise SerializationError(
                "Expecting data framing of length 6 bytes or "
                "more but total data size is {} bytes. This "
                "message was not produced with a Confluent "
                "Schema Registry serializer".format(len(data))
            )

        subject = (
            (
                await self._subject_name_func(ctx, None, self._registry, self._subject_name_conf)
                if self._strategy_accepts_client
                else self._subject_name_func(ctx, None)
            )
            if ctx
            else None
        )
        latest_schema = None
        if subject is not None:
            latest_schema = await self._get_reader_schema(subject)

        schema_id = SchemaId(AVRO_TYPE)
        payload = self._schema_id_deserializer(data, ctx, schema_id)

        writer_schema_raw = await self._get_writer_schema(schema_id, subject)
        writer_schema = await self._get_parsed_schema(writer_schema_raw)
        if subject is None and isinstance(writer_schema, dict):
            subject = (
                (
                    await self._subject_name_func(
                        ctx, writer_schema.get("name"), self._registry, self._subject_name_conf
                    )
                    if self._strategy_accepts_client
                    else self._subject_name_func(ctx, writer_schema.get("name"))
                )
                if ctx
                else None
            )
            if subject is not None:
                latest_schema = await self._get_reader_schema(subject)

        if ctx is not None and subject is not None:
            payload = self._execute_rules_with_phase(
                ctx, subject, RulePhase.ENCODING, RuleMode.READ, None, writer_schema_raw, payload, None, None
            )
        if isinstance(payload, bytes):
            payload = io.BytesIO(payload)

        reader_schema: AvroSchema
        if latest_schema is not None and subject is not None:
            migrations = await self._get_migrations(subject, writer_schema_raw, latest_schema, None)
            reader_schema_raw = latest_schema.schema
            reader_schema = await self._get_parsed_schema(latest_schema.schema)
        elif self._schema is not None and self._reader_schema is not None:
            migrations = None
            reader_schema_raw = self._schema
            reader_schema = self._reader_schema
        else:
            migrations = None
            reader_schema_raw = writer_schema_raw
            reader_schema = writer_schema

        # Check if it's a simple bytes type
        is_bytes = writer_schema == "bytes" or (
            isinstance(writer_schema, dict) and writer_schema.get("type") == "bytes"
        )

        if migrations and ctx is not None and subject is not None:
            if is_bytes:
                # For simple bytes type, read payload directly
                obj_dict = payload.read()
            else:
                obj_dict = schemaless_reader(payload, writer_schema, None, self._return_record_name)
            obj_dict = self._execute_migrations(ctx, subject, migrations, obj_dict)
        else:
            if is_bytes:
                # For simple bytes type, read payload directly
                obj_dict = payload.read()
            else:
                obj_dict = schemaless_reader(payload, writer_schema, reader_schema, self._return_record_name)

        expanded_reader_schema = expand_schema(reader_schema)

        def field_transformer(rule_ctx, field_transform, message):
            return transform(rule_ctx, expanded_reader_schema, message, field_transform)  # noqa: E731

        if ctx is not None and subject is not None:
            inline_tags = get_inline_tags(reader_schema) if reader_schema is not None else None
            obj_dict = self._execute_rules(
                ctx, subject, RuleMode.READ, None, reader_schema_raw, obj_dict, inline_tags, field_transformer
            )

        if self._from_dict is not None:
            if ctx is None:
                raise TypeError("SerializationContext cannot be None")
            return self._from_dict(obj_dict, ctx)

        return obj_dict

    async def _get_parsed_schema(self, schema: Schema) -> AvroSchema:
        parsed_schema = self._parsed_schemas.get_parsed_schema(schema)
        if parsed_schema is not None:
            return parsed_schema

        named_schemas = await _resolve_named_schema(schema, self._registry)
        if schema.schema_str is None:
            raise TypeError("Schema string cannot be None")
        prepared_schema = _schema_loads(schema.schema_str)
        if prepared_schema.schema_str is None:
            raise TypeError("Prepared schema string cannot be None")
        parsed_schema = parse_schema_with_repo(prepared_schema.schema_str, named_schemas=named_schemas)

        self._parsed_schemas.set(schema, parsed_schema)
        return parsed_schema
