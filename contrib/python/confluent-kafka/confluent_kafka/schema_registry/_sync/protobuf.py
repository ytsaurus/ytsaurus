#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020-2022 Confluent Inc.
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
from typing import Any, Callable, List, Optional, Set, Tuple, Union, cast

from google.protobuf import descriptor_pb2, json_format
from google.protobuf.descriptor import Descriptor, FileDescriptor
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message import DecodeError, Message
from google.protobuf.message_factory import GetMessageClass

from confluent_kafka.schema_registry import (
    RuleMode,
    Schema,
    SchemaReference,
    dual_schema_id_deserializer,
    prefix_schema_id_serializer,
    reference_subject_name_strategy,
)
from confluent_kafka.schema_registry.common.protobuf import (
    PROTOBUF_TYPE,
    _bytes,
    _ContextStringIO,
    _create_index_array,
    _init_pool,
    _is_builtin,
    _schema_to_str,
    _str_to_proto,
    transform,
)
from confluent_kafka.schema_registry.common.schema_registry_client import RulePhase
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.serde import (
    BaseDeserializer,
    BaseSerializer,
    ParsedSchemaCache,
    SchemaId,
)
from confluent_kafka.serialization import SerializationContext, SerializationError

__all__ = [
    '_resolve_named_schema',
    'ProtobufSerializer',
    'ProtobufDeserializer',
]


def _resolve_named_schema(
    schema: Schema,
    schema_registry_client: SchemaRegistryClient,
    pool: DescriptorPool,
    visited: Optional[Set[str]] = None,
):
    """
    Resolves named schemas referenced by the provided schema recursively.

    :param schema: Schema to resolve named schemas for.
    :param schema_registry_client: SchemaRegistryClient to use for retrieval.
    :param pool: DescriptorPool to add resolved schemas to.
    :return: DescriptorPool
    """
    if visited is None:
        visited = set()
    if schema.references is not None:
        for ref in schema.references:
            if ref.name is None:
                raise ValueError("Name cannot be None")

            if _is_builtin(ref.name) or ref.name in visited:
                continue
            visited.add(ref.name)

            if ref.subject is None or ref.version is None:
                raise ValueError("Subject or version cannot be None")
            referenced_schema = schema_registry_client.get_version(ref.subject, ref.version, True, 'serialized')
            if referenced_schema.schema.schema_str is None:
                raise ValueError("Schema string cannot be None")
            _resolve_named_schema(referenced_schema.schema, schema_registry_client, pool, visited)
            file_descriptor_proto = _str_to_proto(ref.name, referenced_schema.schema.schema_str)
            pool.Add(file_descriptor_proto)


class ProtobufSerializer(BaseSerializer):
    """
    Serializer for Protobuf Message derived classes. Serialization format is Protobuf,
    with Confluent Schema Registry framing.

    Configuration properties:

    +-------------------------------------+----------+------------------------------------------------------+
    | Property Name                       | Type     | Description                                          |
    +=====================================+==========+======================================================+
    |                                     |          | If True, automatically register the configured       |
    | ``auto.register.schemas``           | bool     | schema with Confluent Schema Registry if it has      |
    |                                     |          | not previously been associated with the relevant     |
    |                                     |          | subject (determined via subject.name.strategy).      |
    |                                     |          |                                                      |
    |                                     |          | Defaults to True.                                    |
    |                                     |          |                                                      |
    |                                     |          | Raises SchemaRegistryError if the schema was not     |
    |                                     |          | registered against the subject, or could not be      |
    |                                     |          | successfully registered.                             |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to normalize schemas, which will             |
    | ``normalize.schemas``               | bool     | transform schemas to have a consistent format,       |
    |                                     |          | including ordering properties and references.        |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the given schema ID for               |
    | ``use.schema.id``                   | int      | serialization.                                       |
    |                                     |          |                                                      |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the latest subject version for        |
    | ``use.latest.version``              | bool     | serialization.                                       |
    |                                     |          |                                                      |
    |                                     |          | WARNING: There is no check that the latest           |
    |                                     |          | schema is backwards compatible with the object       |
    |                                     |          | being serialized.                                    |
    |                                     |          |                                                      |
    |                                     |          | Defaults to False.                                   |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the latest subject version with       |
    | ``use.latest.with.metadata``        | dict     | the given metadata.                                  |
    |                                     |          |                                                      |
    |                                     |          | WARNING: There is no check that the latest           |
    |                                     |          | schema is backwards compatible with the object       |
    |                                     |          | being serialized.                                    |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether or not to skip known types when resolving    |
    | ``skip.known.types``                | bool     | schema dependencies.                                 |
    |                                     |          |                                                      |
    |                                     |          | Defaults to True.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | The type of subject name strategy to use.            |
    | ``subject.name.strategy.type``      | str      | Valid values are: TOPIC, RECORD, TOPIC_RECORD,       |
    |                                     |          | ASSOCIATED.                                          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to ASSOCIATED if neither this nor           |
    |                                     |          | subject.name.strategy is specified.                  |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Configuration dictionary passed to strategies        |
    | ``subject.name.strategy.conf``      | dict     | that require additional configuration, such as       |
    |                                     |          | ASSOCIATED.                                          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``subject.name.strategy``           | callable | Defines how Schema Registry subject names are        |
    |                                     |          | constructed. Standard naming strategies are          |
    |                                     |          | defined in the confluent_kafka.schema_registry       |
    |                                     |          | namespace. Takes precedence over                     |
    |                                     |          | subject.name.strategy.type if both are set.          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``reference.subject.name.strategy`` | callable | Defines how Schema Registry subject names for schema |
    |                                     |          | references are constructed.                          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to reference_subject_name_strategy          |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(bytes, SerializationContext, schema_id)     |
    |                                     |          |   -> bytes                                           |
    |                                     |          |                                                      |
    | ``schema.id.serializer``            | callable | Defines how the schema id/guid is serialized.        |
    |                                     |          | Defaults to prefix_schema_id_serializer.             |
    +-------------------------------------+----------+------------------------------------------------------+

    Schemas are registered against subject names in Confluent Schema Registry that
    define a scope in which the schemas can be evolved. By default, the subject name
    is formed by concatenating the topic name with the message field (key or value)
    separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    ``subject.name.strategy``.

    Supported subject name strategies

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

    Args:
        msg_type (Message): Protobuf Message type.

        schema_registry_client (SchemaRegistryClient): Schema Registry
            client instance.

        conf (dict): ProtobufSerializer configuration.

    See Also:
        `Protobuf API reference <https://googleapis.dev/python/protobuf/latest/google/protobuf.html>`_
    """  # noqa: E501

    __slots__ = [
        '_skip_known_types',
        '_known_subjects',
        '_msg_class',
        '_index_array',
        '_schema',
        '_schema_id',
        '_ref_reference_subject_func',
        '_use_deprecated_format',
        '_parsed_schemas',
    ]

    _default_conf = {
        'auto.register.schemas': True,
        'normalize.schemas': False,
        'use.schema.id': None,
        'use.latest.version': False,
        'use.latest.with.metadata': None,
        'skip.known.types': True,
        'subject.name.strategy.type': None,
        'subject.name.strategy.conf': None,
        'subject.name.strategy': None,
        'reference.subject.name.strategy': reference_subject_name_strategy,
        'schema.id.serializer': prefix_schema_id_serializer,
        'use.deprecated.format': False,
    }

    def __init_impl(
        self,
        msg_type: Message,
        schema_registry_client: SchemaRegistryClient,
        conf: Optional[dict] = None,
        rule_conf: Optional[dict] = None,
        rule_registry: Optional[RuleRegistry] = None,
    ):
        super().__init__()

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

        self._skip_known_types = cast(bool, conf_copy.pop('skip.known.types'))
        if not isinstance(self._skip_known_types, bool):
            raise ValueError("skip.known.types must be a boolean value")

        self._use_deprecated_format = cast(bool, conf_copy.pop('use.deprecated.format'))
        if not isinstance(self._use_deprecated_format, bool):
            raise ValueError("use.deprecated.format must be a boolean value")
        if self._use_deprecated_format:
            raise ValueError("use.deprecated.format is no longer supported")

        self.configure_subject_name_strategy(
            subject_name_strategy_type=cast(Any, conf_copy.pop('subject.name.strategy.type')),
            subject_name_strategy_conf=cast(Any, conf_copy.pop('subject.name.strategy.conf')),
            subject_name_strategy=cast(Any, conf_copy.pop('subject.name.strategy')),
        )

        self._ref_reference_subject_func = cast(
            Callable[[Optional[SerializationContext], Any], Optional[str]],
            conf_copy.pop('reference.subject.name.strategy'),
        )
        if not callable(self._ref_reference_subject_func):
            raise ValueError("reference.subject.name.strategy must be callable")

        self._schema_id_serializer = cast(
            Callable[[bytes, Optional[SerializationContext], Any], bytes], conf_copy.pop('schema.id.serializer')
        )
        if not callable(self._schema_id_serializer):
            raise ValueError("schema.id.serializer must be callable")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}".format(", ".join(conf_copy.keys())))

        self._registry = schema_registry_client
        self._rule_registry = rule_registry if rule_registry else RuleRegistry.get_global_instance()
        self._schema_id: Optional[SchemaId] = None
        self._known_subjects: set[str] = set()
        self._msg_class = msg_type
        self._parsed_schemas = ParsedSchemaCache()

        descriptor = msg_type.DESCRIPTOR
        self._index_array = _create_index_array(descriptor)
        self._schema = Schema(_schema_to_str(descriptor.file), schema_type='PROTOBUF')

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {}, rule_conf if rule_conf else {})

    __init__ = __init_impl

    @staticmethod
    def _write_varint(buf: io.BytesIO, val: int, zigzag: bool = True):
        """
        Writes val to buf, either using zigzag or uvarint encoding.

        Args:
            buf (BytesIO): buffer to write to.
            val (int): integer to be encoded.
            zigzag (bool): whether to encode in zigzag or uvarint encoding
        """

        if zigzag:
            val = (val << 1) ^ (val >> 63)

        while (val & ~0x7F) != 0:
            buf.write(_bytes((val & 0x7F) | 0x80))
            val >>= 7
        buf.write(_bytes(val))

    @staticmethod
    def _encode_varints(buf: io.BytesIO, ints: List[int], zigzag: bool = True):
        """
        Encodes each int as a uvarint onto buf

        Args:
            buf (BytesIO): buffer to write to.
            ints ([int]): ints to be encoded.
            zigzag (bool): whether to encode in zigzag or uvarint encoding
        """

        assert len(ints) > 0
        # The root element at the 0 position does not need a length prefix.
        if ints == [0]:
            buf.write(_bytes(0x00))
            return

        ProtobufSerializer._write_varint(buf, len(ints), zigzag=zigzag)

        for value in ints:
            ProtobufSerializer._write_varint(buf, value, zigzag=zigzag)

    def _resolve_dependencies(self, ctx: SerializationContext, file_desc: FileDescriptor) -> List[SchemaReference]:
        """
        Resolves and optionally registers schema references recursively.

        Args:
            ctx (SerializationContext): Serialization context.

            file_desc (FileDescriptor): file descriptor to traverse.
        """

        schema_refs = []
        for dep in file_desc.dependencies:
            if self._skip_known_types and _is_builtin(dep.name):
                continue
            dep_refs = self._resolve_dependencies(ctx, dep)
            subject = self._ref_reference_subject_func(ctx, dep)
            schema = Schema(_schema_to_str(dep), references=dep_refs, schema_type='PROTOBUF')
            if self._auto_register:
                self._registry.register_schema(subject, schema, normalize_schemas=self._normalize_schemas)

            reference = self._registry.lookup_schema(subject, schema, normalize_schemas=self._normalize_schemas)
            # schema_refs are per file descriptor
            schema_refs.append(SchemaReference(dep.name, subject, reference.version))
        return schema_refs

    def __call__(  # type: ignore[override]
        self, message: Message, ctx: Optional[SerializationContext] = None
    ) -> Optional[bytes]:
        return self.__serialize(message, ctx)

    def __serialize(self, message: Message, ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        """
        Serializes an instance of a class derived from Protobuf Message, and prepends
        it with Confluent Schema Registry framing.

        Args:
            message (Message): An instance of a class derived from Protobuf Message.

            ctx (SerializationContext): Metadata relevant to the serialization.
                operation.

        Raises:
            SerializerError if any error occurs during serialization.

        Returns:
            None if messages is None, else a byte array containing the Protobuf
            serialized message with Confluent Schema Registry framing.
        """

        if message is None:
            return None

        if not isinstance(message, self._msg_class):
            raise ValueError("message must be of type {} not {}".format(self._msg_class, type(message)))

        subject = (
            (
                self._subject_name_func(ctx, message.DESCRIPTOR.full_name, self._registry, self._subject_name_conf)
                if self._strategy_accepts_client
                else self._subject_name_func(ctx, message.DESCRIPTOR.full_name)
            )
            if ctx
            else None
        )
        latest_schema = None
        if subject is not None:
            latest_schema = self._get_reader_schema(subject, fmt='serialized')

        if latest_schema is not None:
            self._schema_id = SchemaId(PROTOBUF_TYPE, latest_schema.schema_id, latest_schema.guid, self._index_array)

        elif subject is not None and subject not in self._known_subjects and ctx is not None:
            references = self._resolve_dependencies(ctx, message.DESCRIPTOR.file)
            self._schema = Schema(self._schema.schema_str, self._schema.schema_type, references)

            if self._auto_register:
                registered_schema = self._registry.register_schema_full_response(
                    subject, self._schema, normalize_schemas=self._normalize_schemas
                )
                self._schema_id = SchemaId(
                    PROTOBUF_TYPE, registered_schema.schema_id, registered_schema.guid, self._index_array
                )
            else:
                registered_schema = self._registry.lookup_schema(
                    subject, self._schema, normalize_schemas=self._normalize_schemas
                )
                self._schema_id = SchemaId(
                    PROTOBUF_TYPE, registered_schema.schema_id, registered_schema.guid, self._index_array
                )

            self._known_subjects.add(subject)

        if latest_schema is not None:
            fd_proto, pool = self._get_parsed_schema(latest_schema.schema)
            fd = pool.FindFileByName(fd_proto.name)
            desc = fd.message_types_by_name[message.DESCRIPTOR.name]

            def field_transformer(rule_ctx, field_transform, msg):
                return transform(rule_ctx, desc, msg, field_transform)  # noqa: E731

            if ctx is not None and subject is not None:
                message = self._execute_rules(
                    ctx, subject, RuleMode.WRITE, None, latest_schema.schema, message, None, field_transformer
                )

        with _ContextStringIO() as fo:
            fo.write(message.SerializeToString())
            if self._schema_id is not None:
                self._schema_id.message_indexes = self._index_array
            buffer = fo.getvalue()

            if latest_schema is not None and ctx is not None and subject is not None:
                buffer = self._execute_rules_with_phase(
                    ctx, subject, RulePhase.ENCODING, RuleMode.WRITE, None, latest_schema.schema, buffer, None, None
                )

            return self._schema_id_serializer(buffer, ctx, self._schema_id)

    def _get_parsed_schema(self, schema: Schema) -> Tuple[descriptor_pb2.FileDescriptorProto, DescriptorPool]:
        result = self._parsed_schemas.get_parsed_schema(schema)
        if result is not None:
            return result

        pool = DescriptorPool()
        _init_pool(pool)
        _resolve_named_schema(schema, self._registry, pool)
        if schema.schema_str is None:
            raise ValueError("Schema string cannot be None")
        fd_proto = _str_to_proto("default", schema.schema_str)
        pool.Add(fd_proto)
        self._parsed_schemas.set(schema, (fd_proto, pool))
        return fd_proto, pool


class ProtobufDeserializer(BaseDeserializer):
    """
    Deserializer for Protobuf serialized data with Confluent Schema Registry framing.

    Args:
        message_type (Message derived type): Protobuf Message type.
        conf (dict): Configuration dictionary.

    ProtobufDeserializer configuration properties:

    +-------------------------------------+----------+------------------------------------------------------+
    | Property Name                       | Type     | Description                                          |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the latest subject version for        |
    | ``use.latest.version``              | bool     | deserialization.                                     |
    |                                     |          |                                                      |
    |                                     |          | Defaults to False.                                   |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Whether to use the latest subject version with       |
    | ``use.latest.with.metadata``        | dict     | the given metadata.                                  |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | The type of subject name strategy to use.            |
    | ``subject.name.strategy.type``      | str      | Valid values are: TOPIC, RECORD, TOPIC_RECORD,       |
    |                                     |          | ASSOCIATED.                                          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to ASSOCIATED if neither this nor           |
    |                                     |          | subject.name.strategy is specified.                  |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Configuration dictionary passed to strategies        |
    | ``subject.name.strategy.conf``      | dict     | that require additional configuration, such as       |
    |                                     |          | ASSOCIATED.                                          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(SerializationContext, str) -> str           |
    |                                     |          |                                                      |
    | ``subject.name.strategy``           | callable | Defines how Schema Registry subject names are        |
    |                                     |          | constructed. Standard naming strategies are          |
    |                                     |          | defined in the confluent_kafka.schema_registry       |
    |                                     |          | namespace. Takes precedence over                     |
    |                                     |          | subject.name.strategy.type if both are set.          |
    |                                     |          |                                                      |
    |                                     |          | Defaults to None.                                    |
    +-------------------------------------+----------+------------------------------------------------------+
    |                                     |          | Callable(bytes, SerializationContext, schema_id)     |
    |                                     |          |   -> io.BytesIO                                      |
    |                                     |          |                                                      |
    | ``schema.id.deserializer``          | callable | Defines how the schema id/guid is deserialized.      |
    |                                     |          | Defaults to dual_schema_id_deserializer.             |
    +-------------------------------------+----------+------------------------------------------------------+


    See Also:
    `Protobuf API reference <https://googleapis.dev/python/protobuf/latest/google/protobuf.html>`_
    """

    __slots__ = ['_msg_class', '_use_deprecated_format', '_parsed_schemas']

    _default_conf = {
        'use.latest.version': False,
        'use.latest.with.metadata': None,
        'subject.name.strategy.type': None,
        'subject.name.strategy.conf': None,
        'subject.name.strategy': None,
        'schema.id.deserializer': dual_schema_id_deserializer,
        'use.deprecated.format': False,
    }

    def __init_impl(
        self,
        message_type: Message,
        conf: Optional[dict] = None,
        schema_registry_client: Optional[SchemaRegistryClient] = None,
        rule_conf: Optional[dict] = None,
        rule_registry: Optional[RuleRegistry] = None,
    ):
        super().__init__()

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

        self._use_deprecated_format = cast(bool, conf_copy.pop('use.deprecated.format'))
        if not isinstance(self._use_deprecated_format, bool):
            raise ValueError("use.deprecated.format must be a boolean value")
        if self._use_deprecated_format:
            raise ValueError("use.deprecated.format is no longer supported")

        descriptor = message_type.DESCRIPTOR
        self._msg_class = GetMessageClass(descriptor)

        for rule in self._rule_registry.get_executors():
            rule.configure(self._registry.config() if self._registry else {}, rule_conf if rule_conf else {})

    __init__ = __init_impl

    def __call__(self, data: Optional[bytes], ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        return self.__deserialize(data, ctx)

    def __deserialize(self, data: Optional[bytes], ctx: Optional[SerializationContext] = None) -> Optional[bytes]:
        """
        Deserialize a serialized protobuf message with Confluent Schema Registry
        framing.

        Args:
            data (bytes): Serialized protobuf message with Confluent Schema
                           Registry framing.

            ctx (SerializationContext): Metadata relevant to the serialization
                operation.

        Returns:
            Message: Protobuf Message instance.

        Raises:
            SerializerError: If there was an error reading the Confluent framing
                data, or parsing the protobuf serialized message.
        """

        if data is None:
            return None

        subject = (
            (
                self._subject_name_func(ctx, None, self._registry, self._subject_name_conf)
                if self._strategy_accepts_client
                else self._subject_name_func(ctx, None)
            )
            if ctx
            else None
        )
        latest_schema = None
        if subject is not None and self._registry is not None:
            latest_schema = self._get_reader_schema(subject, fmt='serialized')

        schema_id = SchemaId(PROTOBUF_TYPE)
        payload = self._schema_id_deserializer(data, ctx, schema_id)
        msg_index = schema_id.message_indexes

        if self._registry is not None:
            writer_schema_raw = self._get_writer_schema(schema_id, subject, fmt='serialized')
            fd_proto, pool = self._get_parsed_schema(writer_schema_raw)
            writer_schema = pool.FindFileByName(fd_proto.name)
            writer_desc = self._get_message_desc(pool, writer_schema, msg_index if msg_index is not None else [])
            if subject is None:
                subject = (
                    (
                        self._subject_name_func(ctx, writer_desc.full_name, self._registry, self._subject_name_conf)
                        if self._strategy_accepts_client
                        else self._subject_name_func(ctx, writer_desc.full_name)
                    )
                    if ctx
                    else None
                )
                if subject is not None:
                    latest_schema = self._get_reader_schema(subject, fmt='serialized')
        else:
            writer_schema_raw = None
            writer_schema = None

        if ctx is not None and subject is not None:
            payload = self._execute_rules_with_phase(
                ctx, subject, RulePhase.ENCODING, RuleMode.READ, None, writer_schema_raw, payload, None, None
            )
        if isinstance(payload, bytes):
            payload = io.BytesIO(payload)

        reader_schema_raw: Optional[Schema] = None
        if latest_schema is not None and subject is not None and writer_schema_raw is not None:
            migrations = self._get_migrations(subject, writer_schema_raw, latest_schema, None)
            reader_schema_raw = latest_schema.schema
            fd_proto, pool = self._get_parsed_schema(latest_schema.schema)
            reader_schema = pool.FindFileByName(fd_proto.name)
        else:
            migrations = None
            reader_schema_raw = writer_schema_raw
            reader_schema = writer_schema

        if reader_schema is not None:
            # Initialize reader desc to first message in file
            reader_desc = self._get_message_desc(pool, reader_schema, [0])
            # Attempt to find a reader desc with the same name as the writer
            reader_desc = reader_schema.message_types_by_name.get(writer_desc.name, reader_desc)

        if migrations and ctx is not None and subject is not None:
            msg = GetMessageClass(writer_desc)()
            try:
                msg.ParseFromString(payload.read())
            except DecodeError as e:
                raise SerializationError(str(e))

            obj_dict = json_format.MessageToDict(msg, True)
            obj_dict = self._execute_migrations(ctx, subject, migrations, obj_dict)
            msg = GetMessageClass(reader_desc)()
            msg = json_format.ParseDict(obj_dict, msg)
        else:
            # Protobuf Messages are self-describing; no need to query schema
            msg = self._msg_class()
            try:
                msg.ParseFromString(payload.read())
            except DecodeError as e:
                raise SerializationError(str(e))

        def field_transformer(rule_ctx, field_transform, message):
            return transform(rule_ctx, reader_desc, message, field_transform)  # noqa: E731

        if ctx is not None and subject is not None:
            msg = self._execute_rules(
                ctx, subject, RuleMode.READ, None, reader_schema_raw, msg, None, field_transformer
            )
        return msg

    def _get_parsed_schema(self, schema: Schema) -> Tuple[descriptor_pb2.FileDescriptorProto, DescriptorPool]:
        result = self._parsed_schemas.get_parsed_schema(schema)
        if result is not None:
            return result

        pool = DescriptorPool()
        _init_pool(pool)
        _resolve_named_schema(schema, self._registry, pool)
        if schema.schema_str is None:
            raise ValueError("Schema string cannot be None")
        fd_proto = _str_to_proto("default", schema.schema_str)
        pool.Add(fd_proto)
        self._parsed_schemas.set(schema, (fd_proto, pool))
        return fd_proto, pool

    def _get_message_desc(self, pool: DescriptorPool, fd: FileDescriptor, msg_index: List[int]) -> Descriptor:
        file_desc_proto = descriptor_pb2.FileDescriptorProto()
        fd.CopyToProto(file_desc_proto)
        full_name, desc_proto = self._get_message_desc_proto("", file_desc_proto, msg_index)
        package = file_desc_proto.package
        qualified_name = package + "." + full_name if package else full_name
        return pool.FindMessageTypeByName(qualified_name)

    def _get_message_desc_proto(
        self,
        path: str,
        desc: Union[descriptor_pb2.FileDescriptorProto, descriptor_pb2.DescriptorProto],
        msg_index: List[int],
    ) -> Tuple[str, descriptor_pb2.DescriptorProto]:
        index = msg_index[0]
        if isinstance(desc, descriptor_pb2.FileDescriptorProto):
            msg = desc.message_type[index]
            path = path + "." + msg.name if path else msg.name
            if len(msg_index) == 1:
                return path, msg
            return self._get_message_desc_proto(path, msg, msg_index[1:])
        else:
            msg = desc.nested_type[index]
            path = path + "." + msg.name if path else msg.name
            if len(msg_index) == 1:
                return path, msg
            return self._get_message_desc_proto(path, msg, msg_index[1:])
