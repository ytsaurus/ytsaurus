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
#
import io
from typing import Optional

from ..serialization import MessageField, SerializationContext, SerializationError
from .schema_registry_client import (
    AsyncSchemaRegistryClient,
    ConfigCompatibilityLevel,
    Metadata,
    MetadataProperties,
    MetadataTags,
    RegisteredSchema,
    Rule,
    RuleKind,
    RuleMode,
    RuleParams,
    RuleSet,
    Schema,
    SchemaReference,
    SchemaRegistryClient,
    SchemaRegistryError,
    ServerConfig,
)

_KEY_SCHEMA_ID = "__key_schema_id"
_VALUE_SCHEMA_ID = "__value_schema_id"

_MAGIC_BYTE = 0
_MAGIC_BYTE_V0 = _MAGIC_BYTE
_MAGIC_BYTE_V1 = 1

__all__ = [
    "ConfigCompatibilityLevel",
    "Metadata",
    "MetadataProperties",
    "MetadataTags",
    "RegisteredSchema",
    "Rule",
    "RuleKind",
    "RuleMode",
    "RuleParams",
    "RuleSet",
    "Schema",
    "SchemaRegistryClient",
    "AsyncSchemaRegistryClient",
    "SchemaRegistryError",
    "SchemaReference",
    "ServerConfig",
    "topic_subject_name_strategy",
    "topic_record_subject_name_strategy",
    "record_subject_name_strategy",
    "header_schema_id_serializer",
    "prefix_schema_id_serializer",
    "dual_schema_id_deserializer",
    "prefix_schema_id_deserializer",
]


def topic_subject_name_strategy(ctx: Optional[SerializationContext], record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-key|value.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Required** - will raise ValueError if None.

        record_name (Optional[str]): Record name.

    Raises:
        ValueError: If ctx is None.

    """
    if ctx is None:
        raise ValueError(
            "SerializationContext is required for topic_subject_name_strategy. "
            "Either provide a SerializationContext or use record_subject_name_strategy."
        )
    return ctx.topic + "-" + ctx.field


def topic_record_subject_name_strategy(
    ctx: Optional[SerializationContext], record_name: Optional[str]
) -> Optional[str]:
    """
    Constructs a subject name in the form of {topic}-{record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Required** - will raise ValueError if None.

        record_name (Optional[str]): Record name.

    Raises:
        ValueError: If ctx is None.

    """
    if ctx is None:
        raise ValueError(
            "SerializationContext is required for topic_record_subject_name_strategy. "
            "Either provide a SerializationContext or use record_subject_name_strategy."
        )
    return ctx.topic + "-" + record_name if record_name is not None else None


def record_subject_name_strategy(ctx: Optional[SerializationContext], record_name: Optional[str]) -> Optional[str]:
    """
    Constructs a subject name in the form of {record_name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Not used** by this strategy.

        record_name (Optional[str]): Record name.

    Note:
        This strategy does not require SerializationContext and can be used
        when ctx is None.

    """
    return record_name if record_name is not None else None


def reference_subject_name_strategy(ctx: Optional[SerializationContext], schema_ref: SchemaReference) -> Optional[str]:
    """
    Constructs a subject reference name in the form of {reference name}.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation. **Not used** by this strategy.

        schema_ref (SchemaReference): SchemaReference instance.

    Note:
        This strategy does not require SerializationContext and can be used
        when ctx is None.

    """
    return schema_ref.name if schema_ref is not None else None


def header_schema_id_serializer(payload: bytes, ctx: Optional[SerializationContext], schema_id) -> bytes:
    """
    Serializes the schema guid into the header.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    if ctx is None:
        raise SerializationError("SerializationContext is required for header_schema_id_serializer")

    headers = ctx.headers
    if headers is None:
        raise SerializationError("Missing headers")
    header_key = _KEY_SCHEMA_ID if ctx.field == MessageField.KEY else _VALUE_SCHEMA_ID
    header_value = schema_id.guid_to_bytes()
    if isinstance(headers, list):
        headers.append((header_key, header_value))
    elif isinstance(headers, dict):
        headers[header_key] = header_value
    else:
        raise SerializationError("Invalid headers type")
    return payload


def prefix_schema_id_serializer(payload: bytes, ctx, schema_id) -> bytes:
    """
    Serializes the schema id into the payload prefix.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload prefixed with the schema id
    """
    return schema_id.id_to_bytes() + payload


def dual_schema_id_deserializer(payload: bytes, ctx: Optional[SerializationContext], schema_id) -> io.BytesIO:
    """
    Deserializes the schema id by first checking the header, then the payload prefix.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    # Look for schema ID in headers
    header_value = None

    if ctx is not None:
        headers = ctx.headers
        if headers is not None:
            header_key = _KEY_SCHEMA_ID if ctx.field == MessageField.KEY else _VALUE_SCHEMA_ID
            if isinstance(headers, list):
                # look for header_key in headers
                for header in headers:
                    if header[0] == header_key:
                        header_value = header[1]
                        break
            elif isinstance(headers, dict):
                header_value = headers.get(header_key, None)

    # Parse schema ID from determined source and return appropriate payload
    if header_value is not None:
        schema_id.from_bytes(io.BytesIO(header_value))  # type: ignore[arg-type]
        return io.BytesIO(payload)  # Return full payload when schema ID is in header
    else:
        return schema_id.from_bytes(io.BytesIO(payload))  # Parse from payload, return remainder


def prefix_schema_id_deserializer(payload: bytes, ctx, schema_id) -> io.BytesIO:
    """
    Deserializes the schema id from the payload prefix.

    Args:
        payload (bytes): The payload to serialize.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        schema_id (SchemaId): The schema ID to serialize.

    Returns:
        bytes: The payload
    """
    return schema_id.from_bytes(io.BytesIO(payload))
