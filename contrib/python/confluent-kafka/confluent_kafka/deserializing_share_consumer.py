#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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

from typing import Any, Dict

from confluent_kafka._model import Messages
from confluent_kafka.cimpl import KafkaError, Message
from confluent_kafka.cimpl import ShareConsumer as _ShareConsumerImpl

from .serialization import MessageField, SerializationContext


class DeserializingShareConsumer(_ShareConsumerImpl):
    """
    A high level KIP-932 share consumer with deserialization capabilities.

    `This class is experimental and likely to be removed, or subject to incompatible API
    changes in future versions of the library. To avoid breaking changes on upgrading, we
    recommend using deserializers directly.`

    Derived from the :py:class:`ShareConsumer` class, overriding the
    :py:func:`ShareConsumer.poll` method to add deserialization capabilities.

    Additional configuration properties:

    +-------------------------+---------------------+-----------------------------------------------------+
    | Property Name           | Type                | Description                                         |
    +=========================+=====================+=====================================================+
    |                         |                     | Callable(bytes, SerializationContext) -> obj        |
    | ``key.deserializer``    | callable            |                                                     |
    |                         |                     | Deserializer used for message keys.                 |
    +-------------------------+---------------------+-----------------------------------------------------+
    |                         |                     | Callable(bytes, SerializationContext) -> obj        |
    | ``value.deserializer``  | callable            |                                                     |
    |                         |                     | Deserializer used for message values.               |
    +-------------------------+---------------------+-----------------------------------------------------+

    Deserializers for string, integer and double (:py:class:`StringDeserializer`, :py:class:`IntegerDeserializer`
    and :py:class:`DoubleDeserializer`) are supplied out-of-the-box in the ``confluent_kafka.serialization``
    namespace.

    Deserializers for Protobuf, JSON Schema and Avro (:py:class:`ProtobufDeserializer`, :py:class:`JSONDeserializer`
    and :py:class:`AvroDeserializer`) with Confluent Schema Registry integration are supplied out-of-the-box
    in the ``confluent_kafka.schema_registry`` namespace.

    Unlike :py:class:`DeserializingConsumer`, :py:func:`poll` returns a *list* of
    messages (mirroring :py:class:`ShareConsumer`), and a deserialization failure on
    one record does not discard the rest of the batch. A record whose key or value
    cannot be deserialized is left in the returned list with its raw bytes intact and
    its :py:func:`Message.error` set to a ``_KEY_DESERIALIZATION`` or
    ``_VALUE_DESERIALIZATION`` error, so the application can detect it with the same
    ``if msg.error():`` check it already uses for broker errors and acknowledge it
    accordingly (e.g. with :py:attr:`AcknowledgeType.REJECT`).

    Deserialization mutates each message in place, so the returned messages remain
    valid arguments to :py:func:`ShareConsumer.acknowledge` (acknowledgement is keyed
    on topic, partition and offset, which are left untouched).

    See Also:
        - The :ref:`Configuration Guide <pythonclient_configuration>` for in depth information on how to configure the client.
        - `CONFIGURATION.md <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>`_ for a comprehensive set of configuration properties.
        - The :py:class:`ShareConsumer` class for inherited methods.

    Args:
        conf (dict): DeserializingShareConsumer configuration.

    Raises:
        ValueError: if configuration validation fails
    """  # noqa: E501

    def __init__(self, conf: Dict[str, Any]) -> None:
        conf_copy = conf.copy()
        self._key_deserializer = conf_copy.pop('key.deserializer', None)
        self._value_deserializer = conf_copy.pop('value.deserializer', None)

        super(DeserializingShareConsumer, self).__init__(conf_copy)

    def poll(self, timeout: float = -1) -> Messages:
        """
        Consume messages and deserialize their keys and values in place.

        Args:
            timeout (float): Maximum time to block waiting for messages (Seconds).

        Returns:
            Messages: The polled messages. An empty Messages is
            returned if no messages are available within the timeout. Each
            message is the same object returned by the underlying
            :py:class:`ShareConsumer`, with its key and value replaced by the
            deserialized objects.

            Records that arrived with an error (``msg.error()`` is not None) are
            returned unchanged. Records whose key or value fails to deserialize are
            returned with their raw bytes preserved and ``msg.error()`` set to a
            ``_KEY_DESERIALIZATION`` or ``_VALUE_DESERIALIZATION`` error. That error
            is a :py:class:`KafkaError`, so a caller can tell the two cases apart
            with ``msg.error().code()``.
        """

        messages = super(DeserializingShareConsumer, self).poll(timeout)
        for msg in messages:
            # broker/transport errors carry no payload to deserialize
            if msg.error() is not None:
                continue
            self._deserialize(msg)
        return messages

    def _deserialize(self, msg: Message) -> None:
        """
        Deserialize a single message's value and key.

        Both fields are deserialized into locals and written back to the
        message only once *both* succeed, so a deserialization failure leaves
        the record's raw key and value bytes untouched (and therefore still
        acknowledgeable). On a deserialization failure the record is marked via
        :py:func:`Message.set_error` rather than raising, so the rest of the
        batch (already fetched from the broker) is not lost. The deserializer
        calls are guarded, so a failure marks only this record instead of
        aborting the batch.

        A message with no topic is a broken invariant rather than a per-record
        data error, so it raises :py:exc:`TypeError` (matching
        :py:class:`DeserializingConsumer`).
        """

        topic = msg.topic()
        if topic is None:
            raise TypeError("Message topic is None")

        ctx = SerializationContext(topic, MessageField.VALUE, msg.headers())
        try:
            value = msg.value()
            if self._value_deserializer is not None:
                value = self._value_deserializer(value, ctx)
        except Exception as se:
            msg.set_error(KafkaError(KafkaError._VALUE_DESERIALIZATION, str(se)))
            return

        try:
            key = msg.key()
            if self._key_deserializer is not None:
                ctx.field = MessageField.KEY
                key = self._key_deserializer(key, ctx)
        except Exception as se:
            msg.set_error(KafkaError(KafkaError._KEY_DESERIALIZATION, str(se)))
            return

        msg.set_key(key)
        msg.set_value(value)
