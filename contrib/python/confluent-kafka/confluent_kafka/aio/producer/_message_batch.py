# Copyright 2025 Confluent Inc.
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

import asyncio
from typing import Any, Dict, NamedTuple, Optional, Sequence


# Create immutable MessageBatch value object using modern typing
class MessageBatch(NamedTuple):
    """Immutable batch of messages for Kafka production

    This represents a group of messages destined for the same topic and partition,
    along with their associated futures for delivery confirmation.
    """

    topic: str  # Target topic for this batch
    messages: Sequence[Dict[str, Any]]  # Prepared message dictionaries
    futures: Sequence[asyncio.Future[Any]]  # Futures to resolve on delivery
    partition: int = -1  # Target partition for this batch (-1 = RD_KAFKA_PARTITION_UA)

    @property
    def size(self) -> int:
        """Get the number of messages in this batch"""
        return len(self.messages)

    @property
    def info(self) -> str:
        """Get a string representation of batch info"""
        return f"MessageBatch(topic='{self.topic}', partition={self.partition}, size={len(self.messages)})"


def create_message_batch(
    topic: str,
    messages: Sequence[Dict[str, Any]],
    futures: Sequence[asyncio.Future[Any]],
    callbacks: Optional[Any] = None,
    partition: int = -1,
) -> MessageBatch:
    """Create an immutable MessageBatch from sequences

    This factory function converts mutable sequences into an immutable MessageBatch object.
    Uses tuples internally for immutability while accepting any sequence type as input.

    Args:
        topic: Target topic name
        messages: Sequence of prepared message dictionaries
        futures: Sequence of asyncio.Future objects
        callbacks: Deprecated parameter, ignored for backwards compatibility
        partition: Target partition for this batch (-1 = RD_KAFKA_PARTITION_UA)

    Returns:
        MessageBatch: Immutable batch object
    """
    return MessageBatch(
        topic=topic,
        messages=tuple(messages) if not isinstance(messages, tuple) else messages,
        futures=tuple(futures) if not isinstance(futures, tuple) else futures,
        partition=partition,
    )
