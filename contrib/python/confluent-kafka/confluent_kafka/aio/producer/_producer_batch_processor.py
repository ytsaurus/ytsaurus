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
import copy
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Tuple

from confluent_kafka import KafkaException as _KafkaException

from ._message_batch import MessageBatch, create_message_batch

if TYPE_CHECKING:
    # Import only for type checking to avoid circular dependency
    from ._kafka_batch_executor import ProducerBatchExecutor

logger = logging.getLogger(__name__)


class ProducerBatchManager:
    """Handles batching and processing of Kafka messages for AIOProducer

    This class encapsulates all the logic for:
    - Grouping messages by topic and partition
    - Managing message buffers and futures
    - Creating simple future-resolving callbacks
    - Executing batch operations via librdkafka
    """

    def __init__(self, kafka_executor: "ProducerBatchExecutor") -> None:
        """Initialize the batch processor

        Args:
            kafka_executor: KafkaBatchExecutor instance for Kafka operations
        """
        self._kafka_executor = kafka_executor
        self._message_buffer: List[Dict[str, Any]] = []
        self._buffer_futures: List[asyncio.Future[Any]] = []

    def add_message(self, msg_data: Dict[str, Any], future: asyncio.Future[Any]) -> None:
        """Add a message to the batch buffer

        Args:
            msg_data: Dictionary containing message data
            future: asyncio.Future to resolve when message is delivered
        """
        self._message_buffer.append(msg_data)
        self._buffer_futures.append(future)

    def get_buffer_size(self) -> int:
        """Get the current number of messages in the buffer"""
        return len(self._message_buffer)

    def is_buffer_empty(self) -> bool:
        """Check if the buffer is empty"""
        return len(self._message_buffer) == 0

    def clear_buffer(self) -> None:
        """Clear the entire buffer"""
        self._message_buffer.clear()
        self._buffer_futures.clear()

    def cancel_pending_futures(self) -> None:
        """Cancel all pending futures in the buffer"""
        for future in self._buffer_futures:
            if not future.done():
                future.cancel()

    def create_batches(self, target_topic: Optional[str] = None) -> List[MessageBatch]:
        """Create MessageBatch objects from the current buffer

        Args:
            target_topic: Optional topic to create batches for (None for all)

        Returns:
            List[MessageBatch]: List of immutable MessageBatch objects
        """
        if self.is_buffer_empty():
            return []

        # Group by topic and partition for optimal batching
        topic_partition_groups = self._group_messages_by_topic_and_partition()
        batches = []

        for (topic, partition), group_data in topic_partition_groups.items():
            if target_topic is None or topic == target_topic:
                # Prepare batch messages
                batch_messages = self._prepare_batch_messages(group_data["messages"])

                # Assign simple future-resolving callbacks to messages
                self._assign_future_callbacks(batch_messages, group_data["futures"])

                # Create immutable MessageBatch object with partition info
                batch = create_message_batch(
                    topic=topic,
                    messages=batch_messages,
                    futures=group_data["futures"],
                    callbacks=None,  # No user callbacks anymore
                    partition=partition,  # Add partition info to batch
                )
                batches.append(batch)

        return batches

    def _clear_topic_from_buffer(self, target_topic: str) -> None:
        """Remove messages for a specific topic from the buffer

        Args:
            target_topic: Topic to remove from buffer
        """
        messages_to_keep = []
        futures_to_keep = []

        for i, msg_data in enumerate(self._message_buffer):
            if msg_data["topic"] != target_topic:
                messages_to_keep.append(msg_data)
                futures_to_keep.append(self._buffer_futures[i])

        self._message_buffer = messages_to_keep
        self._buffer_futures = futures_to_keep

    async def flush_buffer(self, target_topic: Optional[str] = None) -> None:
        """Flush the current message buffer using produce_batch

        Args:
            target_topic: Optional topic to flush (None for all topics)

        Returns:
            None
        """
        if self.is_buffer_empty():
            return

        # Create batches for processing
        batches = self.create_batches(target_topic)

        # Clear the buffer immediately to prevent race conditions
        if target_topic is None:
            # Clear entire buffer since we're processing all messages
            self.clear_buffer()
        else:
            # Clear only messages for the target topic that we're processing
            self._clear_topic_from_buffer(target_topic)

        try:
            # Execute batches with cleanup
            await self._execute_batches(batches, target_topic)
        except Exception:
            # Add batches back to buffer on failure
            try:
                self._add_batches_back_to_buffer(batches)
            except Exception:
                logger.error(f"Error adding batches back to buffer on failure. messages might be lost: {batches}")
                raise
            raise

    async def _execute_batches(self, batches: List[MessageBatch], target_topic: Optional[str] = None) -> None:
        """Execute batches and handle cleanup after successful execution

        Args:
            batches: List of batches to execute
            target_topic: Optional topic for selective buffer clearing

        Returns:
            None

        Raises:
            Exception: If any batch execution fails
        """
        # Execute each batch
        for batch in batches:
            try:
                # Execute batch using the Kafka executor
                await self._kafka_executor.execute_batch(batch.topic, batch.messages, batch.partition)

            except Exception as e:
                # Handle batch failure by failing all unresolved futures for this batch
                self._handle_batch_failure(e, batch.futures)
                # Re-raise the exception so caller knows the batch operation failed
                raise

    def _add_batches_back_to_buffer(self, batches: List[MessageBatch]) -> None:
        """Add batches back to the buffer when execution fails

        Args:
            batches: List of MessageBatch objects to add back to buffer
        """
        for batch in batches:
            # Add each message and its future back to the buffer
            for i, message in enumerate(batch.messages):
                # Reconstruct the original message data from the batch
                msg_data = {
                    'topic': batch.topic,
                    'value': message.get('value'),
                    'key': message.get('key'),
                }

                # Add optional fields if present
                if 'partition' in message:
                    msg_data['partition'] = message['partition']
                if 'timestamp' in message:
                    msg_data['timestamp'] = message['timestamp']
                if 'headers' in message:
                    msg_data['headers'] = message['headers']

                # Add the message and its future back to the buffer
                self._message_buffer.append(msg_data)
                self._buffer_futures.append(batch.futures[i])

    def _group_messages_by_topic_and_partition(self) -> Dict[Tuple[str, int], Dict[str, List[Any]]]:
        """Group buffered messages by topic and partition for optimal batching

        This function efficiently organizes the mixed-topic message buffer into
        topic+partition-specific groups, enabling proper partition control while
        maintaining batch efficiency.

        Algorithm:
        - Single O(n) pass through message buffer
        - Groups related data (messages, futures) by (topic, partition) tuple
        - Maintains index relationships between buffer arrays
        - Uses partition from message data, defaults to RD_KAFKA_PARTITION_UA
          (-1) if not specified

        Returns:
            dict: Topic+partition groups with structure:
                {
                    ('topic_name', partition): {
                        'messages': [msg_data1, ...],  # Message dicts
                        'futures': [future1, ...],     # asyncio.Future objects
                    }
                }
        """
        topic_partition_groups: Dict[Tuple[str, int], Dict[str, Any]] = {}

        # Iterate through buffer once - O(n) complexity
        for i, msg_data in enumerate(self._message_buffer):
            topic = msg_data["topic"]
            # Get partition from message data, default to RD_KAFKA_PARTITION_UA (-1) if not specified
            partition = msg_data.get("partition", -1)  # -1 = RD_KAFKA_PARTITION_UA

            # Create composite key for grouping
            group_key = (topic, partition)

            # Create new topic+partition group if this is first message for this combination
            if group_key not in topic_partition_groups:
                topic_partition_groups[group_key] = {
                    "messages": [],  # Message data for produce_batch
                    "futures": [],  # Futures to resolve on delivery
                }

            # Add message and related data to appropriate topic+partition group
            # Note: All arrays stay synchronized by index
            topic_partition_groups[group_key]["messages"].append(msg_data)
            topic_partition_groups[group_key]["futures"].append(self._buffer_futures[i])

        return topic_partition_groups

    def _prepare_batch_messages(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare messages for produce_batch by removing internal fields

        Args:
            messages: List of message dictionaries

        Returns:
            List of cleaned message dictionaries ready for produce_batch
        """
        batch_messages = []
        for msg_data in messages:
            # Create a shallow copy and remove fields not needed by produce_batch
            batch_msg = copy.copy(msg_data)
            batch_msg.pop("topic", None)  # Remove topic since it's passed separately
            # Note: We keep 'partition' in individual messages for reference,
            # but the batch partition will be used by produce_batch
            batch_messages.append(batch_msg)

        return batch_messages

    def _assign_future_callbacks(
        self, batch_messages: List[Dict[str, Any]], futures: Sequence[asyncio.Future[Any]]
    ) -> None:
        """Assign simple future-resolving callbacks to each message in batch

        Args:
            batch_messages: List of message dictionaries for produce_batch
            futures: List of asyncio.Future objects to resolve
        """
        for i, batch_msg in enumerate(batch_messages):
            future = futures[i]

            def create_simple_callback(fut: asyncio.Future[Any]) -> Callable[[Any, Any], None]:
                """Create a simple callback that only resolves the future"""

                def simple_callback(err: Any, msg: Any) -> None:
                    if err:
                        if not fut.done():
                            fut.set_exception(_KafkaException(err))
                    else:
                        if not fut.done():
                            fut.set_result(msg)

                return simple_callback

            # Assign the simple callback to this message
            batch_msg["callback"] = create_simple_callback(future)

    def _handle_batch_failure(self, exception: Exception, batch_futures: Sequence[asyncio.Future[Any]]) -> None:
        """Handle batch operation failure by failing all unresolved futures

        When a batch operation fails before any individual callbacks are invoked,
        we need to fail all futures for this batch since none of the per-message
        callbacks will be called by librdkafka.

        Args:
            exception: The exception that caused the batch to fail
            batch_futures: List of futures for this batch
        """
        # Fail all futures since no individual callbacks will be invoked
        for future in batch_futures:
            # Only set exception if future isn't already done
            if not future.done():
                future.set_exception(exception)
