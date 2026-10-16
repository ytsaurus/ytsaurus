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
import concurrent.futures
import logging
from typing import Any, Dict, List, Sequence

import confluent_kafka

from .. import _common

logger = logging.getLogger(__name__)


class ProducerBatchExecutor:
    """Executes Kafka batch operations via thread pool

    This class is responsible for:
    - Executing produce_batch operations against confluent_kafka.Producer
    - Handling partial batch failures from librdkafka
    - Managing thread pool execution to avoid blocking the event loop
    - Processing delivery callbacks for successful messages
    - Supporting partition-specific batch operations
    """

    def __init__(self, producer: confluent_kafka.Producer, executor: concurrent.futures.Executor) -> None:
        """Initialize the Kafka batch executor

        Args:
            producer: confluent_kafka.Producer instance for Kafka operations
            executor: ThreadPoolExecutor for running blocking operations
        """
        self._producer = producer
        self._executor = executor

    async def execute_batch(self, topic: str, batch_messages: Sequence[Dict[str, Any]], partition: int = -1) -> int:
        """Execute a batch operation via thread pool

        This method handles the complete batch execution workflow:
        1. Execute produce_batch in thread pool to avoid blocking event loop
        2. Handle partial failures that occur during produce_batch
        3. Poll for delivery reports of successful messages

        Args:
            topic: Target topic for the batch
            batch_messages: List of prepared messages with callbacks assigned
            partition: Target partition for the batch (-1 = RD_KAFKA_PARTITION_UA)

        Returns:
            Result from producer.poll() indicating number of delivery reports processed

        Raises:
            Exception: Any exception from the batch operation is propagated
        """

        def _produce_batch_and_poll() -> int:
            """Helper function to run in thread pool

            This function encapsulates all the blocking Kafka operations:
            - Call produce_batch with specific partition and individual message callbacks
            - Handle partial batch failures for messages that fail immediately
            - Poll for delivery reports to trigger callbacks for successful messages
            """
            # Call produce_batch with specific partition and individual callbacks
            # Convert tuple to list since produce_batch expects a list
            messages_list: List[Dict[str, Any]] = (
                list(batch_messages) if isinstance(batch_messages, tuple) else batch_messages  # type: ignore
            )

            # Use the provided partition for the entire batch
            # This enables proper partition control while working around librdkafka limitations
            self._producer.produce_batch(topic, messages_list, partition=partition)

            # Handle partial batch failures: Check for messages that failed
            # during produce_batch. These messages have their msgstates
            # destroyed in Producer.c and won't get callbacks from librdkafka,
            # so we need to manually invoke their callbacks
            self._handle_partial_failures(messages_list)

            # Immediately poll to process delivery callbacks for successful messages
            poll_result = self._producer.poll(0)

            return poll_result

        # Execute in thread pool to avoid blocking event loop
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, _produce_batch_and_poll)

    async def flush_librdkafka_queue(self, timeout=-1):
        """Flush the librdkafka queue and wait for all messages to be delivered
        This method awaits until all outstanding produce requests are completed
        or the timeout is reached, unless the timeout is set to 0 (non-blocking).
        Args:
            timeout: Maximum time to wait in seconds:
                    - -1 = wait indefinitely (default)
                    - 0 = non-blocking, return immediately
                    - >0 = wait up to timeout seconds
        Returns:
            Number of messages still in queue after flush attempt
        """
        return await _common.async_call(self._executor, self._producer.flush, timeout)

    def _handle_partial_failures(self, batch_messages: List[Dict[str, Any]]) -> None:
        """Handle messages that failed during produce_batch

        When produce_batch encounters messages that fail immediately (e.g.,
        message too large, invalid topic, etc.), librdkafka destroys their
        msgstates and won't call their callbacks. We detect these failures by
        checking for '_error' in the message dict (set by Producer.c) and
        manually invoke the simple future-resolving callbacks.

        Args:
            batch_messages: List of message dictionaries that were passed to produce_batch
        """
        for msg_dict in batch_messages:
            if '_error' in msg_dict:
                # This message failed during produce_batch - its callback
                # won't be called by librdkafka
                callback = msg_dict.get('callback')
                if callback:
                    # Extract the error from the message dict (set by Producer.c)
                    error = msg_dict['_error']
                    # Manually invoke the callback with the error
                    # Note: msg is None since the message failed before being queued
                    try:
                        callback(error, None)
                    except Exception:
                        logger.warning("Exception in callback during partial failure handling", exc_info=True)
                        raise
