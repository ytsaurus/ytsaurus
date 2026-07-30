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
from typing import Any, Callable, Dict, Optional

try:
    from typing import Self
except ImportError:
    # FIXME: drop fallback once we require Python >= 3.11
    from typing_extensions import Self

import confluent_kafka

from .. import _common as _common
from ._buffer_timeout_manager import BufferTimeoutManager
from ._kafka_batch_executor import ProducerBatchExecutor
from ._producer_batch_processor import ProducerBatchManager

logger = logging.getLogger(__name__)


class AIOProducer:

    # ========================================================================
    # INITIALIZATION AND LIFECYCLE MANAGEMENT
    # ========================================================================

    def __init__(
        self,
        producer_conf: Dict[str, Any],
        max_workers: int = 4,
        executor: Optional[concurrent.futures.Executor] = None,
        batch_size: int = 1000,
        buffer_timeout: float = 1.0,
    ) -> None:
        if executor is not None:
            self.executor = executor
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        # Store the event loop for async operations
        self._loop = asyncio.get_running_loop()

        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_common_callbacks(self._loop, producer_conf)

        self._producer: confluent_kafka.Producer = confluent_kafka.Producer(producer_conf)

        # Batching configuration
        self._batch_size: int = batch_size

        # Producer state management
        self._is_closed: bool = False  # Track if producer is closed

        # Initialize Kafka batch executor for handling Kafka operations
        self._kafka_executor = ProducerBatchExecutor(self._producer, self.executor)

        # Initialize batch processor for message batching and processing
        self._batch_processor = ProducerBatchManager(self._kafka_executor)

        # Initialize buffer timeout manager for timeout handling
        self._buffer_timeout_manager = BufferTimeoutManager(self._batch_processor, self._kafka_executor, buffer_timeout)
        if buffer_timeout > 0:
            self._buffer_timeout_manager.start_timeout_monitoring()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the producer and cleanup resources

        This method performs a graceful shutdown sequence to ensure all resources
        are properly cleaned up and no messages are lost:

        1. **Signal Shutdown**: Sets the closed flag to signal the timeout task to stop
        2. **Cancel Timeout Task**: Immediately cancels the buffer timeout monitoring task
        3. **Flush All Messages**: Flushes any buffered messages and waits for delivery confirmation
        4. **Shutdown ThreadPool**: Waits for all pending ThreadPool operations to complete
        5. **Cleanup**: Ensures the underlying librdkafka producer is properly closed. The shutdown
            is designed to be safe and non-blocking for the asyncio event loop
            while ensuring all pending operations complete before the producer is closed.

        Raises:
            Exception: May raise exceptions from buffer flushing, but these are logged
                      and don't prevent the cleanup process from completing.
        """
        # Set closed flag to signal timeout task to stop
        self._is_closed = True

        # Stop the buffer timeout monitoring task
        self._buffer_timeout_manager.stop_timeout_monitoring()

        # Flush any remaining messages
        try:
            await self.flush()
        except Exception:
            logger.error("Error flushing messages during close", exc_info=True)
            raise

        # Shutdown the ThreadPool executor and wait for any remaining tasks to complete
        # This ensures that all pending poll(), flush(), and other blocking operations
        # finish before the producer is considered fully closed
        if hasattr(self, 'executor'):
            # executor.shutdown(wait=True) is a blocking call that:
            # - Prevents new tasks from being submitted to the ThreadPool
            # - Waits for all currently executing and queued tasks to complete
            # - Returns only when all worker threads have finished
            #
            # We run this in a separate thread (using None as executor) to avoid
            # blocking the asyncio event loop during the potentially long shutdown wait
            await asyncio.get_running_loop().run_in_executor(None, self.executor.shutdown, True)

    def __del__(self) -> None:
        """Cleanup method called during garbage collection

        This ensures that the timeout task is properly cancelled even if
        close() wasn't explicitly called.
        """
        if hasattr(self, '_is_closed'):
            self._is_closed = True
        if hasattr(self, '_buffer_timeout_manager'):
            self._buffer_timeout_manager.stop_timeout_monitoring()

    def __len__(self) -> int:
        """Return the total number of pending messages.

        This includes:
        - Messages in librdkafka's output queue (waiting to be delivered to broker)
        - Messages in the async batch buffer (waiting to be sent to librdkafka)

        Returns:
            int: Total number of pending messages across both queues
        """
        if self._is_closed:
            return 0

        # Count messages in librdkafka queue
        librdkafka_count = len(self._producer)

        # Count messages in async batch buffer
        buffer_count = self._batch_processor.get_buffer_size()

        return librdkafka_count + buffer_count

    # ========================================================================
    # CORE PRODUCER OPERATIONS - Main public API
    # ========================================================================

    async def poll(self, timeout: float = 0, *args: Any, **kwargs: Any) -> int:
        """Processes delivery callbacks from librdkafka - blocking depends on timeout

        This method triggers any pending delivery reports that have been
        queued by librdkafka when messages are delivered or fail to deliver.

        Args:
            timeout: Timeout in seconds for waiting for callbacks:
                    - 0 = non-blocking, return after processing available callbacks
                    - >0 = block up to timeout seconds waiting for new callbacks
                    - -1 = block indefinitely until callbacks are available

        Returns:
            Number of callbacks processed during this call
        """
        return await self._call(self._producer.poll, timeout, *args, **kwargs)

    async def produce(
        self, topic: str, value: Optional[Any] = None, key: Optional[Any] = None, *args: Any, **kwargs: Any
    ) -> asyncio.Future[Any]:
        """Batched produce: Accumulates messages in buffer and flushes when threshold reached

        Args:
            topic: Kafka topic name (required)
            value: Message payload (optional)
            key: Message key (optional)
            *args, **kwargs: Additional parameters like partition, timestamp, headers

        Returns:
            asyncio.Future: Future that resolves to the delivered message or raises exception on failure
        """
        result = asyncio.get_running_loop().create_future()

        msg_data = {'topic': topic, 'value': value, 'key': key}

        # Add optional parameters to message data
        if 'partition' in kwargs:
            msg_data['partition'] = kwargs['partition']
        if 'timestamp' in kwargs:
            msg_data['timestamp'] = kwargs['timestamp']
        if 'headers' in kwargs:
            # Headers are not supported in batch mode due to librdkafka API limitations.
            # Use individual synchronous produce() calls if headers are required.
            raise NotImplementedError(
                "Headers are not supported in AIOProducer batch mode. "
                "Use the synchronous Producer.produce() method if headers are required."
            )

        self._batch_processor.add_message(msg_data, result)

        self._buffer_timeout_manager.mark_activity()

        # Check if we should flush the buffer
        if self._batch_processor.get_buffer_size() >= self._batch_size:
            await self._flush_buffer()

        return result

    async def flush(self, *args: Any, **kwargs: Any) -> Any:
        """Waits until all messages are delivered or timeout

        This method performs a complete flush:
        1. Flushes any buffered messages from local buffer to librdkafka
        2. Waits for librdkafka to deliver/acknowledge all messages
        """
        # First, flush any remaining messages in the buffer for all topics
        if not self._batch_processor.is_buffer_empty():
            await self._flush_buffer()
            # Update buffer activity since we just flushed
            self._buffer_timeout_manager.mark_activity()

        # Then flush the underlying producer and wait for delivery confirmation
        return await self._call(self._producer.flush, *args, **kwargs)

    async def purge(self, *args: Any, **kwargs: Any) -> Any:
        """Purges messages from internal queues - may block during cleanup"""
        # Cancel all pending futures
        self._batch_processor.cancel_pending_futures()

        # Clear local message buffer and futures
        self._batch_processor.clear_buffer()

        # Update buffer activity since we cleared the buffer
        self._buffer_timeout_manager.mark_activity()

        return await self._call(self._producer.purge, *args, **kwargs)

    async def list_topics(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._producer.list_topics, *args, **kwargs)

    # ========================================================================
    # TRANSACTION OPERATIONS - Kafka transaction support
    # ========================================================================

    async def init_transactions(self, *args: Any, **kwargs: Any) -> Any:
        """Network call to initialize transactions"""
        return await self._call(self._producer.init_transactions, *args, **kwargs)

    async def begin_transaction(self, *args: Any, **kwargs: Any) -> Any:
        """Network call to begin transaction"""

        # Flush messages to set a clean state before entering a transaction
        await self.flush()

        return await self._call(self._producer.begin_transaction, *args, **kwargs)

    async def send_offsets_to_transaction(self, *args: Any, **kwargs: Any) -> Any:
        """Network call to send offsets to transaction"""
        return await self._call(self._producer.send_offsets_to_transaction, *args, **kwargs)

    async def commit_transaction(self, *args: Any, **kwargs: Any) -> Any:
        """Commit transaction after flushing all buffered messages"""

        # Flush to ensure messages in the local batch_processor buffer are
        # delivered to librdkafka
        await self.flush()

        # Then commit transaction
        return await self._call(self._producer.commit_transaction, *args, **kwargs)

    async def abort_transaction(self, *args: Any, **kwargs: Any) -> Any:
        """Network call to abort transaction

        Messages produced before the call (i.e. inside the transaction boundary) will be aborted.
        Messages that are still in flight may be failed by librdkafka as they are considered
        outside the transaction boundary.
        Refer to librdkafka documentation section "Transactional producer API"
        for more details:
        https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#transactional-producer
        """

        # Flush to ensure messages in the local batch_processor buffer are
        # delivered to librdkafka
        await self.flush()

        return await self._call(self._producer.abort_transaction, *args, **kwargs)

    # ========================================================================
    # AUTHENTICATION AND SECURITY
    # ========================================================================

    async def set_sasl_credentials(self, *args: Any, **kwargs: Any) -> Any:
        """Authentication operation that may involve network calls"""
        return await self._call(self._producer.set_sasl_credentials, *args, **kwargs)

    # ========================================================================
    # BATCH PROCESSING OPERATIONS - Delegated to BatchProcessor
    # ========================================================================

    async def _flush_buffer(self, target_topic: Optional[str] = None) -> None:
        """Flush the current message buffer using clean batch processing flow

        This method demonstrates the new architecture where AIOProducer simply
        orchestrates the workflow between components:
        1. BatchProcessor creates immutable MessageBatch objects
        2. ProducerBatchExecutor executes each batch
        3. BufferTimeoutManager handles activity tracking
        """
        await self._batch_processor.flush_buffer(target_topic)

    # ========================================================================
    # UTILITY METHODS - Helper functions and internal utilities
    # ========================================================================

    async def _call(self, blocking_task: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Helper method for blocking operations that need ThreadPool execution"""
        return await _common.async_call(self.executor, blocking_task, *args, **kwargs)
