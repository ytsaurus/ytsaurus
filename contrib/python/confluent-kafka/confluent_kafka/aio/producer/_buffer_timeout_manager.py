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
import logging
import time
import weakref
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    # Import only for type checking to avoid circular dependency
    from ._kafka_batch_executor import ProducerBatchExecutor
    from ._producer_batch_processor import ProducerBatchManager

logger = logging.getLogger(__name__)


class BufferTimeoutManager:
    """Manages buffer timeout and activity tracking for message batching

    This class is responsible for:
    - Monitoring buffer inactivity and triggering automatic flushes
    - Tracking buffer activity timestamps
    - Managing background timeout monitoring tasks
    - Coordinating between batch processor and executor for timeout flushes
    """

    def __init__(
        self, batch_processor: "ProducerBatchManager", kafka_executor: "ProducerBatchExecutor", timeout: float
    ) -> None:
        """Initialize the buffer timeout manager

        Args:
            batch_processor: ProducerBatchManager instance for creating batches
            kafka_executor: ProducerBatchExecutor instance for executing batches
            timeout: Timeout in seconds for buffer inactivity (0 disables timeout)
        """
        self._batch_processor = batch_processor
        self._kafka_executor = kafka_executor
        self._timeout = timeout
        self._last_activity: float = time.time()
        self._timeout_task: Optional[asyncio.Task[None]] = None
        self._running: bool = False

    def start_timeout_monitoring(self) -> None:
        """Start the background task that monitors buffer inactivity

        Creates an async task that runs in the background and periodically checks
        if messages have been sitting in the buffer for too long without being
        flushed.

        Key design decisions:
        1. **Weak Reference**: Uses weakref.ref(self) to prevent circular refs
        2. **Self-Canceling**: The task stops itself if manager is GC'd
        3. **Adaptive Check Interval**: Uses timeout to determine check frequency
        """
        if not self._timeout or self._timeout <= 0:
            return  # Timeout disabled

        self._running = True
        self._timeout_task = asyncio.create_task(self._monitor_timeout())

    def stop_timeout_monitoring(self) -> None:
        """Stop and cleanup the buffer timeout monitoring task"""
        self._running = False
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
            self._timeout_task = None

    def mark_activity(self) -> None:
        """Update the timestamp of the last buffer activity

        This method should be called whenever:
        1. Messages are added to the buffer (in produce())
        2. Buffer is manually flushed
        3. Buffer is purged/cleared
        """
        self._last_activity = time.time()

    async def _monitor_timeout(self) -> None:
        """Monitor buffer timeout in background task

        This method runs continuously in the background, checking for buffer
        inactivity and triggering flushes when the timeout threshold is exceeded.
        """
        # Use weak reference to avoid circular reference and allow garbage collection
        manager_ref = weakref.ref(self)

        while True:
            # Check interval should be proportional to buffer timeout for efficiency
            manager = manager_ref()
            if manager is None or not manager._running:
                break

            # Calculate adaptive check interval: timeout/2 with bounds
            # Examples: 0.1s→0.1s, 1s→0.5s, 5s→1.0s, 30s→1.0s
            check_interval = max(0.1, min(1.0, manager._timeout / 2))
            await asyncio.sleep(check_interval)

            # Re-check manager after sleep
            manager = manager_ref()
            if manager is None or not manager._running:
                break

            # Check if buffer has been inactive for too long
            time_since_activity = time.time() - manager._last_activity
            if time_since_activity >= manager._timeout:

                try:
                    # Flush the buffer due to timeout
                    await manager._flush_buffer_due_to_timeout()
                    # Update activity since we just flushed
                    manager.mark_activity()
                except Exception:
                    logger.error("Error flushing buffer due to timeout", exc_info=True)
                    # Re-raise all exceptions - don't swallow any errors
                    raise

    async def _flush_buffer_due_to_timeout(self) -> None:
        """Flush buffer due to timeout by coordinating batch processor/executor

        This method handles the complete timeout flush workflow:
        1. Create batches from the batch processor
        2. Execute batches from the batch processor
        3. Flush librdkafka queue to ensure messages are delivered
        """
        # Create batches from current buffer and send to librdkafka queue
        await self._batch_processor.flush_buffer()

        # Flush librdkafka queue to ensure messages are delivered to broker
        # 0 timeout means non-blocking flush
        await self._kafka_executor.flush_librdkafka_queue(0)
