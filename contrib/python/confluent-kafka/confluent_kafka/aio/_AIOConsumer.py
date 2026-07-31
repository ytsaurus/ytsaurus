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
from typing import Any, Callable, Dict, Optional, Tuple

try:
    from typing import Self
except ImportError:
    # FIXME: drop fallback once we require Python >= 3.11
    from typing_extensions import Self

import confluent_kafka

from . import _common as _common


class AIOConsumer:
    def __init__(
        self,
        consumer_conf: Dict[str, Any],
        max_workers: int = 2,
        executor: Optional[concurrent.futures.Executor] = None,
    ) -> None:
        if executor is not None:
            # Executor must have at least one worker.
            # At least two workers are needed when calling re-entrant
            # methods from callbacks.
            self.executor = executor
        else:
            if max_workers < 1:
                raise ValueError("max_workers must be at least 1")
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

        loop = asyncio.get_event_loop()
        wrap_common_callbacks = _common.wrap_common_callbacks
        wrap_conf_callback = _common.wrap_conf_callback
        wrap_common_callbacks(loop, consumer_conf)
        wrap_conf_callback(loop, consumer_conf, 'on_commit')

        self._consumer: confluent_kafka.Consumer = confluent_kafka.Consumer(consumer_conf)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def _call(self, blocking_task: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return await _common.async_call(self.executor, blocking_task, *args, **kwargs)

    def _wrap_callback(
        self,
        loop: asyncio.AbstractEventLoop,
        callback: Callable[..., Any],
        edit_args: Optional[Callable[[Tuple[Any, ...]], Tuple[Any, ...]]] = None,
        edit_kwargs: Optional[Callable[[Any], Any]] = None,
    ) -> Callable[..., Any]:
        def ret(*args: Any, **kwargs: Any) -> Any:
            if edit_args:
                args = edit_args(args)
            if edit_kwargs:
                kwargs = edit_kwargs(kwargs)
            f = asyncio.run_coroutine_threadsafe(callback(*args, **kwargs), loop)
            return f.result()

        return ret

    async def poll(self, *args: Any, **kwargs: Any) -> Any:
        """
        Polls for a single message from the subscribed topics.

        Performance Note:
            For high-throughput applications, prefer consume() over poll():
            consume() can retrieve multiple messages per call and amortize the
            async overhead across the entire batch.

            On the other hand, poll() retrieves one message per call, which means
            the ThreadPoolExecutor overhead is applied to each individual message.
            This can result in lower throughput compared to the synchronous
            consumer.poll() due to the async coordination overhead not being
            amortized.

        """
        return await self._call(self._consumer.poll, *args, **kwargs)

    async def consume(self, *args: Any, **kwargs: Any) -> Any:
        """
        Consumes a batch of messages from the subscribed topics.

        Performance Note:
            This method is recommended for high-throughput applications.

            By retrieving multiple messages per ThreadPoolExecutor call, the async
            coordination overhead is shared across all messages in the batch,
            resulting in much better throughput compared to repeated poll() calls.
        """
        return await self._call(self._consumer.consume, *args, **kwargs)

    def _edit_rebalance_callbacks_args(self, args: Tuple[Any, ...]) -> Tuple[Any, ...]:
        args_list = list(args)
        args_list[0] = self
        return tuple(args_list)

    async def subscribe(self, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_event_loop()
        for callback in ['on_assign', 'on_revoke', 'on_lost']:
            if callback in kwargs:
                kwargs[callback] = self._wrap_callback(
                    loop, kwargs[callback], self._edit_rebalance_callbacks_args
                )  # noqa: E501
        return await self._call(self._consumer.subscribe, *args, **kwargs)

    async def unsubscribe(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.unsubscribe, *args, **kwargs)

    async def commit(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.commit, *args, **kwargs)

    async def close(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.close, *args, **kwargs)

    async def seek(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.seek, *args, **kwargs)

    async def pause(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.pause, *args, **kwargs)

    async def resume(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.resume, *args, **kwargs)

    async def store_offsets(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.store_offsets, *args, **kwargs)

    async def committed(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.committed, *args, **kwargs)

    async def assign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.assign, *args, **kwargs)

    async def unassign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.unassign, *args, **kwargs)

    async def incremental_assign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.incremental_assign, *args, **kwargs)

    async def incremental_unassign(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.incremental_unassign, *args, **kwargs)

    async def assignment(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.assignment, *args, **kwargs)

    async def position(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.position, *args, **kwargs)

    async def consumer_group_metadata(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.consumer_group_metadata, *args, **kwargs)

    async def set_sasl_credentials(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.set_sasl_credentials, *args, **kwargs)

    async def list_topics(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.list_topics, *args, **kwargs)

    async def get_watermark_offsets(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.get_watermark_offsets, *args, **kwargs)

    async def offsets_for_times(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call(self._consumer.offsets_for_times, *args, **kwargs)
