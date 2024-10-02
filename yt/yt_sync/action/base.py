from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as dataclass_field
import logging
from typing import Any
from typing import ClassVar
from typing import Generator

import yt.wrapper as yt
from yt.yt_sync.core import Settings
from yt.yt_sync.core.client import YtClientProxy

LOG = logging.getLogger("yt_sync")

# See https://a.yandex-team.ru/arcadia/yt/yt/client/tablet_client/public.h?rev=r14591157#L53 for details
_RETRYABLE_ERRORS: list[int] = [1701, 1702, 1707, 1725]


class ActionBase(ABC):
    """
    Some action with YT client over DB.

    Actions are:
    - stateful: action instance should contain it's internal logic if needed
    - multistepped: several yt client calls may be needed to complete action
    """

    def __init__(self):
        self.dry_run: bool = False

    @abstractmethod
    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        """
        Invokes some method of YT batch client and stores result.

        Returns True if next YT client call needed to complete action.
        """
        raise NotImplementedError

    @abstractmethod
    def process(self):
        """
        Process result of YT batch client call (invoked in schedule_next).
        Invoked after every schedule_next call.
        """
        raise NotImplementedError

    @property
    def is_awaiting(self) -> bool:
        return False

    @property
    def recommended_iteration_delay(self) -> float:
        """
        Recommended iteration delay for awaiting actions.
        Applies to all actions in ActionBatch.
        """
        return 0.0

    @property
    def recommended_max_iterations(self) -> int:
        """
        Recommended max count of iterations for awaiting actions.
        Applies to all action in ActionBatch.
        """
        return 0

    @staticmethod
    def assert_response(response: Any | None):
        assert response is not None, "Response not set"
        if response.is_ok():
            return
        raise yt.YtResponseError(response.get_error())

    @classmethod
    def is_retryable_error(cls, response: Any | None) -> bool:
        if response is None:
            return False
        if response.is_ok():
            return False
        error = yt.YtResponseError(response.get_error())

        for error_code in _RETRYABLE_ERRORS:
            if error.contains_code(error_code):
                LOG.debug("%s: got retryable error %s", cls.__name__, error)
                return True
        return False


class LongOperationActionBase(ActionBase):
    def __init__(self, settings: Settings):
        super().__init__()
        self.settings: Settings = settings

    @property
    def is_awaiting(self) -> bool:
        return True

    @property
    def recommended_iteration_delay(self) -> float:
        return 2.0

    @property
    def recommended_max_iterations(self) -> int:
        return int(self.settings.max_operation_action_duration / self.recommended_iteration_delay)


class GeneratorActionBase(ActionBase):
    def __init__(self):
        super().__init__()
        self._result: Any | None = None
        self._generator = self.act()
        self._stop_iteration = False
        self._batch_client: YtClientProxy | None = None

    def act(self) -> Generator:
        yield

    def _generator_iteration(self):
        try:
            self._result = self._generator.send(self._result)
        except StopIteration:
            self._stop_iteration = True

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        self._batch_client = batch_client
        self._generator_iteration()
        return not self._stop_iteration

    def process(self):
        pass


class AwaitingResultAction(ActionBase):
    _STATE_INITIAL: ClassVar[int] = 0
    _STATE_MODIFY: ClassVar[int] = 1
    _STATE_AWAIT: ClassVar[int] = 2
    _STATE_DONE: ClassVar[int] = 3

    def __init__(self):
        super().__init__()
        self._state: int = self._STATE_INITIAL
        self._modify_response: Any | None = None
        self._await_response: Any | None = None

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert self._state != self._STATE_MODIFY, "Bad state, maybe process() was not invoked after previous call"
        if self._STATE_INITIAL == self._state:
            self._modify_response = self.do_modify_request(batch_client)
            self._state = self._STATE_MODIFY
            return self._modify_response is not None
        if self._STATE_AWAIT == self._state:
            if self.dry_run:
                return False
            self._await_response = self.do_await_request(batch_client)
            return True
        return False

    def process(self):
        assert self._STATE_INITIAL != self._state, "Bad state"
        if self._STATE_MODIFY == self._state:
            if self._modify_response is not None:
                self.assert_response(self._modify_response)
                self._state = self._STATE_AWAIT
                return
            else:
                self._finish()
                return
        if self._STATE_AWAIT == self._state:
            if self.dry_run:
                self._finish()
                return
            self.assert_response(self._await_response)
            if self.is_required_state(self._await_response.get_result()):  # type:ignore
                self._finish()
            return

    def _finish(self):
        self._state = self._STATE_DONE
        self.postprocess()

    @property
    def is_awaiting(self) -> bool:
        return True

    @abstractmethod
    def do_modify_request(self, batch_client: YtClientProxy) -> Any | None:
        raise NotImplementedError

    @abstractmethod
    def do_await_request(self, batch_client: YtClientProxy) -> Any:
        raise NotImplementedError

    @abstractmethod
    def is_required_state(self, result: Any) -> bool:
        raise NotImplementedError

    def postprocess(self):
        pass


@dataclass
class ActionBatch:
    """
    Set of actions to be executed simultaneously with YT Batch client.
    All actions in batch are over single cluster.
    """

    cluster_name: str
    actions: list[ActionBase] = dataclass_field(default_factory=list)
    iteration_delay: float | None = None
    max_iterations: int | None = None

    @staticmethod
    def _get_max_value(lhs: int | float | None, rhs: int | float | None) -> int | float | None:
        if lhs is not None and rhs is not None:
            return max(lhs, rhs)
        if lhs is None:
            return rhs
        return lhs

    def update_from(self, other: ActionBatch):
        assert self.cluster_name == other.cluster_name
        self.actions.extend(other.actions)
        self.iteration_delay = self._get_max_value(self.iteration_delay, other.iteration_delay)
        self.max_iterations = self._get_max_value(self.max_iterations, other.max_iterations)


class TableActionCollector:
    """Collects sequential actions over each table in table set and converts them to parallel ActionBatch'es."""

    def __init__(self, cluster_name: str, iteration_delay: float = 0.5, max_iterations: int = 1000):
        self.cluster_name: str = cluster_name
        self._iteration_delay: float = iteration_delay
        self._max_iterations: int = max_iterations
        self._actions: dict[str, list[ActionBase]] = defaultdict(list)

    def add(self, table_key: str, action: ActionBase):
        self._actions[table_key].append(action)

    @staticmethod
    def make_slices(table_keys: list[str], limit: int | None) -> list[list[str]]:
        slices: list[list[str]] = list()
        current_slice: list[str] = list()
        for table_key in table_keys:
            current_slice.append(table_key)
            if limit and len(current_slice) == limit:
                slices.append(current_slice)
                current_slice = list()
        if current_slice:
            slices.append(current_slice)
        return slices

    def dump(self, limit: int | None = None) -> list[ActionBatch]:
        """Converts already collected actions to sequence of ActionBatch'es and clears inner state."""
        result: list[ActionBatch] = list()
        slices: list[list[str]] = self.make_slices(sorted(self._actions), limit)
        for slice in slices:
            current_slice = slice
            next_slice = list()
            while current_slice:
                script = ActionBatch(cluster_name=self.cluster_name)
                for table_key in current_slice:
                    action_list = self._actions[table_key]
                    if action_list:
                        script.actions.append(action_list.pop(0))
                    if action_list:
                        next_slice.append(table_key)
                current_slice = next_slice
                next_slice = list()
                if True in set([a.is_awaiting for a in script.actions]):
                    script.iteration_delay = max(
                        self._iteration_delay, max([action.recommended_iteration_delay for action in script.actions])
                    )
                    script.max_iterations = max(
                        self._max_iterations, max([action.recommended_max_iterations for action in script.actions])
                    )
                if script.actions:
                    result.append(script)
        self._actions.clear()
        return result

    def pop_table(self, table_key: str) -> list[ActionBatch]:
        if table_key not in self._actions:
            return []
        action_list = self._actions[table_key]
        if action_list:
            action = action_list.pop(0)
            script = ActionBatch(cluster_name=self.cluster_name)
            script.actions.append(action)
            script.iteration_delay = max(self._iteration_delay, action.recommended_iteration_delay)
            script.max_iterations = max(self._max_iterations, action.recommended_max_iterations)
            if not action_list:
                self._actions.pop(table_key, None)
            return [script]
        else:
            self._actions.pop(table_key, None)
        return []


class DbTableActionCollector:
    def __init__(self, iteration_delay: float = 0.5, max_iterations: int = 1000):
        self._iteration_delay: float = iteration_delay
        self._max_iterations: int = max_iterations
        self._last_used_collector: dict[str, int] = defaultdict(lambda: -1)
        self._collectors: list[TableActionCollector] = list()

    def add(self, cluster_name: str, table_key: str, action: ActionBase):
        self._get_collector(cluster_name, table_key).add(table_key, action)

    def dump(self, parallel: bool, limit: int | None = None) -> list[ActionBatch]:
        """Converts already collected actions to sequence of ActionBatch'es and clears inner state."""
        result: list[ActionBatch] = list()
        if parallel and not limit:
            for collector in self._collectors:
                result.extend(collector.dump())
        else:
            limit = limit if parallel else 1
            slices: list[list[str]] = TableActionCollector.make_slices(sorted(self._last_used_collector), limit)
            for slice in slices:
                for collector in self._collectors:
                    slice_actions: dict[str, list[ActionBatch]] = defaultdict(lambda: list())
                    for table_key in slice:
                        batches = collector.pop_table(table_key)
                        while batches:
                            slice_actions[table_key].extend(batches)
                            batches = collector.pop_table(table_key)
                    slice_actions_result: list[ActionBatch] = list()
                    while slice_actions:
                        script = ActionBatch(cluster_name=collector.cluster_name)
                        for table_key in slice:
                            if table_key not in slice_actions:
                                continue
                            actions = slice_actions[table_key]
                            if not actions:
                                slice_actions.pop(table_key, None)
                                continue
                            script.update_from(actions.pop(0))
                        if script.actions:
                            slice_actions_result.append(script)
                    result.extend(slice_actions_result)

        self._last_used_collector.clear()
        self._collectors.clear()
        return result

    def _get_collector(self, cluster_name: str, table_key: str) -> TableActionCollector:
        last_used: int = self._last_used_collector[table_key]
        if last_used < len(self._collectors):
            for i in range(last_used + 1, len(self._collectors)):
                collector = self._collectors[i]
                if collector.cluster_name == cluster_name:
                    self._last_used_collector[table_key] = i
                    return collector

        self._last_used_collector[table_key] = len(self._collectors)
        self._collectors.append(TableActionCollector(cluster_name, self._iteration_delay, self._max_iterations))
        return self._collectors[-1]
