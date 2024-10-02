from typing import Any
from typing import ClassVar

import pytest

import yt.wrapper as yt

from yt.yt_sync.action.base import ActionBase
from yt.yt_sync.action.base import ActionBatch
from yt.yt_sync.action.base import AwaitingResultAction
from yt.yt_sync.action.base import DbTableActionCollector
from yt.yt_sync.action.base import GeneratorActionBase
from yt.yt_sync.action.base import TableActionCollector
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_generic_error


class MyGeneratorAction(GeneratorActionBase):
    def __init__(self):
        super().__init__()
        self.count: int = 0

    def act(self):
        while self.count < 2:
            self.count += 1
            yield self.count


def test_generator_action():
    yt_client_factory = MockYtClientFactory(responses={"primary": {}})
    yt_client = yt_client_factory("primary")
    action = MyGeneratorAction()
    assert action.count == 0
    assert action.schedule_next(yt_client)
    assert action.count == 1
    assert action.schedule_next(yt_client)
    assert action.count == 2
    assert not action.schedule_next(yt_client)


class TestActionBase:
    def test_assert_response_none(self):
        with pytest.raises(AssertionError):
            ActionBase.assert_response(None)

    def test_assert_response_error(self):
        with pytest.raises(yt.YtResponseError):
            ActionBase.assert_response(MockResult(error=True))

    def test_assert_response_ok(self):
        ActionBase.assert_response(MockResult(result=True))


class TestAwaitingResultAction:
    class DummyAction(AwaitingResultAction):
        def __init__(self, modify_path: str | None, modify_value: str, check_path: str, required_state: str):
            super().__init__()
            self._modify_path: str | None = modify_path
            self._modify_value: str = modify_value
            self._check_path: str = check_path
            self._required_state: str = required_state
            self.postprocess_called: bool = False

        def do_modify_request(self, batch_client: YtClientProxy) -> Any | None:
            if self._modify_path:
                return batch_client.set(self._modify_path, self._modify_value)
            return None

        def do_await_request(self, batch_client: YtClientProxy) -> Any:
            return batch_client.get(self._check_path)

        def is_required_state(self, result: Any) -> bool:
            return self._required_state == result

        def postprocess(self):
            self.postprocess_called = True

    @pytest.fixture
    @staticmethod
    def cluster_name() -> str:
        return "primary"

    @pytest.fixture
    @staticmethod
    def modify_path(table_path: str) -> str:
        return f"{table_path}/@my_attr1"

    @pytest.fixture
    @staticmethod
    def check_path(table_path: str) -> str:
        return f"{table_path}/@my_attr2"

    @pytest.fixture
    @staticmethod
    def yt_client_factory(cluster_name: str, modify_path: str, check_path: str) -> MockYtClientFactory:
        return MockYtClientFactory(
            {cluster_name: {"set": {modify_path: MockResult(result=True)}, "get": {check_path: MockResult(result="2")}}}
        )

    @pytest.fixture
    @classmethod
    def dummy_action(cls, modify_path: str, check_path: str) -> DummyAction:
        return cls.DummyAction(modify_path, "1", check_path, "2")

    def test_call_process_before_schedule(self, dummy_action: DummyAction):
        with pytest.raises(AssertionError):
            dummy_action.process()

    def test_error_on_modify(self, cluster_name: str, modify_path: str, check_path: str, dummy_action: DummyAction):
        yt_client_factory = MockYtClientFactory(
            {
                cluster_name: {
                    "set": {modify_path: MockResult(error=yt_generic_error())},
                    "get": {check_path: MockResult(result="2")},
                }
            }
        )
        yt_client = yt_client_factory(cluster_name)
        assert dummy_action.schedule_next(yt_client) is True

        with pytest.raises(yt.YtResponseError):
            dummy_action.process()
        assert not dummy_action.postprocess_called

    def test_error_on_check(self, cluster_name: str, modify_path: str, check_path: str, dummy_action: DummyAction):
        yt_client_factory = MockYtClientFactory(
            {
                cluster_name: {
                    "set": {modify_path: MockResult(result=True)},
                    "get": {check_path: MockResult(error=yt_generic_error())},
                }
            }
        )
        yt_client = yt_client_factory(cluster_name)
        assert dummy_action.schedule_next(yt_client) is True
        dummy_action.process()

        assert dummy_action.schedule_next(yt_client) is True
        with pytest.raises(yt.YtResponseError):
            dummy_action.process()
        assert not dummy_action.postprocess_called

    def test_awaiting_action(
        self,
        cluster_name: str,
        modify_path: str,
        check_path: str,
        dummy_action: DummyAction,
        yt_client_factory: MockYtClientFactory,
    ):
        yt_client = yt_client_factory(cluster_name)

        # schedule modify action
        assert dummy_action.schedule_next(yt_client) is True
        dummy_action.process()

        call_tracker = yt_client_factory.get_call_tracker(cluster_name)
        assert call_tracker
        set_calls = call_tracker.calls["set"]
        assert 1 == len(set_calls)
        assert modify_path == set_calls[0].path_or_type
        assert set_calls[0].args is not None
        assert "1" == set_calls[0].args[0]

        # schedule check action
        assert dummy_action.schedule_next(yt_client) is True
        dummy_action.process()

        get_calls = call_tracker.calls["get"]
        assert 1 == len(get_calls)
        assert check_path == get_calls[0].path_or_type

        assert dummy_action.schedule_next(yt_client) is False
        assert dummy_action.postprocess_called

    def test_awaiting_no_required_state(
        self, cluster_name: str, modify_path: str, check_path: str, yt_client_factory: MockYtClientFactory
    ):
        dummy_action = self.DummyAction(modify_path, "1", check_path, "1")
        yt_client = yt_client_factory(cluster_name)

        # schedule modify action
        assert dummy_action.schedule_next(yt_client) is True
        dummy_action.process()

        # await result
        call_count = 10
        for _ in range(call_count):
            assert dummy_action.schedule_next(yt_client) is True
            dummy_action.process()

        call_tracker = yt_client_factory.get_call_tracker(cluster_name)
        assert call_tracker
        get_calls = call_tracker.calls["get"]
        assert call_count == len(get_calls)
        for call in get_calls:
            assert check_path == call.path_or_type
        assert not dummy_action.postprocess_called

    def test_awaiting_action_dry(
        self,
        cluster_name: str,
        modify_path: str,
        dummy_action: DummyAction,
        yt_client_factory: MockYtClientFactory,
    ):
        dummy_action.dry_run = True
        yt_client = yt_client_factory(cluster_name)

        # schedule modify action
        assert dummy_action.schedule_next(yt_client) is True
        dummy_action.process()

        call_tracker = yt_client_factory.get_call_tracker(cluster_name)
        assert call_tracker
        set_calls = call_tracker.calls["set"]
        assert 1 == len(set_calls)
        assert modify_path == set_calls[0].path_or_type
        assert set_calls[0].args is not None
        assert "1" == set_calls[0].args[0]

        # assert no check action
        assert dummy_action.schedule_next(yt_client) is False
        dummy_action.process()

        assert "get" not in call_tracker.calls
        assert dummy_action.postprocess_called

    def test_no_modify_request(self, cluster_name: str, check_path: str, yt_client_factory: MockYtClientFactory):
        dummy_action = self.DummyAction(None, "1", check_path, "2")
        yt_client = yt_client_factory(cluster_name)

        assert dummy_action.schedule_next(yt_client) is False
        dummy_action.process()

        call_tracker = yt_client_factory.get_call_tracker(cluster_name)
        assert call_tracker
        assert "set" not in call_tracker.calls
        assert "get" not in call_tracker.calls

        assert dummy_action.postprocess_called


class _BaseDummyAction(ActionBase):
    def __init__(self):
        super().__init__()

    def schedule_next(self, _: YtClientProxy) -> bool:
        return False

    def process(self):
        pass


class TestActionBatch:
    class DummyAction(_BaseDummyAction):
        def __init__(self, num: int):
            super().__init__()
            self.num: int = num

        def __eq__(self, value: object) -> bool:
            assert isinstance(value, self.__class__)
            return self.num == value.num

    def test_update_from_error(self):
        with pytest.raises(AssertionError):
            ActionBatch(cluster_name="c1").update_from(ActionBatch(cluster_name="c2"))

    def test_update_from_empty(self):
        batch = ActionBatch(cluster_name="c")
        batch.update_from(ActionBatch(cluster_name="c"))
        assert not batch.actions
        assert not batch.iteration_delay
        assert not batch.max_iterations

    def test_update_from_1(self):
        batch = ActionBatch(cluster_name="c", actions=[self.DummyAction(0)], iteration_delay=None, max_iterations=None)
        batch.update_from(
            ActionBatch(cluster_name="c", actions=[self.DummyAction(1)], iteration_delay=1, max_iterations=2)
        )
        assert batch.actions == [self.DummyAction(0), self.DummyAction(1)]
        assert batch.iteration_delay == 1
        assert batch.max_iterations == 2

    def test_update_from_2(self):
        batch = ActionBatch(cluster_name="c", actions=[self.DummyAction(0)], iteration_delay=1, max_iterations=2)
        batch.update_from(
            ActionBatch(cluster_name="c", actions=[self.DummyAction(1)], iteration_delay=None, max_iterations=None)
        )
        assert batch.actions == [self.DummyAction(0), self.DummyAction(1)]
        assert batch.iteration_delay == 1
        assert batch.max_iterations == 2

    def test_update_from_3(self):
        batch = ActionBatch(cluster_name="c", actions=[self.DummyAction(0)], iteration_delay=1, max_iterations=10)
        batch.update_from(
            ActionBatch(cluster_name="c", actions=[self.DummyAction(1)], iteration_delay=2, max_iterations=2)
        )
        assert batch.actions == [self.DummyAction(0), self.DummyAction(1)]
        assert batch.iteration_delay == 2
        assert batch.max_iterations == 10


class TestTableActionCollector:
    PATH1: ClassVar[str] = "//tmp/table_1"
    PATH2: ClassVar[str] = "//tmp/table_2"
    PATH3: ClassVar[str] = "//tmp/table_3"

    class DummyAction(_BaseDummyAction):
        def __init__(
            self,
            path: str,
            is_awaiting: bool = False,
            recommended_iteration_delay: float = 0.0,
            recommended_max_iterations: int = 0,
        ):
            self.path: str = path
            self._is_awaiting: bool = is_awaiting
            self._iteration_delay: float = recommended_iteration_delay
            self._max_iterations: int = recommended_max_iterations

        @property
        def is_awaiting(self) -> bool:
            return self._is_awaiting

        @property
        def recommended_iteration_delay(self) -> float:
            return self._iteration_delay

        @property
        def recommended_max_iterations(self) -> int:
            return self._max_iterations

    @classmethod
    def _check_actions(cls, paths: list[str], actions: list[ActionBase]):
        assert len(paths) == len(actions)
        for path, action in zip(paths, actions):
            assert isinstance(action, cls.DummyAction)
            assert path == action.path

    def test_empty(self):
        collector = TableActionCollector("primary")
        assert 0 == len(collector.dump())

    def _make_collector(self) -> TableActionCollector:
        collector = TableActionCollector("primary")
        collector.add(self.PATH1, self.DummyAction(self.PATH1))
        collector.add(self.PATH2, self.DummyAction(self.PATH2))
        collector.add(self.PATH2, self.DummyAction(self.PATH2))
        collector.add(self.PATH3, self.DummyAction(self.PATH3))
        collector.add(self.PATH3, self.DummyAction(self.PATH3))
        collector.add(self.PATH3, self.DummyAction(self.PATH3))
        return collector

    def _check_limit(self, limit: int | None, expected_paths: list[list[str]]):
        actions = self._make_collector().dump(limit=limit)
        assert len(actions) == len(expected_paths)
        for action, paths in zip(actions, expected_paths):
            assert "primary" == action.cluster_name
            self._check_actions(paths, action.actions)

    def test_limit_1(self):
        self._check_limit(
            limit=1, expected_paths=[[self.PATH1], [self.PATH2], [self.PATH2], [self.PATH3], [self.PATH3], [self.PATH3]]
        )

    def test_limit_2(self):
        self._check_limit(
            limit=2, expected_paths=[[self.PATH1, self.PATH2], [self.PATH2], [self.PATH3], [self.PATH3], [self.PATH3]]
        )

    def test_limit_3(self):
        self._check_limit(
            limit=3, expected_paths=[[self.PATH1, self.PATH2, self.PATH3], [self.PATH2, self.PATH3], [self.PATH3]]
        )

    def test_unlimited(self):
        self._check_limit(
            limit=None, expected_paths=[[self.PATH1, self.PATH2, self.PATH3], [self.PATH2, self.PATH3], [self.PATH3]]
        )

    def test_with_awaiting(self):
        expected = [
            ([self.PATH1, self.PATH2], None, None),
            ([self.PATH2], 10, 1000),
            ([self.PATH2], 20, 2000),
        ]

        collector = TableActionCollector("primary", iteration_delay=10, max_iterations=1000)
        collector.add(self.PATH1, self.DummyAction(self.PATH1))
        collector.add(self.PATH2, self.DummyAction(self.PATH2))
        collector.add(self.PATH2, self.DummyAction(self.PATH2, is_awaiting=True))
        collector.add(
            self.PATH2,
            self.DummyAction(
                self.PATH2, is_awaiting=True, recommended_iteration_delay=20, recommended_max_iterations=2000
            ),
        )
        actions = collector.dump()

        assert len(expected) == len(actions)
        for action, (paths, iter_delay, iter_count) in zip(actions, expected):
            assert "primary" == action.cluster_name
            self._check_actions(paths, action.actions)
            assert action.iteration_delay == iter_delay
            assert action.max_iterations == iter_count

    def test_pop_table(self):
        collector = TableActionCollector("primary")

        assert not collector.pop_table(self.PATH1)

        collector.add(self.PATH1, self.DummyAction(self.PATH1))
        collector.add(self.PATH1, self.DummyAction(self.PATH1))
        collector.add(self.PATH2, self.DummyAction(self.PATH2))

        def _check_action_batches(action_batches: list[ActionBatch], expected_path: str):
            assert 1 == len(action_batches)
            assert "primary" == action_batches[0].cluster_name
            assert 1 == len(action_batches[0].actions)
            action = action_batches[0].actions[0]
            assert isinstance(action, self.DummyAction)
            assert expected_path == action.path

        for _ in range(2):
            action_batches = collector.pop_table(self.PATH1)
            _check_action_batches(action_batches, self.PATH1)
        assert not collector.pop_table(self.PATH1)

        action_batches = collector.dump()
        _check_action_batches(action_batches, self.PATH2)


class TestDbTableActionCollector:
    CLUSTER1: ClassVar[str] = "c1"
    CLUSTER2: ClassVar[str] = "c2"
    PATH1: ClassVar[str] = "//tmp/table_1"
    PATH2: ClassVar[str] = "//tmp/table_2"
    PATH3: ClassVar[str] = "//tmp/table_3"

    class DummyAction(_BaseDummyAction):
        def __init__(self, path: str, num: int):
            self.path: str = path
            self.num: int = num

    @classmethod
    def _check_actions(cls, cluster: str, paths_and_nums: list[tuple[str, int]], action_batch: ActionBatch):
        assert cluster == action_batch.cluster_name
        assert len(paths_and_nums) == len(action_batch.actions)
        for path_and_num, action in zip(paths_and_nums, action_batch.actions):
            assert isinstance(action, cls.DummyAction)
            path, num = path_and_num
            assert path == action.path
            assert num == action.num

    @classmethod
    def _check_batches(cls, action_batches: list[ActionBatch], expected: list[tuple[str, list[tuple[str, int]]]]):
        assert len(expected) == len(action_batches)
        for action_batch, (cluster, paths_and_nums) in zip(action_batches, expected):
            cls._check_actions(cluster, paths_and_nums, action_batch)

    @classmethod
    @pytest.fixture
    def collector(cls) -> DbTableActionCollector:
        collector = DbTableActionCollector()
        collector.add(cls.CLUSTER1, cls.PATH1, cls.DummyAction(cls.PATH1, 1))
        collector.add(cls.CLUSTER1, cls.PATH2, cls.DummyAction(cls.PATH2, 2))
        collector.add(cls.CLUSTER1, cls.PATH3, cls.DummyAction(cls.PATH3, 3))
        collector.add(cls.CLUSTER1, cls.PATH1, cls.DummyAction(cls.PATH1, 4))
        collector.add(cls.CLUSTER2, cls.PATH2, cls.DummyAction(cls.PATH2, 5))
        collector.add(cls.CLUSTER1, cls.PATH3, cls.DummyAction(cls.PATH3, 6))

        return collector

    def test_sequential(self, collector: DbTableActionCollector):
        action_batches = collector.dump(parallel=False)
        expected = [
            (self.CLUSTER1, [(self.PATH1, 1)]),
            (self.CLUSTER1, [(self.PATH1, 4)]),
            (self.CLUSTER1, [(self.PATH2, 2)]),
            (self.CLUSTER2, [(self.PATH2, 5)]),
            (self.CLUSTER1, [(self.PATH3, 3)]),
            (self.CLUSTER1, [(self.PATH3, 6)]),
        ]
        self._check_batches(action_batches, expected)

    def test_parallel_limit_1(self, collector: DbTableActionCollector):
        action_batches = collector.dump(parallel=True, limit=1)
        expected = [
            (self.CLUSTER1, [(self.PATH1, 1)]),
            (self.CLUSTER1, [(self.PATH1, 4)]),
            (self.CLUSTER1, [(self.PATH2, 2)]),
            (self.CLUSTER2, [(self.PATH2, 5)]),
            (self.CLUSTER1, [(self.PATH3, 3)]),
            (self.CLUSTER1, [(self.PATH3, 6)]),
        ]
        self._check_batches(action_batches, expected)

    def test_parallel_limit_2(self, collector: DbTableActionCollector):
        action_batches = collector.dump(parallel=True, limit=2)
        expected = [
            (self.CLUSTER1, [(self.PATH1, 1), (self.PATH2, 2)]),
            (self.CLUSTER1, [(self.PATH1, 4)]),
            (self.CLUSTER2, [(self.PATH2, 5)]),
            (self.CLUSTER1, [(self.PATH3, 3)]),
            (self.CLUSTER1, [(self.PATH3, 6)]),
        ]
        self._check_batches(action_batches, expected)

    def test_parallel_limit_3(self, collector: DbTableActionCollector):
        action_batches = collector.dump(parallel=True, limit=3)
        expected = [
            (self.CLUSTER1, [(self.PATH1, 1), (self.PATH2, 2), (self.PATH3, 3)]),
            (self.CLUSTER1, [(self.PATH1, 4), (self.PATH3, 6)]),
            (self.CLUSTER2, [(self.PATH2, 5)]),
        ]
        self._check_batches(action_batches, expected)

    def test_parallel_unlimited(self, collector: DbTableActionCollector):
        action_batches = collector.dump(parallel=True, limit=None)
        expected = [
            (self.CLUSTER1, [(self.PATH1, 1), (self.PATH2, 2), (self.PATH3, 3)]),
            (self.CLUSTER1, [(self.PATH1, 4), (self.PATH3, 6)]),
            (self.CLUSTER2, [(self.PATH2, 5)]),
        ]
        self._check_batches(action_batches, expected)
