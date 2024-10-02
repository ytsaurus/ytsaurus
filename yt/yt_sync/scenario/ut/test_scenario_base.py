from typing import Any

import pytest

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types, YtDatabase
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.test_lib import MockResult, MockYtClientFactory

from yt.yt_sync.action import ActionBase, ActionBatch
from yt.yt_sync.scenario.base import ScenarioBase

PATH1: str = "//tmp/table_1"
PATH2: str = "//tmp/table_2"
PATH3: str = "//tmp/table_3"


class DummyAction(ActionBase):
    def __init__(
        self,
        path: str,
        count: int = 1,
    ):
        self.path: str = path
        assert count > 0
        self._count: int = count
        self.schedule_count: int = 0
        self.process_count: int = 0

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        self.schedule_count += 1
        if self._count > 0:
            batch_client.get(self.path)
        self._count -= 1
        return self._count > 0

    def process(self):
        self.process_count += 1


class DummyScenario(ScenarioBase):
    def __init__(
        self,
        yt_client_factory: MockYtClientFactory,
        actions: list[ActionBatch] = list(),
    ):
        super().__init__(YtDatabase(), YtDatabase(), Settings(db_type=Settings.REPLICATED_DB), yt_client_factory)
        self._actions: list[ActionBatch] = actions
        self.pre_called: bool = False
        self.post_called: bool = False

    def pre_action(self):
        self.pre_called = True

    def generate_actions(self) -> list[ActionBatch]:
        return self._actions

    def post_action(self):
        self.post_called = True


class TestScenarioBase:
    @pytest.fixture
    @staticmethod
    def table_attributes(default_schema: Types.Schema) -> Types.Attributes:
        return {"dynamic": True, "schema": default_schema}

    def test_run_scenario(self):
        actions: list[ActionBatch] = list()
        calls: dict[str, Any] = dict()
        for cluster in ("primary", "remote0", "remote1"):
            batch = ActionBatch(cluster_name=cluster, actions=[])
            batch.actions.append(DummyAction(PATH1, 1))
            cluster_calls = calls.setdefault(cluster, dict()).setdefault("get", dict())
            cluster_calls[PATH1] = MockResult(result=True)

            if cluster in ("primary", "remote0"):
                batch.actions.append(DummyAction(PATH2, 2))
                cluster_calls[PATH2] = MockResult(result=True)

            if cluster in ("primary", "remote1"):
                batch.actions.append(DummyAction(PATH3, 3))
                cluster_calls[PATH3] = MockResult(result=True)

            actions.append(batch)

        yt_client_factory = MockYtClientFactory(calls)
        scenario = DummyScenario(yt_client_factory, actions)
        scenario.run()
        assert scenario.pre_called
        assert scenario.post_called

        # assert actions methods invoke
        for batch in actions:
            path1_action = batch.actions[0]
            path2_action = batch.actions[1] if batch.cluster_name in ("primary", "remote0") else None
            path3_action = None
            if "primary" == batch.cluster_name:
                path3_action = batch.actions[2]
            elif "remote1" == batch.cluster_name:
                path3_action = batch.actions[1]

            assert isinstance(path1_action, DummyAction)
            assert 1 == path1_action.schedule_count
            assert 1 == path1_action.process_count

            if path2_action:
                assert isinstance(path2_action, DummyAction)
                assert 2 == path2_action.schedule_count
                assert 2 == path2_action.process_count

            if path3_action:
                assert isinstance(path3_action, DummyAction)
                assert 3 == path3_action.schedule_count
                assert 3 == path3_action.process_count

        # assert YT client calls
        for cluster in ("primary", "remote0", "remote1"):
            call_tracker = yt_client_factory.get_call_tracker(cluster)
            assert call_tracker
            get_calls = call_tracker.calls["get"]
            assert 1 == len([c for c in get_calls if c.path_or_type == PATH1])

            p2_calls_count = len([c for c in get_calls if c.path_or_type == PATH2])
            p3_calls_count = len([c for c in get_calls if c.path_or_type == PATH3])

            if cluster in ("primary", "remote0"):
                assert 2 == p2_calls_count
            else:
                assert 0 == p2_calls_count

            if cluster in ("primary", "remote1"):
                assert 3 == p3_calls_count
            else:
                assert 0 == p3_calls_count
