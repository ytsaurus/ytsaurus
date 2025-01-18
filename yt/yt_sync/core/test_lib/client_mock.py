from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any

from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy


@dataclass
class CallTracker:
    @dataclass
    class Call:
        path_or_type: str
        args: list[Any] | None = None
        kwargs: dict[str, Any] | None = None

    calls: dict[str, list[Call]] = dataclass_field(default_factory=dict)

    def register_call(self, method_name: str, path_or_type: str, *args, **kwargs):
        self.calls.setdefault(method_name, list()).append(
            self.Call(path_or_type=path_or_type, args=list(args), kwargs=kwargs)
        )


class MockYtClientProxy(YtClientProxy):
    def __init__(self, cluster: str, responses: dict[str, dict[str, MockResult]], call_tracker: CallTracker):
        self._cluster: str = cluster
        self._responses: dict[str, dict[str, MockResult]] = responses
        self._call_tracker = call_tracker
        self._dry_run = False

    @property
    def underlying_client_proxy(self) -> YtClientProxy | None:
        return self

    @property
    def cluster(self) -> str:
        return self._cluster

    def create_batch_client(self, *args, **kwargs) -> Any:
        return self

    def commit_batch(self, *args, **kwargs):
        pass

    def _call_impl(self, method: str, path: str, *args, **kwargs) -> Any:
        self._call_tracker.register_call(method, path, *args, **kwargs)
        mock_result = self._responses[method][path]
        if mock_result.raw:
            return mock_result.get_result()
        return mock_result

    def get(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("get", path, *args, **kwargs)

    def get_tablet_infos(self, path: str, tablets: list[int]) -> Any:
        return self._call_impl("get_tablet_infos", path, tablets=tablets)

    def exists(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("exists", path, *args, **kwargs)

    def set(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("set", path, *args, **kwargs)

    def remove(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("remove", path, *args, **kwargs)

    def create(self, type: str, path: str | None = None, *args, **kwargs) -> Any:
        return self._call_impl("create", path or type, *args, type=type, **kwargs)

    def mount_table(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("mount_table", path, *args, **kwargs)

    def move(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("move", path, *args, **kwargs)

    def unmount_table(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("unmount_table", path, *args, **kwargs)

    def remount_table(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("remount_table", path, *args, **kwargs)

    def freeze_table(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("freeze_table", path, *args, **kwargs)

    def unfreeze_table(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("unfreeze_table", path, *args, **kwargs)

    def reshard_table(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("reshard_table", path, *args, **kwargs)

    def alter_table_replica(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("alter_table_replica", path, *args, **kwargs)

    def alter_table(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("alter_table", path, *args, **kwargs)

    def get_in_sync_replicas(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("get_in_sync_replicas", path, *args, **kwargs)

    def generate_timestamp(self, *args, **kwargs) -> Any:
        return self._call_impl("generate_timestamp", "*", *args, **kwargs)

    def alter_replication_card(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("alter_replication_card", path, *args, **kwargs)

    def run_remote_copy(self, path: str, *args, **kwargs) -> Any:
        return self._call_impl("run_remote_copy", path, *args, **kwargs)


class MockYtClientFactory(YtClientFactory):
    def __init__(self, responses: dict[str, dict[str, dict[str, MockResult]]]):
        """responses = {"cluster": {"method": {"path": MockResult}}}"""
        self._responses: dict[str, dict[str, dict[str, MockResult]]] = responses
        self._call_trackers: dict[str, CallTracker] = dict()
        self.aliases: dict[str, str] = dict()

    def is_dry_run(self) -> bool:
        return False

    def add_alias(self, name: str, address: str):
        self.aliases[name] = address

    def __call__(self, cluster: str, *args, **kwargs) -> YtClientProxy:
        return MockYtClientProxy(
            cluster, self._responses[cluster], self._call_trackers.setdefault(cluster, CallTracker())
        )

    def get_call_tracker(self, cluster: str) -> CallTracker | None:
        return self._call_trackers.get(cluster, None)
