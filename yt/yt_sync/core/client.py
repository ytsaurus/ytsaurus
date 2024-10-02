from __future__ import annotations

from contextlib import AbstractContextManager
import logging
import os
from typing import Any
from typing import Callable
from typing import Iterable

from yt import wrapper as yt

LOG = logging.getLogger("yt_sync")


def _do_trace(log_func: Callable, prefix: str, func_name: str, args: Iterable, kwargs: dict):
    args = ", ".join(repr(x) for x in args)
    kwargs = ", ".join("%s=%r" % x for x in kwargs.items())
    log_func("%s:%s(%s)", prefix, func_name, ", ".join(k for k in (args, kwargs) if k))


class DryRun:
    def __init__(self, cluster: str):
        self.cluster = cluster

    def __getattr__(self, name: str) -> Callable:
        return lambda *args, **kwargs: self._print_command(self.cluster, name, *args, **kwargs)

    @staticmethod
    def _print_command(cluster: str, name: str, *args, **kwargs):
        _do_trace(LOG.debug, cluster, name, args, kwargs)


class DryRunTransaction(DryRun, AbstractContextManager):
    def __init__(self, cluster: str):
        super().__init__(cluster)

    def __exit__(self, *args, **kwargs):
        pass


class DryRunOperationState(DryRun):
    def __init__(self, cluster: str):
        super().__init__(cluster)
        self._count: int = 0

    def is_finished(self, *args, **kwargs) -> bool:
        self._print_command(self.cluster, "is_finished", *args, **kwargs)
        self._count += 1
        return self._count > 1  # to get rid of yield and check scheduling order


class DryRunOperation(DryRun):
    def __init__(self, cluster: str):
        super().__init__(cluster)
        self._state = None

    def get_state(self, *args, **kwargs) -> DryRunOperationState:
        self._print_command(self.cluster, "get_state", *args, **kwargs)
        if not self._state:
            self._state = DryRunOperationState(self.cluster)
        return self._state


class MockResult:
    def __init__(self, result: Any | None = None, error: Any | None = None, raw: bool = False):
        assert result is not None or error is not None
        if result is not None:
            assert error is None
        if error is not None:
            assert result is None
        self._result = result
        self._error = error
        self.raw = raw

    def is_ok(self):
        return self._error is None

    def get_result(self) -> Any:
        assert self._result is not None
        return self._result

    def get_error(self) -> Any:
        assert self._error is not None
        return self._error


class MockOperation:
    class MockState:
        def __init__(self, name: str):
            self.name = name

        def __repr__(self):
            return self.name

        def __str__(self):
            return self.name

        def is_finished(self):
            return self.name in ("aborted", "completed", "failed")

        def is_unsuccessfully_finished(self):
            return self.name in ("aborted", "failed")

    def __init__(self, state: str):
        self._state: self.MockState = self.MockState(state)

    def get_state(self) -> MockState:
        return self._state


class YtClientProxy(DryRun):
    """Just print modification command instead of execute in dry_run mode."""

    def __init__(
        self,
        dry_run: bool,
        cluster: str,
        client: yt.YtClient,
        batch: bool = False,
        underlying_client_proxy: YtClientProxy | None = None,
    ):
        super().__init__(cluster)
        self._dry_run = dry_run
        self._client = client
        self._batch: bool = batch
        self.underlying_client_proxy: YtClientProxy | None = underlying_client_proxy

    def __str__(self) -> str:
        return self.cluster

    @property
    def __dict__(self):
        return self.client.__dict__

    def __getattr__(self, name) -> Any:
        v = getattr(self._client, name)
        if not callable(v):
            return v
        if self._dry_run:
            if self._batch:
                return self._enable_trace(self.cluster, name, lambda *args, **kwargs: MockResult(result=True))
            else:
                return super().__getattr__(name)
        else:
            return self._enable_trace(self.cluster, name, v)

    def run_remote_copy(self, *args, **kwargs) -> Any:
        assert not self._batch, "run_remote_copy is not supported in batch client, use underlying_client_proxy"
        self._print_command(self.cluster, "run_remote_copy", *args, **kwargs)
        if self._dry_run:
            return MockOperation("completed")
        return self._client.run_remote_copy(*args, **kwargs)

    def exists(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "exists", *args, **kwargs)
        return self._client.exists(*args, **kwargs)

    def get(self, path: str, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "get", path, *args, **kwargs)
        try:
            return self._client.get(path, *args, **kwargs)
        except yt.YtHttpResponseError as err:
            if err.is_access_denied():
                raise

    def get_tablet_infos(self, path: str, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "get_tablet_infos", path, *args, **kwargs)
        try:
            return self._client.get_tablet_infos(path, *args, **kwargs)
        except yt.YtHttpResponseError as err:
            if err.is_access_denied():
                raise

    def lookup_rows(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "lookup_rows", *args, **kwargs)
        try:
            return self._client.lookup_rows(*args, **kwargs)
        except yt.YtHttpResponseError as err:
            if err.is_access_denied():
                raise
            return []

    def list_queue_consumer_registrations(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "list_queue_consumer_registrations", *args, **kwargs)
        try:
            return self._client.list_queue_consumer_registrations(*args, **kwargs)
        except yt.YtHttpResponseError:
            raise

    def select_rows(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "select_rows", *args, **kwargs)
        try:
            return self._client.select_rows(*args, **kwargs)
        except yt.YtHttpResponseError as err:
            if err.is_access_denied():
                raise
            return []

    def list(self, *args, **kwargs) -> Any | None:
        self._print_command(self.cluster, "list", *args, **kwargs)
        try:
            return self._client.list(*args, **kwargs)
        except yt.YtHttpResponseError as err:
            if err.is_access_denied():
                raise

    def create(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "create", *args, **kwargs)
        if self._dry_run:
            if self._batch:
                return MockResult(result="00000000-00000000-00000000-00000000")
            else:
                return "00000000-00000000-00000000-00000000"
        return self._client.create(*args, **kwargs)

    def run_map(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "run_map", *args, **kwargs)
        if self._dry_run:
            return DryRunOperation(self.cluster)
        return self._client.run_map(*args, **kwargs)

    def run_sort(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "run_sort", *args, **kwargs)
        if self._dry_run:
            return DryRunOperation(self.cluster)
        return self._client.run_sort(*args, **kwargs)

    def create_batch_client(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "create_batch_client", *args, **kwargs)
        return YtClientProxy(self._dry_run, self.cluster, self._client.create_batch_client(*args, **kwargs), True, self)

    def commit_batch(self, *args, **kwargs):
        self._print_command(self.cluster, "commit_batch", *args, **kwargs)
        self._client.commit_batch(*args, **kwargs)

    def Transaction(self, *args, **kwargs):
        self._print_command(self.cluster, "Transaction", *args, **kwargs)
        if self._dry_run:
            return DryRunTransaction(self.cluster)
        else:
            return self._client.Transaction(*args, **kwargs)

    @staticmethod
    def _enable_trace(cluster: str, name: str, func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            _do_trace(LOG.debug, cluster, name, args, kwargs)
            return func(*args, **kwargs)

        return wrapper


class YtClientFactory:
    def __init__(self, dry_run: bool, token: str | None, per_cluster_config: dict[str, Any] | None = None):
        self._dry_run: bool = dry_run
        self._token: str | None = token
        self._aliases: dict[str, str] = {}
        self._per_cluster_config: dict[str, Any] = per_cluster_config or dict()

    def is_dry_run(self) -> bool:
        return self._dry_run

    def add_alias(self, name: str, address: str):
        self._aliases[name] = address

    def get_proxy_address(self, cluster: str) -> str | None:
        name, address = self.split_name(cluster)
        address = address or self._aliases.get(name, name)
        return address

    def __call__(self, cluster: str, config: dict | None = None) -> YtClientProxy:
        name, address = self.split_name(cluster)
        address = address or self._aliases.get(name, name)
        return YtClientProxy(
            self._dry_run, name, yt.YtClient(address, token=self._token, config=self._get_config(cluster, config))
        )

    def _get_config(self, cluster: str, patch: dict | None = None) -> dict:
        config = yt.default_config.get_config_from_env()
        config["tablets_ready_timeout"] = 300 * 1000
        config["backend"] = "rpc"

        cluster_config = self._per_cluster_config.get(cluster, dict())
        config.update(cluster_config)
        if patch:
            config.update(patch)
        return config

    @staticmethod
    def split_name(cluster_name: str) -> tuple[str, str | None]:
        name, _, address = cluster_name.partition("#")
        return (name, address)


def get_yt_client_factory(
    dry_run: bool, token: str | None = None, per_cluster_config: dict[str, Any] | None = None
) -> YtClientFactory:
    """Return functor to create yt clients."""
    token = token or os.getenv("YT_TOKEN")
    if not token:
        try:
            with open(os.path.expanduser("~/.yt/token")) as fd:
                token = fd.read().strip()
        except OSError:
            pass
    assert token, "Please specify token for YT via YT_TOKEN env variable."

    return YtClientFactory(dry_run, token, per_cluster_config)
