from __future__ import annotations

import logging
from typing import Any
from typing import Callable

from yt import wrapper as yt

from .dry import DryRun
from .dry import DryRunOperation
from .dry import DryRunTransaction
from .mock import MockOperation
from .mock import MockResult
from .trace import do_trace

LOG = logging.getLogger("yt_sync")


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

    def search(self, *args, **kwargs) -> Any:
        self._print_command(self.cluster, "search", *args, **kwargs)
        return self._client.search(*args, **kwargs)

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
        return YtClientProxy(
            self._dry_run,
            self.cluster,
            self._client.create_batch_client(*args, **kwargs),
            batch=True,
            underlying_client_proxy=self,
        )

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
            do_trace(LOG.debug, cluster, name, args, kwargs)
            return func(*args, **kwargs)

        return wrapper
