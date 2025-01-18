import logging
from typing import Any
from typing import Callable

import yt.wrapper as yt
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.client.dry import DryRun

from .yt_errors import yt_generic_error

LOG = logging.getLogger("yt sync")


class FaultyYtClientProxy(DryRun):
    def __init__(
        self,
        yt_client: YtClientProxy,
        dry_run: bool,
        fault_trigger: Callable[[str], bool],
        batch: bool = False,
    ):
        self._client: YtClientProxy = yt_client
        self._dry_run: bool = dry_run
        self._fault_trigger: Callable[[str], bool] = fault_trigger
        self._batch: bool = batch

    @property
    def underlying_client_proxy(self) -> YtClientProxy | None:
        underlying = self._client.underlying_client_proxy
        return FaultyYtClientProxy(underlying, self._dry_run, self._fault_trigger) if underlying else None

    @property
    def cluster(self) -> str:
        return self._client.cluster

    def create_batch_client(self, *args, **kwargs) -> Any:
        return FaultyYtClientProxy(
            self._client.create_batch_client(*args, **kwargs),
            self._dry_run,
            self._fault_trigger,
            batch=True,
        )

    def __getattr__(self, name) -> Any:
        v = getattr(self._client, name)
        if not callable(v):
            return v
        return self._enable_fault(name, v)

    def _enable_fault(self, method_name: str, func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            error = self._fault_trigger(method_name, *args, **kwargs)
            if not error:
                return func(*args, **kwargs)

            self._print_command(self.cluster, f"{method_name}[FAULT]", *args, **kwargs)
            if self._dry_run:
                return MockResult(error=yt_generic_error())
            elif self._batch:
                return MockResult(error=error)
            else:
                raise error

        return wrapper


class FaultyYtClientFactory(YtClientFactory):
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        fault_triggers: dict[str, Callable[[str], yt.YtError | None]],
    ):
        self._yt_client_factory = yt_client_factory
        self._fault_triggers = fault_triggers

    def is_dry_run(self) -> bool:
        return self._yt_client_factory.is_dry_run()

    def add_alias(self, name: str, address: str):
        self._yt_client_factory.add_alias(name, address)

    def __call__(self, cluster: str, *args, **kwargs) -> YtClientProxy:
        return FaultyYtClientProxy(
            self._yt_client_factory(cluster, *args, **kwargs),
            self.is_dry_run(),
            self._fault_triggers.get(cluster, lambda *args, **kwargs: False),
        )
