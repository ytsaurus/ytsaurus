from contextlib import AbstractContextManager
import logging
from typing import Callable

from .trace import do_trace

LOG = logging.getLogger("yt_sync")


class DryRun:
    def __init__(self, cluster: str):
        self.cluster = cluster

    def __getattr__(self, name: str) -> Callable:
        return lambda *args, **kwargs: self._print_command(self.cluster, name, *args, **kwargs)

    @staticmethod
    def _print_command(cluster: str, name: str, *args, **kwargs):
        do_trace(LOG.debug, cluster, name, args, kwargs)


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
