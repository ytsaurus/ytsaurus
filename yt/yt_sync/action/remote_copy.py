from enum import Enum
import logging
from typing import Any

from yt.yt_sync.core import Settings
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import LongOperationActionBase

LOG = logging.getLogger("yt_sync")


class _State(Enum):
    INIT = 0
    IN_PROGRESS = 1
    DONE = 2


class RemoteCopyAction(LongOperationActionBase):
    def __init__(self, settings: Settings, from_table: YtTable, to_table: YtTable):
        assert (
            f"{from_table.cluster_name}:{from_table.path}" != f"{to_table.cluster_name}:{to_table.path}"
        ), "Can't run copy to self"
        assert (
            not from_table.is_replicated
        ), f"Expected non-replicated table: {from_table.cluster_name}:{from_table.path}"
        assert not from_table.is_ordered, f"Queues are not supported: {from_table.cluster_name}:{from_table.path}"
        assert (
            not from_table.has_hunks
        ), f"Remote copy not supported for table with hunks: {from_table.cluster_name}:{from_table.path}"

        super().__init__(settings)
        self._from_table: YtTable = from_table
        self._to_table: YtTable = to_table
        self._op: Any = None
        self._state = _State.INIT

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert self._from_table.exists and self._to_table.exists, "Tables are expected to exist"
        match self._state:
            case _State.DONE:
                return False
            case _State.INIT:
                LOG.info("%s:%s start remote copy", self._to_table.cluster_name, self._to_table.path)
                assert batch_client.underlying_client_proxy
                self._op = batch_client.underlying_client_proxy.run_remote_copy(
                    self._from_table.path,
                    self._to_table.path,
                    self._from_table.cluster_name,
                    spec=self.settings.get_operation_spec(batch_client.cluster, "remote_copy"),
                    copy_attributes=True,
                    sync=False,
                )
                self._state = _State.IN_PROGRESS
                return True
            case _State.IN_PROGRESS:
                return True

    def process(self):
        assert self._state != _State.INIT, "Called process before schedule_next"
        match self._state:
            case _State.DONE:
                return
            case _State.IN_PROGRESS:
                state = self._op.get_state()
                if state.is_unsuccessfully_finished():
                    raise Exception(
                        f"YT run_remote_copy({self._from_table.rich_path} -> {self._to_table.rich_path}) failed"
                        + f" with state={state} and error={self._op.get_error()}"
                    )
                if state.is_finished():
                    LOG.info("%s finish remote copy", self._to_table.rich_path)
                    self._state = _State.DONE
                    return
                LOG.debug("%s remote copy state: %s", self._to_table.rich_path, state)
