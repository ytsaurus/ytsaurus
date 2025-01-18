from abc import ABC, abstractmethod
from contextlib import contextmanager
import logging

from yt.yt_sync.core.client import YtClientProxy

LOG = logging.getLogger("yt_sync")


class DbLockBase(ABC):
    def __init__(self):
        pass

    @abstractmethod
    @contextmanager
    def lock(self):
        raise NotImplementedError


class DummyLock(DbLockBase):
    def __init__(self):
        super().__init__()

    @contextmanager
    def lock(self):
        LOG.debug("DummyLock.try_acquire()")
        try:
            yield self
        finally:
            LOG.debug("DummyLock.release()")


class YtSyncLock(DbLockBase):
    def __init__(self, yt_client: YtClientProxy, *folders: str):
        super().__init__()
        self._client = yt_client
        self._folders = list(folders)
        assert len(self._folders) > 0, "Empty folders list in YtSyncLock"

    @contextmanager
    def lock(self):
        LOG.debug("YT Sync lock: try acquire")
        try:
            for folder in self._folders:
                self._client.create("map_node", folder, ignore_existing=True, recursive=True)
            with self._client.Transaction():
                for folder in self._folders:
                    self._client.lock(folder, mode="shared", child_key="yt_sync")
                yield self
        finally:
            LOG.debug("YT Sync lock: release")
