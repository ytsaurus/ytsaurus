import logging

from yt.yt_sync.core.model import YtTable

from .base import GeneratorActionBase
from .gradual_remount import TABLETS_REMOUNTED, SLEEP_AFTER_TABLET

LOG = logging.getLogger("yt_sync")

FORCED_COMPACTION_REVISION = "forced_compaction_revision"

_FORCE_COMPACTION_ATTRIBUTES = (
    FORCED_COMPACTION_REVISION,
    TABLETS_REMOUNTED,
    SLEEP_AFTER_TABLET,
)


class SetForceCompactionRevisionAction(GeneratorActionBase):
    def __init__(self, table: YtTable, revision: int):
        super().__init__()
        self._table: YtTable = table
        self._revision = revision

    def act(self):
        LOG.warning(
            "Set forced compaction revision %d for %s:%s", self._revision, self._table.cluster_name, self._table.path
        )
        assert self._table.exists
        assert self._batch_client
        result = yield self._batch_client.set(f"{self._table.path}/@{FORCED_COMPACTION_REVISION}", self._revision)
        self.assert_response(result)
        return
        yield


class CleanupForceCompactionAttributesAction(GeneratorActionBase):
    def __init__(self, table: YtTable):
        super().__init__()
        self._table: YtTable = table

    def act(self):
        LOG.warning("Cleaning up after force compaction for table %s:%s", self._table.cluster_name, self._table.path)
        assert self._table.exists
        assert self._batch_client
        results = []
        for attribute in _FORCE_COMPACTION_ATTRIBUTES:
            results.append(self._batch_client.remove(f"{self._table.path}/@{attribute}"))
        yield results
        for result in results:
            self.assert_response(result)
