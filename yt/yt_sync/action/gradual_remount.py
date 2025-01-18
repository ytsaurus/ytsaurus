import logging
import time

from yt.yt_sync.core.model import YtTable

from .base import GeneratorActionBase

LOG = logging.getLogger("yt_sync")

TABLETS_REMOUNTED = "_tablets_remounted"
SLEEP_AFTER_TABLET = "_sleep_after_tablet"


class GradualRemountActon(GeneratorActionBase):
    def __init__(self, table: YtTable, sleep_after_tablet: float = 1):
        super().__init__()
        self._table: YtTable = table
        self._tablets_remounted = f"{self._table.path}/@{TABLETS_REMOUNTED}"
        self._sleep_after_tablet = sleep_after_tablet

    def act(self):
        LOG.warning(
            "Starting gradual remount of table %s:%s, sleep after tablet remount: %.2f seconds",
            self._table.cluster_name,
            self._table.path,
            self._sleep_after_tablet,
        )
        assert self._table.exists
        assert self._batch_client
        yt_client = self._batch_client.underlying_client_proxy
        assert yt_client
        tablets_remounted = yt_client.get(self._tablets_remounted) or 0
        while tablets_remounted < yt_client.get(f"{self._table.path}/@tablet_count"):
            assert self._batch_client
            LOG.warning("Remount tablet %s of %s:%s", tablets_remounted, self._table.cluster_name, self._table.path)
            result = yield self._batch_client.remount_table(self._table.path, tablets_remounted, tablets_remounted)
            self.assert_response(result)
            tablets_remounted += 1
            yt_client.set(self._tablets_remounted, tablets_remounted)
            LOG.info("Sleeping for %.2f seconds after tablet remount", self._sleep_after_tablet)
            if not self.dry_run:
                time.sleep(self._sleep_after_tablet)
