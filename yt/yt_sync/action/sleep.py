import logging
import time

from yt.yt_sync.core import Settings

from .base import ActionBase, ActionBatch

LOG = logging.getLogger("yt_sync")


class SleepAction(ActionBase):
    def __init__(self, wait_between_replica_switch_delay: int):
        super().__init__()
        self._delay: int = wait_between_replica_switch_delay

    def schedule_next(self, _) -> bool:
        if not self.dry_run:
            LOG.info("Wait for %s seconds", self._delay)
            time.sleep(self._delay)
        return False

    def process(self):
        pass

    @classmethod
    def make_sleep_batch(cls, cluster_name: str, settings: Settings) -> list[ActionBatch]:
        if settings.wait_between_replica_switch_delay == 0:
            return []
        sleep_batch = ActionBatch(cluster_name)
        sleep_batch.actions.append(cls(settings.wait_between_replica_switch_delay))
        return [sleep_batch]
