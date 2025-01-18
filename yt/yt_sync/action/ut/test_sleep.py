import time

import pytest

from yt.yt_sync.action.sleep import SleepAction
from yt.yt_sync.core import Settings

from .helpers import empty_mock_client


@pytest.mark.parametrize("delay", [1, 2])
@pytest.mark.parametrize("dry_run", [True, False])
def test_sleep(delay: int, dry_run: bool):
    action = SleepAction(delay)
    action.dry_run = dry_run
    before = int(time.time())
    assert action.schedule_next(empty_mock_client()) is False
    after = int(time.time())
    if dry_run:
        assert after - before <= 1
    else:
        assert after - before >= delay


@pytest.mark.parametrize("delay", [0, 1])
def test_make_sleep_batch(delay: int):
    settings = Settings(db_type=Settings.REPLICATED_DB, wait_between_replica_switch_delay=delay)
    batches = SleepAction.make_sleep_batch("k", settings)
    if delay:
        assert batches
    else:
        assert not batches
