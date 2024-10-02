import pytest

from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.scenario import SwitchReplicaScenario


@pytest.fixture
def db() -> YtDatabase:
    db = YtDatabase()
    db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
    return db


def test_empty(db: YtDatabase):
    scenario = SwitchReplicaScenario(
        db,
        db,
        Settings(db_type=Settings.REPLICATED_DB, always_async={"remote2"}),
        MockYtClientFactory({}),
    )
    scenario.setup(desired_sync_replicas=["remote1"])
    scenario.run()


def test_sync_always_async_intersection(db: YtDatabase):
    scenario = SwitchReplicaScenario(
        db,
        db,
        Settings(db_type=Settings.REPLICATED_DB, always_async={"remote2"}),
        MockYtClientFactory({}),
    )
    scenario.setup(desired_sync_replicas=["remote1", "remote2"])
    with pytest.raises(AssertionError):
        scenario.run()
