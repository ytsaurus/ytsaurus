from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.scenario import DumpDiffScenario
from yt.yt_sync.scenario import get_scenario_type


def test_dump_diff_registered():
    assert DumpDiffScenario == get_scenario_type("dump_diff")


def test_dump_diff_cluster_scripts():
    scenario = DumpDiffScenario(
        YtDatabase(), YtDatabase(), Settings(db_type=Settings.REPLICATED_DB), MockYtClientFactory({})
    )
    assert [] == scenario.generate_actions()
    assert not scenario.has_diff
