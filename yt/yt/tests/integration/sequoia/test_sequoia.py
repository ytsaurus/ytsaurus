from yt_env_setup import YTEnvSetup

from yt_commands import authors, create

##################################################################


class TestSequoia(YTEnvSetup):
    USE_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True
    NUM_CYPRESS_PROXIES = 1

    NUM_SECONDARY_MASTER_CELLS = 0

    @authors("kvk1920")
    def test_create(self):
        create("map_node", "//tmp/some_node")
