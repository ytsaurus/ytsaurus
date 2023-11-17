from yt_commands import (ls, authors)

from yt.test_helpers import assert_items_equal

from base import SpytCluster, SpytTestBase

import pytest


class TestSpytCluster(SpytTestBase):
    @authors("alex-shishkin")
    def test_spyt_root_existence(self):
        assert_items_equal(ls(self.SPYT_ROOT_PATH), ["bin", "conf", "spark", "spyt"])

    @authors("alex-shishkin")
    @pytest.mark.timeout(60)
    def test_cluster_startup(self):
        with SpytCluster() as cluster:
            assert_items_equal(ls(cluster.discovery_path),
                               ["discovery", "logs"])
            assert_items_equal(ls(cluster.discovery_path + "/discovery"),
                               ["conf", "master_wrapper", "operation", "rest", "spark_address", "version", "webui"])
