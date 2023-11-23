from yt_commands import (ls, authors)

from spyt.testing.common.helpers import assert_items_equal

from base import SpytInternalTestBase

import pytest


class TestSpytCluster(SpytInternalTestBase):
    @authors("alex-shishkin")
    def test_spyt_root_existence(self):
        assert_items_equal(ls("//home/spark"), ["bin", "conf", "spark", "spyt"])

    @authors("alex-shishkin")
    @pytest.mark.timeout(45)
    def test_cluster_startup(self):
        with self.spyt_cluster() as cluster:
            assert_items_equal(ls(cluster.discovery_path),
                               ["discovery", "logs"])
            assert_items_equal(ls(cluster.discovery_path + "/discovery"),
                               ["conf", "master_wrapper", "operation", "rest", "spark_address", "version", "webui"])
