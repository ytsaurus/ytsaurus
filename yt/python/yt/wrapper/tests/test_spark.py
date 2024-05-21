from .conftest import authors

from yt.wrapper.cypress_commands import create
from yt.wrapper.spark import find_spark_cluster

import pytest


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestSpark(object):
    @authors("sashbel")
    def test_find_spark_cluster(self):
        create("map_node", "//tmp/spark-discovery/discovery/spark_address/host1:1", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/webui/host1:2", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/rest/host1:3", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/operation/1234-5678", recursive=True)
        create("map_node", "//tmp/spark-discovery/discovery/shs/host2:4", recursive=True)

        res = find_spark_cluster("//tmp/spark-discovery")
        assert res.master_endpoint == "host1:1"
        assert res.master_web_ui_url == "host1:2"
        assert res.master_rest_endpoint == "host1:3"
        assert res.operation_id == "1234-5678"
        assert res.shs_url == "host2:4"
