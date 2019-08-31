from .conftest import create_nodes, wait

from yp.common import YtResponseError, YpNoSuchObjectError

import pytest


@pytest.mark.usefixtures("yp_env")
class TestNodeSegments(object):
    def test_simple(self, yp_env):
        yp_client = yp_env.yp_client

        segment_id = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": "0=0"}})
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": segment_id}})
        assert yp_client.get_object("pod_set", pod_set_id, selectors=["/spec/node_segment_id"])[0] == segment_id
        assert yp_client.select_objects("pod_set", filter="[/spec/node_segment_id] = \"{}\"".format(segment_id), selectors=["/meta/id"]) == [[pod_set_id]]

    def test_cannot_remove_nonempty(self, yp_env):
        yp_client = yp_env.yp_client

        segment_id = yp_client.create_object("node_segment", attributes={"spec": {"node_filter": "0=0"}})
        pod_set_id = yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": segment_id}})
        with pytest.raises(YtResponseError):
            yp_client.remove_object("node_segment", segment_id)
        yp_client.remove_object("pod_set", pod_set_id)
        yp_client.remove_object("node_segment", segment_id)

    def test_pod_set_must_refer_to_valid_node_segment(self, yp_env):
        yp_client = yp_env.yp_client

        with pytest.raises(YpNoSuchObjectError):
            yp_client.create_object("pod_set", attributes={"spec": {"node_segment_id": "nonexisting"}})

    def test_total_resources(self, yp_env):
        yp_client = yp_env.yp_client

        node_segment_id = yp_client.create_object("node_segment", attributes={
            "spec": {
                "node_filter": "%true"
            }
        })
        get_total_resources = lambda: yp_client.get_object("node_segment", node_segment_id,
                                                           selectors=["/status/total_resources"])[0]
        cpu_capacity_per_node = 1000
        memory_capacity_per_node = 5000
        network_bandwidth_per_node = 400

        make_nodes = lambda nodes_count: create_nodes(yp_client, nodes_count,
                                                      cpu_total_capacity=cpu_capacity_per_node,
                                                      memory_total_capacity=memory_capacity_per_node,
                                                      network_bandwidth=network_bandwidth_per_node)
        def check_totals(nodes_count):
            import time
            time.sleep(10)
            totals = get_total_resources()
            return totals["cpu"]["capacity"] == cpu_capacity_per_node * nodes_count \
                and totals["memory"]["capacity"] == memory_capacity_per_node * nodes_count \
                and totals["network"]["bandwidth"] == network_bandwidth_per_node * nodes_count

        make_nodes(5)
        wait(lambda: check_totals(5))

        make_nodes(20)
        wait(lambda: check_totals(25))

        make_nodes(75)
        wait(lambda: check_totals(100))
