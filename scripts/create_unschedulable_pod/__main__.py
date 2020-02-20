#!/usr/bin/env python

from yp.scripts.library.cluster_snapshot import load_scheduler_cluster_snapshot

from yp.client import YpClient, find_token

import argparse
import logging


def configure_logger():
    FORMAT = "%(asctime)s\t%(levelname)s\t%(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = [handler]


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--node-segment-id", required=True)
    return parser.parse_args()


def main(arguments):
    configure_logger()
    with YpClient(address=arguments.cluster, config=dict(token=find_token())) as yp_client:
        snapshot = load_scheduler_cluster_snapshot(
            yp_client, node_segment_id=arguments.node_segment_id, object_types=["node", "resource"],
        )

        pod_cpu = None
        pod_node_id = None
        for resource in snapshot.resources:
            if resource["meta"]["kind"] != "cpu":
                continue

            free_cpu = resource["status"].get("free", {}).get("cpu", {}).get("capacity", 0)
            used_cpu = resource["status"].get("used", {}).get("cpu", {}).get("capacity", 0)
            if free_cpu <= 100 or used_cpu <= 0:
                continue

            node_id = resource["meta"]["node_id"]

            node_hfsm_state = yp_client.get_object(
                "node", node_id, selectors=["/status/hfsm/state"]
            )[0]
            if node_hfsm_state != "up":
                continue

            pod_cpu = free_cpu + 1
            pod_node_id = node_id
            break

        assert pod_node_id is not None

        pod_set_id = yp_client.create_object(
            "pod_set",
            attributes=dict(
                spec=dict(
                    node_segment_id=arguments.node_segment_id,
                    node_filter='[/labels/topology/node] = "{}"'.format(pod_node_id),
                )
            ),
        )
        logging.info("Created pod set {} with node {}".format(pod_set_id, pod_node_id))

        pod_id = yp_client.create_object(
            "pod",
            attributes=dict(
                meta=dict(pod_set_id=pod_set_id),
                spec=dict(enable_scheduling=True, resource_requests=dict(vcpu_guarantee=pod_cpu),),
            ),
        )
        logging.info("Created pod {}".format(pod_id))


if __name__ == "__main__":
    main(parse_arguments())
