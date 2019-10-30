#!/usr/bin/env python

from yp.scripts.library.cluster_snapshot import load_scheduler_cluster_snapshot

from yp.client import YpClient, find_token

from yt.yson.convert import yson_to_json

import argparse
import json
import logging
import sys


def configure_logger():
    FORMAT = "%(asctime)s\t%(levelname)s\t%(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = [handler]


def parse_arguments():
    parser = argparse.ArgumentParser(description="Dump YP cluster objects related to scheduler")
    parser.add_argument("--cluster", required=True, help="YP cluster address")
    parser.add_argument("--node-segment-id", required=True, help="Apply filter by node segment id")
    return parser.parse_args()


def main(arguments):
    configure_logger()
    yp_client = YpClient(address=arguments.cluster, config=dict(token=find_token()))
    snapshot = load_scheduler_cluster_snapshot(yp_client, arguments.node_segment_id)
    objects = snapshot.pods + snapshot.resources + snapshot.pod_sets + snapshot.nodes
    json.dump(yson_to_json(objects), sys.stdout, indent=4)


if __name__ == "__main__":
    main(parse_arguments())
