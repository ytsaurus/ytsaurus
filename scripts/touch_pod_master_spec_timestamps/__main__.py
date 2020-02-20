#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yp.client import YpClient, find_token
from yt.common import get_value
from yt.wrapper.cli_helpers import ParseStructuredArgument

import argparse
import logging


def configure_logging():
    logging.basicConfig(
        format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s", level=logging.DEBUG,
    )


def main_impl(yp_client, arguments):
    def get_filter(optional_node_ids):
        filter = ""
        if not optional_node_ids:
            filter = arguments.filter
        else:
            assert len(optional_node_ids) > 0
            filter = (
                "({}) and ("
                + " or ".join(['[/spec/node_id] = "{}"'] * len(optional_node_ids))
                + ")"
            ).format(arguments.filter, *optional_node_ids)
        return filter

    def process_batch(optional_node_ids):
        filter = get_filter(optional_node_ids)
        responses = yp_client.select_objects("pod", selectors=["/meta/id"], filter=filter)
        return map(lambda response: response[0], responses)

    def get_optional_node_ids():
        optional_node_ids = None
        if arguments.node:
            optional_node_ids = [arguments.node]
        elif arguments.node_list:
            optional_node_ids = [line.rstrip("\n") for line in open(arguments.node_list)]
        return optional_node_ids

    def get_optional_node_batch(optional_node_ids, batch_index):
        if not optional_node_ids:
            return None
        else:
            return optional_node_ids[batch_index::batch_step]

    def get_number_of_batches(optional_node_ids):
        if not optional_node_ids:
            return 1
        else:
            return 1 + (len(optional_node_ids) - 1) // arguments.batch_size

    assert arguments.batch_size > 0
    optional_node_ids = get_optional_node_ids()
    batch_step = get_number_of_batches(optional_node_ids)

    for i in range(batch_step):
        filtered_pod_ids = process_batch(get_optional_node_batch(optional_node_ids, i))
        logging.info("Selected %d pods in %d-th batch", len(filtered_pod_ids), i + 1)
        for pod_id in filtered_pod_ids:
            logging.info("Touching pod (id: %s)", pod_id)
        if not arguments.dry_run:
            yp_client.touch_pod_master_spec_timestamps(filtered_pod_ids)


def main(arguments):
    configure_logging()

    config = arguments.config
    token = find_token()
    if token is not None:
        config = get_value(config, {})
        config["token"] = token

    if arguments.node and arguments.node_list:
        raise Exception("Only one of --node and --node-list parameters may be given")

    with YpClient(address=arguments.cluster, config=config) as yp_client:
        main_impl(yp_client, arguments)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Touch pods by given filter")
    parser.add_argument(
        "--cluster", type=str, required=True, help="YP cluster address",
    )
    parser.add_argument(
        "--filter", type=str, default="True", help="Touch pods with given pod id filter",
    )
    parser.add_argument(
        "--node", type=str, default=None, help="Restrict filtering to the given node",
    )
    parser.add_argument(
        "--node-list",
        type=str,
        default=None,
        help="Restrict filtering to the list of nodes in given file, one node id at each line",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="Group nodes in batches with given size",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Do not actually touch pods, just show filtered pod ids",
    )
    parser.add_argument(
        "--config", help="Configuration of client in YSON format", action=ParseStructuredArgument,
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
