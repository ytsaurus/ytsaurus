#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yp.client import YpClient, find_token
from yp.cli_helpers import ParseStructuredArgument
from yt.common import get_value

import argparse
import logging


def configure_logging():
    logging.basicConfig(
        format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
        level=logging.DEBUG,
    )


def main_impl(yp_client, arguments):
    filter = get_value(arguments.filter, 'True')

    if arguments.node:
        filter = "({}) and [/spec/node_id] = \"{}\"".format(filter, arguments.node)
    elif arguments.node_list:
        node_ids = [line.rstrip('\n') for line in open(arguments.node_list)]
        filter = ("({}) and (" + " or ".join(["[/spec/node_id] = \"{}\""] * len(node_ids)) + ")").format(filter, *node_ids)

    responses = yp_client.select_objects("pod", selectors=["/meta/id"], filter=filter)
    filtered_pod_ids = map(lambda response: response[0], responses)
    logging.info("Selected %d pods", len(filtered_pod_ids))

    tx_id = yp_client.start_transaction()
    for pod_id in filtered_pod_ids:
        logging.info("Touching pod (id: %s)", pod_id)
    if not arguments.dry_run:
        yp_client.touch_master_spec_timestamps(filtered_pod_ids, transaction_id=tx_id)
    yp_client.commit_transaction(tx_id)


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
        "--cluster",
        type=str,
        required=True,
        help="YP cluster address",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Touch pods with given pod id filter",
    )
    parser.add_argument(
        "--node",
        type=str,
        default=None,
        help="Restrict filtering to the given node",
    )
    parser.add_argument(
        "--node-list",
        type=str,
        default=None,
        help="Restrict filtering to the list of nodes in given file, one node id at each line",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Do not actually touch pods, just show filtered pod ids",
    )
    parser.add_argument(
        "--config",
        help="Configuration of client in YSON format",
        action=ParseStructuredArgument
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
