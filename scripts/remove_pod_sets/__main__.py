#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yp.client import YpClient, find_token

import argparse
import logging


def configure_logging():
    logging.basicConfig(
        format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s", level=logging.DEBUG,
    )


def sanity_check_filters(arguments):
    if arguments.force:
        logging.warning("Bypassing sanity checks.")
        return
    if len(arguments.pod_set_id_prefix) < 10:
        raise RuntimeError(
            "Too small prefix of the pod set id {}. "
            "If it is not an error, use argument --force to bypass.".format(
                arguments.pod_set_id_prefix
            )
        )


def main_impl(yp_client, arguments):
    sanity_check_filters(arguments)
    responses = yp_client.select_objects("pod_set", selectors=["/meta/id"])
    all_pod_set_ids = map(lambda response: response[0], responses)
    logging.info("Selected %d pod sets", len(all_pod_set_ids))
    filtered_pod_set_ids = filter(
        lambda pod_set_id: pod_set_id.startswith(arguments.pod_set_id_prefix), all_pod_set_ids,
    )
    logging.info("Remained %d pod sets after filtration", len(filtered_pod_set_ids))
    for pod_set_id in filtered_pod_set_ids:
        logging.info("Removing pod set (id: %s)", pod_set_id)
        if not arguments.dry_run:
            yp_client.remove_object("pod_set", pod_set_id)


def main(arguments):
    configure_logging()
    with YpClient(address=arguments.cluster, config=dict(token=find_token())) as yp_client:
        main_impl(yp_client, arguments)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Remove pod sets by given filters")
    parser.add_argument(
        "--cluster", type=str, required=True, help="YP cluster address",
    )
    parser.add_argument(
        "--pod-set-id-prefix",
        type=str,
        required=True,
        help="Remove pod sets with given pod set id prefix",
    )
    parser.add_argument(
        "--force", action="store_true", default=False,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Do not actually remove pod sets, just show filtered pod set ids",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
