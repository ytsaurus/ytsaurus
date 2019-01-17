#!/usr/bin/env python

from yp.client import YpClient, find_token
from yp.common import GrpcResourceExhaustedError

from yt.yson.convert import yson_to_json

import argparse
import json
import logging


# TODO(bidzilya): Generalize this tool and similar code from scheduler simulator player.


def clear_fields(objects, field_paths):
    for object_ in objects:
        for field_path in field_paths:
            subobject = object_
            for i, token in enumerate(field_path):
                if token not in subobject:
                    break
                if i + 1 == len(field_path):
                    subobject.pop(token)
                else:
                    subobject = subobject[token]


def select(yp_client, *args, **kwargs):
    assert "offset" not in kwargs
    assert "limit" not in kwargs
    assert "selectors" not in kwargs

    def do_select(batch_size):
        result = []
        offset = 0
        while True:
            logging.debug("Selecting batch")
            batch_responses = yp_client.select_objects(
                *args,
                selectors=[""],
                offset=offset,
                limit=batch_size,
                **kwargs
            )
            batch = map(lambda response: response[0], batch_responses)
            logging.debug("Selected batch of size %d", len(batch))
            result.extend(batch)
            if len(batch) < batch_size:
                break
            offset += batch_size
        return result

    batch_size = kwargs.pop("batch_size", 100)
    while True:
        try:
            return do_select(batch_size)
        except GrpcResourceExhaustedError:
            if batch_size == 1:
                raise
            new_batch_size = batch_size // 2
            logging.warning(
                "Grpc resource exhausted with batch_size = %d. "
                "Probably too big response. Will try smaller batch_size = %d",
                batch_size,
                new_batch_size,
            )
            batch_size = new_batch_size


def any_filter(filters):
    return " or ".join(filters)


def select_with_multiple_filters(yp_client, *args, **kwargs):
    filters = kwargs.pop("filters")
    filter_batch_size = kwargs.pop("filter_batch_size", 30)
    result = []
    for start_index in xrange(0, len(filters), filter_batch_size):
        logging.info("Selecting with filters batch")
        filters_batch = filters[start_index: start_index + filter_batch_size]
        result.extend(
            select(
                yp_client,
                *args,
                filter=any_filter(filters_batch),
                **kwargs
            )
        )
    return result


def select_scheduler_objects(yp_client, node_segment_id):
    timestamp = yp_client.generate_timestamp()

    logging.info("Selecting pod sets")
    pod_sets = select(
        yp_client,
        "pod_set",
        filter="[/spec/node_segment_id] = \"{}\"".format(node_segment_id),
        timestamp=timestamp,
    )

    def filter_pod_by_pod_set_id(pod_set_id):
        return "[/meta/pod_set_id] = \"{}\"".format(pod_set_id)

    logging.info("Selecting pods")
    pod_set_ids = map(lambda pod_set: pod_set["meta"]["id"], pod_sets)
    pods = select_with_multiple_filters(
        yp_client,
        "pod",
        timestamp=timestamp,
        filters=map(filter_pod_by_pod_set_id, pod_set_ids),
    )

    logging.info("Selecting nodes")
    nodes = select(
        yp_client,
        "node",
        filter="[/labels/segment] = \"{}\"".format(node_segment_id),
        timestamp=timestamp,
    )

    def filter_resource_by_node_id(node_id):
        return "[/meta/node_id] = \"{}\"".format(node_id)

    logging.info("Selecting resources")
    node_ids = map(lambda node: node["meta"]["id"], nodes)
    resources = select_with_multiple_filters(
        yp_client,
        "resource",
        timestamp=timestamp,
        filters=map(filter_resource_by_node_id, node_ids),
    )

    return (pods, resources)


def configure_logger():
    FORMAT = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = [handler]


def main(arguments):
    configure_logger()
    yp_client = YpClient(address=arguments.cluster, config=dict(token=find_token()))
    pods, resources = select_scheduler_objects(yp_client, arguments.node_segment_id)
    # Filter potentially problematic for yson_to_json conversion and useless for scheduler fields.
    clear_fields(
        pods,
        [
            ["status", "agent"],
            ["spec", "iss_payload"],
            ["spec", "iss"],
        ],
    )
    objects = pods + resources
    print(json.dumps(yson_to_json(objects), indent=4))


def parse_arguments():
    parser = argparse.ArgumentParser(description="Dump YP cluster objects related to scheduler")
    parser.add_argument("--cluster", type=str, required=True, help="YP cluster address")
    parser.add_argument(
        "--node-segment-id",
        type=str,
        required=True,
        help="Apply filter by node segment id",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
