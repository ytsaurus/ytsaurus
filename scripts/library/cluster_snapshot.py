from yp.common import GrpcResourceExhaustedError

import yt.yson as yson

from six.moves import range

import logging
from collections import namedtuple


logger = logging.getLogger(__name__)

FILTER_BATCH_SIZE = 30  # batch_size > 30 exceeds maximum expression depth.
SELECT_BATCH_SIZE = 1000

SchedulerCluster = namedtuple("SchedulerClusterSnapshot", ["pods", "resources"])

POD_SELECTORS = [
    "/meta",
    "/spec/node_id",
    "/spec/resource_requests",
    "/spec/ip6_address_requests",
    "/spec/ip6_subnet_requests",
    "/spec/disk_volume_requests",
    "/spec/gpu_requests",
    "/spec/enable_scheduling",
    "/spec/node_filter",
    "/status/scheduled_resource_allocations",
    "/status/ip6_address_allocations",
    "/status/ip6_subnet_allocations",
    "/status/disk_volume_allocations",
    "/status/gpu_allocations",
    "/status/eviction",
    "/status/scheduling",
]

RESOURCE_SELECTORS = [
    "/meta",
    "/spec",
    "/status",
]

KEY_COLUMNS_PER_OBJECT_TYPE = {
    "pod_set": [
        "/meta/id",
    ],
    "pod": [
        "/meta/pod_set_id",
        "/meta/id",
    ],
    "resource": [
        "/meta/node_id",
        "/meta/id",
    ],
    "node": [
        "/meta/id",
    ],
}


def reconstruct_object(field_values, field_paths):
    assert len(field_values) == len(field_paths)
    obj = {}
    for value, path in zip(field_values, field_paths):
        if value is None or isinstance(value, yson.YsonEntity):
            continue
        subobj = obj
        tokens = [token for token in path.split("/") if token]
        for token in tokens[:-1]:
            if token not in subobj:
                subobj[token] = {}
            subobj = subobj[token]
            assert isinstance(subobj, dict)
        assert tokens[-1] not in subobj
        subobj[tokens[-1]] = value
    return obj


def batch_select(yp_client, object_type, filter, selectors, timestamp=None):
    if timestamp is None:
        timestamp = yp_client.generate_timestamp()

    key_columns = KEY_COLUMNS_PER_OBJECT_TYPE[object_type]
    selectors_with_key = key_columns + selectors

    def do_select(batch_size, continuation_key=None):
        if continuation_key is None:
            current_filter = filter
        else:
            current_filter = all_filter([filter, lower_bound_filter(key_columns, continuation_key)])
        logging.debug("Selecting batch")
        result = yp_client.select_objects(
            object_type,
            filter=current_filter,
            selectors=selectors_with_key,
            timestamp=timestamp,
            limit=batch_size,
        )
        logging.debug("Selected batch of size %d", len(result))
        try:
            continuation_key = max(values[:len(key_columns)] for values in result)
        except ValueError:
            continuation_key = None
        result = [values[len(key_columns):] for values in result]
        return result, continuation_key

    batch_size = SELECT_BATCH_SIZE
    result = []
    continuation_key = None
    while True:
        try:
            batch, continuation_key = do_select(batch_size, continuation_key)
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
        else:
            result.extend(batch)
            if len(batch) < batch_size:
                return result


def combibe_expressions(predicate, expressions):
    return "(" + ") {} (".format(predicate).join(expressions) + ")"


def any_filter(filters):
    return combibe_expressions("or", filters)


def all_filter(filters):
    return combibe_expressions("and", filters)


def lower_bound_filter(columns, values):
    return '([{}]) > ("{}")'.format(
        '], ['.join(columns),
        '", "'.join(values),
    )


def select_with_multiple_filters(yp_client, object_type, filters, selectors, timestamp=None):
    result = []
    for start_index in range(0, len(filters), FILTER_BATCH_SIZE):
        logging.info("Selecting with filters batch")
        filters_batch = filters[start_index: start_index + FILTER_BATCH_SIZE]
        result.extend(
            batch_select(
                yp_client,
                object_type,
                filter=any_filter(filters_batch),
                selectors=selectors,
                timestamp=timestamp,
            )
        )
    return result


def load_scheduler_cluster_snapshot(yp_client, node_segment_id):
    timestamp = yp_client.generate_timestamp()

    logging.info("Selecting pod sets")
    pod_set_ids = [r[0] for r in batch_select(
        yp_client,
        "pod_set",
        filter="[/spec/node_segment_id] = \"{}\"".format(node_segment_id),
        selectors=["/meta/id"],
        timestamp=timestamp,
    )]
    logging.info("Got %s pod sets", len(pod_set_ids))

    def filter_pod_by_pod_set_id(pod_set_id):
        return "[/meta/pod_set_id] = \"{}\"".format(pod_set_id)

    logging.info("Selecting pods")
    pods_fields = select_with_multiple_filters(
        yp_client,
        "pod",
        timestamp=timestamp,
        filters=[filter_pod_by_pod_set_id(pod_set_id) for pod_set_id in pod_set_ids],
        selectors=POD_SELECTORS,
    )
    pods = [reconstruct_object(fields, POD_SELECTORS) for fields in pods_fields]
    logging.info("Got %s pods", len(pods))

    logging.info("Selecting nodes")
    node_ids = [r[0] for r in batch_select(
        yp_client,
        "node",
        filter="[/labels/segment] = \"{}\"".format(node_segment_id),
        selectors=["/meta/id"],
        timestamp=timestamp,
    )]
    logging.info("Got %s nodes", len(node_ids))

    def filter_resource_by_node_id(node_id):
        return "[/meta/node_id] = \"{}\"".format(node_id)

    logging.info("Selecting resources")
    resources_fields = select_with_multiple_filters(
        yp_client,
        "resource",
        timestamp=timestamp,
        filters=[filter_resource_by_node_id(node_id) for node_id in node_ids],
        selectors=RESOURCE_SELECTORS,
    )
    resources = [reconstruct_object(fields, RESOURCE_SELECTORS) for fields in resources_fields]
    logging.info("Got %s resources", len(resources))

    return SchedulerCluster(pods=pods, resources=resources)
