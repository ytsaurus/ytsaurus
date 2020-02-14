from yp.common import GrpcResourceExhaustedError

import yt.yson as yson
from yt.yson.convert import yson_to_json

from six.moves import range

import json
import logging
from collections import namedtuple


logger = logging.getLogger(__name__)

FILTER_BATCH_SIZE = 30  # batch_size > 30 exceeds maximum expression depth.
SELECT_BATCH_SIZE = 1000

SchedulerClusterBase = namedtuple("SchedulerClusterBase", [
    "pods",
    "resources",
    "nodes",
    "pod_sets",
    "pod_disruption_budgets",
    "internet_addresses",
    "network_projects",
    "virtual_services",
])


class SchedulerCluster(SchedulerClusterBase):
    # NB: as namedtuple uses __new__ instead of __init__, we have to do the same.
    def __new__(cls, **kwargs):
        for field in cls._fields:
            kwargs[field] = kwargs.get(field, [])
        return super(SchedulerCluster, cls).__new__(cls, **yson_to_json(kwargs))

    @classmethod
    def from_flatten(cls, objects):
        attributes = {}
        for field in cls._fields:
            attributes[field] = []
        for obj in objects:
            attr = cls._get_attribute_name_by_type(obj["meta"]["type"])
            attributes[attr].append(obj)
        return cls(**yson_to_json(attributes))

    @classmethod
    def get_object_types(cls):
        return [type_[:-1] if type_ != "internet_addresses" else type_[:-2]
                for type_ in cls._fields]

    def iter_pods_with_pod_set(self):
        id_to_pod_set = {pod_set["meta"]["id"]: pod_set for pod_set in self.pod_sets}
        for pod in self.pods:
            yield pod, id_to_pod_set[pod["meta"]["pod_set_id"]]

    def get_by_type(self, type_):
        return getattr(self, self._get_attribute_name_by_type(type_))

    def set_by_type(self, type_, objects):
        return setattr(self, self._get_attribute_name_by_type(type_), objects)

    @staticmethod
    def _get_attribute_name_by_type(type_):
        if type_ == "internet_address":
            return type_ + "es"
        else:
            return type_ + "s"

    def dump(self, file_):
        snapshot = {type_: getattr(self, type_) for type_ in self._fields}
        json.dump(snapshot, file_, indent=4)

    def dump_to_path(self, path):
        logging.info("Dumping YP scheduler objects to file %s", path)
        with open(path, "w") as f:
            self.dump(f)

    @classmethod
    def load(cls, file_):
        snapshot = json.load(file_)
        return cls(**snapshot)

    @classmethod
    def load_from_path(cls, path):
        logging.info("Reading YP scheduler objects from file %s", path)
        with open(path, "r") as f:
            snapshot = cls.load(f)

        for type_ in snapshot.get_object_types():
            logging.info(
                "Read %d YP objects of type '%s'",
                len(snapshot.get_by_type(type_)),
                type_,
            )

        return snapshot

TYPE_SELECTORS = {
    "pod_set": [
        "/meta",
        "/spec",
        "/labels",
    ],
    "pod": [
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
        "/labels",
    ],
    "node": [
        "/meta",
        "/spec",
        "/labels",
    ],
    "resource": [
        "/meta",
        "/spec",
        "/status",
        "/labels",
    ],
    "internet_address": [
        "/meta",
        "/spec",
        "/status",
        "/labels",
    ],
    "network_project": [
        "/meta",
        "/spec",
        "/labels",
    ],
    "virtual_service": [
        "/meta",
        "/spec",
        "/labels",
    ],
    "pod_disruption_budget": [
        "/meta",
        "/spec",
        "/status",
        "/labels",
    ]
}

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
    "internet_address": [
        "/meta/ip4_address_pool_id",
        "/meta/id",
    ],
    "network_project": [
        "/meta/id",
    ],
    "virtual_service": [
        "/meta/id",
    ],
    "pod_disruption_budget": [
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
        logging.debug("Selecting batch of %ss", object_type)
        result = yp_client.select_objects(
            object_type,
            filter=current_filter,
            selectors=selectors_with_key,
            timestamp=timestamp,
            limit=batch_size,
        )
        logging.debug("Selected batch of %ss (size: %d)", object_type, len(result))
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


def select_pod_sets_and_pods(yp_client, timestamp, node_segment_id, pod_set_selectors,
                             pod_selectors):
    assert pod_set_selectors or pod_selectors

    if node_segment_id:
        pod_sets_filter = "[/spec/node_segment_id] = \"{}\"".format(node_segment_id)
    else:
        pod_sets_filter = "%true"

    logging.info("Selecting pod sets")
    pod_set_selectors_with_id = ["/meta/id"] + pod_set_selectors
    pod_sets_fields = batch_select(
        yp_client,
        "pod_set",
        filter=pod_sets_filter,
        selectors=pod_set_selectors_with_id,
        timestamp=timestamp,
    )
    pod_sets = [reconstruct_object(fields, pod_set_selectors_with_id) for fields in pod_sets_fields]
    logging.info("Got %s pod sets", len(pod_sets))

    if not pod_selectors:
        return pod_sets, None

    pod_set_ids = [ps["meta"]["id"] for ps in pod_sets]

    def filter_pod_by_pod_set_id(pod_set_id):
        return "[/meta/pod_set_id] = \"{}\"".format(pod_set_id)

    logging.info("Selecting pods")
    pods_fields = select_with_multiple_filters(
        yp_client,
        "pod",
        timestamp=timestamp,
        filters=[filter_pod_by_pod_set_id(pod_set_id) for pod_set_id in pod_set_ids],
        selectors=pod_selectors,
    )
    pods = [reconstruct_object(fields, TYPE_SELECTORS["pod"]) for fields in pods_fields]
    logging.info("Got %s pods", len(pods))

    if pod_set_selectors:
        return pod_sets, pods
    else:
        return None, pods


def select_nodes_and_resources(yp_client, timestamp, node_segment_id, node_selectors,
                               resource_selectors):
    logging.info("Selecting nodes")
    node_selectors_with_id = ["/meta/id"] + node_selectors

    if node_segment_id:
        nodes_filter = "[/labels/segment] = \"{}\"".format(node_segment_id)
    else:
        nodes_filter = "%true"

    nodes_fields = batch_select(
        yp_client,
        "node",
        filter=nodes_filter,
        selectors=node_selectors_with_id,
        timestamp=timestamp,
    )
    nodes = [reconstruct_object(fields, node_selectors_with_id) for fields in nodes_fields]
    logging.info("Got %s nodes", len(nodes))

    if not resource_selectors:
        return nodes, None

    node_ids = [n["meta"]["id"] for n in nodes]

    def filter_resource_by_node_id(node_id):
        return "[/meta/node_id] = \"{}\"".format(node_id)

    logging.info("Selecting resources")
    resources_fields = select_with_multiple_filters(
        yp_client,
        "resource",
        timestamp=timestamp,
        filters=[filter_resource_by_node_id(node_id) for node_id in node_ids],
        selectors=TYPE_SELECTORS["resource"],
    )
    resources = [reconstruct_object(fields, TYPE_SELECTORS["resource"]) for fields in resources_fields]
    logging.info("Got %s resources", len(resources))

    if node_selectors:
        return nodes, resources
    else:
        return None, resources


def load_scheduler_cluster_snapshot(yp_client, node_segment_id=None, object_types=None):
    if object_types is None:
        object_types = SchedulerCluster.get_object_types()

    timestamp = yp_client.generate_timestamp()

    all_objects = []

    def try_extend_all_objects(*args):
        for objects_or_none in args:
            if objects_or_none is not None:
                all_objects.extend(objects_or_none)

    if "pod_set" in object_types or "pod" in object_types:
        if "pod_set" in object_types:
            pod_set_selectors = TYPE_SELECTORS["pod_set"]
        else:
            pod_set_selectors = []
        if "pod" in object_types:
            pod_selectors = TYPE_SELECTORS["pod"]
        else:
            pod_selectors = []

        pod_sets, pods = select_pod_sets_and_pods(yp_client, timestamp, node_segment_id,
                                                  pod_set_selectors, pod_selectors)

        try_extend_all_objects(pod_sets, pods)

    if "node" in object_types or "resource" in object_types:
        if "node" in object_types:
            node_selectors = TYPE_SELECTORS["node"]
        else:
            node_selectors = []
        if "resource" in object_types:
            resource_selectors = TYPE_SELECTORS["resource"]
        else:
            resource_selectors = []

        nodes, resources = select_nodes_and_resources(yp_client, timestamp, node_segment_id,
                                                      node_selectors, resource_selectors)

        try_extend_all_objects(nodes, resources)

    other_types = sorted(set(SchedulerCluster.get_object_types()) - {"pod_set", "pod", "resource", "node"})
    for object_type in other_types:
        if object_type in object_types:
            logging.info("Selecting objects of type '%s'", object_type)
            objects_fields = batch_select(
                yp_client,
                object_type,
                filter="%true",
                selectors=TYPE_SELECTORS[object_type],
                timestamp=timestamp,
            )
            objects = [reconstruct_object(fields, TYPE_SELECTORS[object_type])
                       for fields in objects_fields]

            try_extend_all_objects(objects)

    return SchedulerCluster.from_flatten(all_objects)
