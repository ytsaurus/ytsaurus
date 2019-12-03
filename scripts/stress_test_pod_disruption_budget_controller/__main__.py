#!/usr/bin/env python

from yp.scripts.library.batch_client import create_batch_yp_client
from yp.scripts.library.cluster_snapshot import batch_select

from yp.client import YpClient, find_token
from yp.cli_helpers import AliasedSubParsersAction

from yt.common import get_value

from copy import deepcopy
import argparse
import enum
import logging
import os

DEFAULT_ID_PREFIX = "stress-test-pdbcontroller-"

DEFAULT_POD_DISRUPTION_BUDGET_ATTRS = dict(spec=dict(max_pods_unavailable=2, max_pod_disruptions_between_syncs=1))
DEFAULT_POD_SET_ATTRS = dict(spec=dict(node_segment_id="default"))
DEFAULT_POD_ATTRS = dict(spec=dict(enable_scheduling=True))


class ObjectTypes(enum.Enum):
    POD = "pod"
    POD_SET = "pod_set"
    POD_DISRUPTION_BUDGET = "pod_disruption_budget"


def configure_logging(verbose):
    logging.basicConfig(
        format="%(asctime)-15s %(levelname)s %(message)s",
        level=logging.DEBUG if verbose else logging.INFO,
    )


def select_objects_batch(yp_client, object_type, filter_query, selectors):
    return [response[0] for response in batch_select(
        yp_client, object_type, filter_query, selectors)]


def create_objects_batch(batch_yp_client, object_type, object_ids, default_object_attrs, dry_run, object_type_name=None):
    if object_type_name is None:
        object_type_name = object_type

    for object_id in object_ids:
        logging.debug("Creating {} (id: '{}')".format(object_type_name, object_id))
        if not dry_run:
            object_attrs = deepcopy(default_object_attrs)
            if "meta" not in object_attrs:
                object_attrs["meta"] = {}
            object_attrs["meta"]["id"] = object_id
            batch_yp_client.create_object(object_type, object_attrs)


def remove_objects_batch(batch_yp_client, object_type, object_ids, dry_run, object_type_name=None):
    if object_type_name is None:
        object_type_name = object_type

    for object_id in object_ids:
        logging.debug("Removing {} (id: '{}')".format(object_type_name, object_id))
        if not dry_run:
            batch_yp_client.remove_object(object_type, object_id)


def create_objects(yp_client, dry_run, podset_count, pod_count_per_podset, id_prefix):
    assert podset_count > 0, "--podset-count must be greater than zero"
    assert pod_count_per_podset >= 0, "--pod-count-per-podset must be not less than zero"

    POD_DISRUPTION_BUDGET_ID_PREFIX = id_prefix + "pdbudget-"
    POD_SET_ID_PREFIX = id_prefix + "podset-"
    POD_ID_PREFIX = id_prefix + "pod-"

    logging.info("Running in '--create' mode. Pod set count: {}, pod count per pod set: {} (total {}) with id prefix '{}'".format(
        podset_count, pod_count_per_podset, podset_count * pod_count_per_podset, id_prefix))

    with create_batch_yp_client(yp_client, batch_size=100) as batch_yp_client:
        pod_set_ids = [POD_SET_ID_PREFIX + str(index + 1) for index in xrange(podset_count)]
        logging.info("Creating {} pod sets with ids from '{}' to '{}'".format(podset_count, pod_set_ids[0], pod_set_ids[-1]))
        create_objects_batch(batch_yp_client, ObjectTypes.POD_SET.value, pod_set_ids, DEFAULT_POD_SET_ATTRS, dry_run, "pod set")

        pod_disruption_budget_ids = [POD_DISRUPTION_BUDGET_ID_PREFIX + str(index + 1) for index in xrange(podset_count)]
        logging.info("Creating {} pod disruption budget objects with ids from '{}' to '{}'".format(
            podset_count, pod_disruption_budget_ids[0], pod_disruption_budget_ids[-1]))
        create_objects_batch(batch_yp_client, ObjectTypes.POD_DISRUPTION_BUDGET.value, pod_disruption_budget_ids,
                             DEFAULT_POD_DISRUPTION_BUDGET_ATTRS, dry_run, "pod disruption budget object")

        logging.info("Creating {} pods with id prefix '{}'".format(podset_count * pod_count_per_podset, id_prefix))
        for pod_set_index in xrange(podset_count):
            pod_attrs = dict(deepcopy(DEFAULT_POD_ATTRS), meta=dict(pod_set_id=pod_set_ids[pod_set_index]))
            pod_ids = [
                POD_ID_PREFIX + "{}-{}".format(pod_set_index + 1, pod_index + 1)
                for pod_index in xrange(pod_count_per_podset)
            ]
            create_objects_batch(batch_yp_client, ObjectTypes.POD.value, pod_ids, pod_attrs, dry_run, "pod")

        logging.info("Attaching pod sets to pod disruption budget objects")
        for pod_set_id, pd_budget_id in zip(pod_set_ids, pod_disruption_budget_ids):
            logging.debug("Attaching pod set (id: '{}') to pod disruption budget object (id: '{}')".format(pod_set_id, pd_budget_id))
            if not dry_run:
                batch_yp_client.update_object(ObjectTypes.POD_SET.value, pod_set_id, set_updates=[dict(
                    path="/spec/pod_disruption_budget_id",
                    value=pd_budget_id,
                )])


def remove_objects(yp_client, dry_run, id_prefix):
    logging.info("Running in mode '--remove'. Removing pod sets (with attached pods) and pod disruption budget objects"
                 " with id prefix '{}'".format(id_prefix))

    def _upper_bound_prefix(prefix):
        return prefix[:-1] + chr(ord(prefix[-1]) + 1)

    filter_by_prefix_query = "[/meta/id]>'{}' AND [/meta/id]<'{}'".format(id_prefix, _upper_bound_prefix(id_prefix))
    selector_meta_id = ["/meta/id"]

    with create_batch_yp_client(yp_client, batch_size=100) as batch_yp_client:
        pod_set_ids = select_objects_batch(yp_client, ObjectTypes.POD_SET.value, filter_by_prefix_query, selector_meta_id)
        logging.info("Removing {} pod sets".format(len(pod_set_ids)))
        remove_objects_batch(batch_yp_client, ObjectTypes.POD_SET.value, pod_set_ids, dry_run, "pod set")

        pod_disruption_budget_ids = select_objects_batch(yp_client, ObjectTypes.POD_DISRUPTION_BUDGET.value, filter_by_prefix_query, selector_meta_id)
        logging.info("Removing {} pod disruption budget objects".format(len(pod_disruption_budget_ids)))
        remove_objects_batch(batch_yp_client, ObjectTypes.POD_DISRUPTION_BUDGET.value, pod_disruption_budget_ids,
                             dry_run, "pod disruption budget object")

        batch_yp_client.flush()

        pod_ids = select_objects_batch(yp_client, ObjectTypes.POD.value, filter_by_prefix_query, selector_meta_id)
        logging.info("Removing {} pods".format(len(pod_ids)))
        remove_objects_batch(batch_yp_client, ObjectTypes.POD.value, pod_ids, dry_run, "pod")


def main():
    global_parser = argparse.ArgumentParser(add_help=False)

    global_parser.add_argument(
        "--address",
        help="Address to run script on",
    )
    global_parser.add_argument(
        "--id-prefix",
        default=DEFAULT_ID_PREFIX,
        help="Prefix for object ids",
    )
    global_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only log actions that would be done, but not do them",
    )
    global_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Increase verbose level (from info to debug)"
    )

    parser = argparse.ArgumentParser(description="Stress test for YP pod disruption budget controller")
    parser.register("action", "parsers", AliasedSubParsersAction)
    subparsers = parser.add_subparsers()
    subparsers.required = True

    create_objects_parser = subparsers.add_parser("create", help="'create' - mode for creating objects.", parents=[global_parser])
    create_objects_parser.set_defaults(func=create_objects)

    create_objects_parser.add_argument(
        "--podset-count",
        type=int,
        required=True,
        help="Pod set count",
    )
    create_objects_parser.add_argument(
        "--pod-count-per-podset",
        type=int,
        required=True,
        help="Pod count per pod set",
    )

    remove_objects_parser = subparsers.add_parser("remove", help="'remove' - mode for removing objects.", parents=[global_parser])
    remove_objects_parser.set_defaults(func=remove_objects)

    parsed_args = parser.parse_args()
    func_args = dict(vars(parsed_args))
    func_args.pop("func")
    func_args.pop("address")
    func_args.pop("verbose")

    address = get_value(parsed_args.address, os.environ.get("YP_ADDRESS"))
    if address is None:
        raise argparse.ArgumentError(None, "argument --address or environment variable YP_ADDRESS must be specified")

    configure_logging(parsed_args.verbose)

    logging.info("Running on cluster '{}'".format(address))
    if parsed_args.dry_run:
        logging.info("Running in mode --dry-run. Actions will not be made,"
                     " script only logs actions that would be made if argument --dry-run will not be specified.")

    with YpClient(address=address, config=dict(token=find_token())) as yp_client:
        parsed_args.func(yp_client, **func_args)


if __name__ == "__main__":
    main()
