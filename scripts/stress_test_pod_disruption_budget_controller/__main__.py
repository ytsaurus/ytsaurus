#!/usr/bin/env python

from yp.scripts.library.batch_client import create_batch_yp_client
from yp.scripts.library.cluster_snapshot import batch_select

from yp.client import YpClient, find_token
from yp.cli_helpers import AliasedSubParsersAction

from yt.common import get_value

from copy import deepcopy
import argparse
import logging
import os

DEFAULT_ID_PREFIX = "stress-test-pdb-controller-"

DEFAULT_POD_DISRUPTION_BUDGET_ATTRS = dict(spec=dict(max_pods_unavailable=2, max_pod_disruptions_between_syncs=1))
DEFAULT_POD_SET_ATTRS = dict(spec=dict(node_segment_id="default"))


def configure_logging(verbose):
    logging.basicConfig(
        format="%(asctime)-15s %(levelname)s %(message)s",
        level=logging.DEBUG if verbose else logging.INFO,
    )


def _get_filter(id_prefix):
    def _upper_bound_prefix(prefix):
        return prefix[:-1] + chr(ord(prefix[-1]) + 1)

    return "[/meta/id]>'{}' AND [/meta/id]<'{}'".format(id_prefix, _upper_bound_prefix(id_prefix))


def _select_object_ids_batch(yp_client, object_type, id_prefix):
    filter_query = _get_filter(id_prefix)
    return [response[0] for response in batch_select(yp_client, object_type, filter_query, ["/meta/id"])]


def _create_objects(yp_client, object_type, default_object_attrs, object_count, id_prefix, dry_run):
    object_id_prefix = "{}{}-".format(id_prefix, object_type)

    created_object_ids = set(_select_object_ids_batch(yp_client, object_type, id_prefix))
    all_object_ids = [object_id_prefix + str(index + 1) for index in xrange(object_count)]
    object_ids_to_create = [pod_set_id for pod_set_id in all_object_ids if pod_set_id not in created_object_ids]

    logging.info("Creating {} {}s with id prefix '{}'".format(len(object_ids_to_create), object_type, id_prefix))
    for object_id in object_ids_to_create:
        logging.debug("Creating {} (id: '{}')".format(object_type, object_id))
        if not dry_run:
            object_attrs = deepcopy(default_object_attrs)
            if "meta" not in object_attrs:
                object_attrs["meta"] = {}
            object_attrs["meta"]["id"] = object_id
            yp_client.create_object(object_type, object_attrs)

    return all_object_ids


def _remove_objects(yp_client, object_type, id_prefix, dry_run):
    object_ids = _select_object_ids_batch(yp_client, object_type, id_prefix)

    logging.info("Removing {} {}s".format(len(object_ids), object_type))
    for object_id in object_ids:
        logging.debug("Removing {} (id: '{}')".format(object_type, object_id))
        if not dry_run:
            yp_client.remove_object(object_type, object_id)


def _get_pod_set_ids_for_update_indices(yp_client, podset_count, id_prefix):
    filter_by_prefix_query = _get_filter(id_prefix)
    created_pod_sets = batch_select(yp_client, "pod_set", filter_by_prefix_query, ["/meta/id", "/spec/pod_disruption_budget_id"])

    pod_set_ids_for_update_indices_set = set(range(podset_count))
    for index in xrange(len(created_pod_sets)):
        if created_pod_sets[index][1]:
            created_object_id = int(created_pod_sets[index][0].split("-")[-1]) - 1
            if created_object_id in pod_set_ids_for_update_indices_set:
                pod_set_ids_for_update_indices_set.remove(created_object_id)

    return list(sorted(pod_set_ids_for_update_indices_set))


def _update_pod_sets(batch_yp_client, dry_run, podset_count, id_prefix):
    pod_set_ids_for_update_indices = _get_pod_set_ids_for_update_indices(batch_yp_client, podset_count, id_prefix)

    pod_set_id_prefix = "{}{}-".format(id_prefix, "pod_set")
    pod_disruption_budget_id_prefix = "{}{}-".format(id_prefix, "pod_disruption_budget")

    pod_set_ids = [pod_set_id_prefix + str(index + 1) for index in pod_set_ids_for_update_indices]
    pod_disruption_budget_id_prefix = [pod_disruption_budget_id_prefix + str(index + 1) for index in pod_set_ids_for_update_indices]

    logging.info("Attaching {} pod sets to pod disruption budgets".format(len(pod_set_ids_for_update_indices)))
    for pod_set_id, pod_disruption_budget_id in zip(pod_set_ids, pod_disruption_budget_id_prefix):
        logging.debug("Attaching pod set (id: '{}') to pod disruption budget (id: '{}')".format(pod_set_id, pod_disruption_budget_id))
        if not dry_run:
            batch_yp_client.update_object("pod_set", pod_set_id, set_updates=[dict(
                path="/spec/pod_disruption_budget_id",
                value=pod_disruption_budget_id,
            )])


def create_objects(yp_client, dry_run, podset_count, id_prefix):
    assert podset_count > 0, "--podset-count must be greater than zero"

    logging.info("Running in 'create' mode. Pod set and pod disruption budget count: {}, id prefix: '{}'".format(podset_count, id_prefix))

    with create_batch_yp_client(yp_client, batch_size=100, retries_count=5) as batch_yp_client:
        _create_objects(batch_yp_client, "pod_set", DEFAULT_POD_SET_ATTRS, podset_count, id_prefix, dry_run)

        _create_objects(batch_yp_client, "pod_disruption_budget", DEFAULT_POD_DISRUPTION_BUDGET_ATTRS, podset_count, id_prefix, dry_run)

        batch_yp_client.flush()

        _update_pod_sets(batch_yp_client, dry_run, podset_count, id_prefix)


def remove_objects(yp_client, dry_run, id_prefix):
    logging.info("Running in 'remove' mode. Removing pod sets and pod disruption budgets with id prefix '{}'".format(id_prefix))

    with create_batch_yp_client(yp_client, batch_size=100, retries_count=5) as batch_yp_client:
        _remove_objects(batch_yp_client, "pod_set", id_prefix, dry_run)
        _remove_objects(batch_yp_client, "pod_disruption_budget", id_prefix, dry_run)


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
