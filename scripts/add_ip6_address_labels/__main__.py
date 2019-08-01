from __future__ import print_function

from yp.client import YpClient, find_token
from yp.logger import logger

from yt.wrapper.errors import YtTabletTransactionLockConflict
from yt.wrapper.retries import run_with_retries
from yt.yson.yson_types import YsonEntity
from yt.yson import dumps
import yt.wrapper as yt

from copy import deepcopy
import argparse
import sys
import time


BACKBONE = "backbone"
UNISTAT_KEY = "unistat"
UNISTAT_VALUE = "enabled"


def find_pods_impl(arguments, yp_client):
    def filter_by_vlan_id(objects, vlan_id):
        return filter(lambda obj: obj["vlan_id"] == vlan_id, objects)
    def has_label(obj):
        return obj.get("labels", {}).get(UNISTAT_KEY, None) == UNISTAT_VALUE
    timestamp = yp_client.generate_timestamp()
    offset = 0
    while True:
        response = yp_client.select_objects(
            "pod",
            selectors=[
                "/meta/id",
                "/meta/pod_set_id",
                "/spec/ip6_address_requests",
                "/status/ip6_address_allocations",
            ],
            filter="[/labels/deploy_engine] = \"YP_LITE\"",
            offset=offset,
            timestamp=timestamp,
            limit=arguments.batch_size,
        )
        for pod_id, pod_set_id, requests, allocations in response:
            required_pod = False

            backbone_requests = filter_by_vlan_id(requests, BACKBONE)
            assert len(backbone_requests) == 1
            if not has_label(backbone_requests[0]):
                required_pod = True

            if not isinstance(allocations, YsonEntity):
                backbone_allocations = filter_by_vlan_id(allocations, BACKBONE)
                assert len(backbone_allocations) <= 1
                if len(backbone_allocations) > 0 and not has_label(backbone_allocations[0]):
                    required_pod = True

            if required_pod:
                assert " " not in pod_id
                assert " " not in pod_set_id
                print(pod_id, pod_set_id)
        offset += len(response)
        if len(response) < arguments.batch_size:
            break


def find_pods(arguments):
    with YpClient(arguments.cluster, config=dict(token=find_token())) as yp_client:
        find_pods_impl(arguments, yp_client)


def isna(value):
    return value is None or isinstance(value, YsonEntity)


def add_label_to_objects(objects):
    for obj in objects:
        if obj["vlan_id"] != BACKBONE:
            continue
        if "labels" not in obj or isna(obj["labels"]):
            obj["labels"] = dict()
        labels = obj["labels"]
        if UNISTAT_KEY in labels:
            assert labels[UNISTAT_KEY] == UNISTAT_VALUE
        else:
            labels[UNISTAT_KEY] = UNISTAT_VALUE


def add_one_label_impl(yt_client, pods_table_path, pod_id, pod_set_id, dry_run, spec_field_name, status_field_name):
    logger.info("Adding to pod: (pod id: %s, pod set id: %s)", pod_id, pod_set_id)

    key = {"meta.id": pod_id, "meta.pod_set_id": pod_set_id}

    rows = list(yt_client.lookup_rows(pods_table_path, [key]))
    assert len(rows) <= 1
    if len(rows) == 0:
        logger.warning("Could not find pod: (pod id: %s, pod set id: %s)", pod_id, pod_set_id)
        return

    row = rows[0]
    assert row["labels"]["deploy_engine"] == "YP_LITE"

    new_row = deepcopy(key)
    if spec_field_name in row and not isna(row[spec_field_name]):
        spec = row[spec_field_name]
        requests = spec.get("ip6_address_requests", [])
        if not isna(requests):
            if dry_run:
                logger.info("Before requests:\n%s", dumps(requests, yson_format="pretty"))
            add_label_to_objects(requests)
            new_row[spec_field_name] = spec

    if status_field_name in row and not isna(row[status_field_name]):
        status = row[status_field_name]
        allocations = status.get("ip6_address_allocations", [])
        if not isna(allocations):
            if dry_run:
                logger.info("Before allocations:\n%s", dumps(allocations, yson_format="pretty"))
            add_label_to_objects(allocations)
            new_row[status_field_name] = status

    update_tag_field_name = "spec.update_tag"
    assert update_tag_field_name in row
    new_row[update_tag_field_name] = row[update_tag_field_name]

    if dry_run:
        logger.info("After:\n%s", dumps(new_row, yson_format="pretty"))
    else:
        yt_client.insert_rows(pods_table_path, [new_row], update=True)


def combine_pods_by_pod_set_id(pods):
    result = dict()
    for pod_id, pod_set_id in pods:
        if pod_set_id not in result:
            result[pod_set_id] = []
        result[pod_set_id].append(pod_id)
    return result


def add_label(arguments):
    def read_pods():
        result = []
        for line in sys.stdin.readlines():
            pod_id, pod_set_id = line.strip().split(" ")
            result.append((pod_id, pod_set_id))
        return result
    def do_sleep(sleep_time):
        if arguments.no_delay:
            return
        logger.info("Sleeping for %f seconds", sleep_time)
        time.sleep(sleep_time)
    yt_client = yt.YtClient(arguments.cluster, config=dict(backend="rpc"))
    pods = read_pods()
    pods_table_path = yt.ypath_join(arguments.yp_path, "db", "pods")
    combined_pods = combine_pods_by_pod_set_id(pods)
    for pod_set_id in combined_pods:
        logger.info("Processing pod set %s", pod_set_id)
        pod_ids = combined_pods[pod_set_id]
        for pod_id in pod_ids:
            def invoke_with_transaction(function, yt_client, *args, **kwargs):
                with yt_client.Transaction(type="tablet"):
                    function(yt_client, *args, **kwargs)
            run_with_retries(
                lambda: invoke_with_transaction(
                    add_one_label_impl,
                    yt_client,
                    pods_table_path,
                    pod_id,
                    pod_set_id,
                    arguments.dry_run,
                    arguments.spec_field_name,
                    arguments.status_field_name,
                ),
                exceptions=(YtTabletTransactionLockConflict,),
            )
            do_sleep(0.1)
        do_sleep(5)


def main():
    parser = argparse.ArgumentParser(description="Implements migration for YP-1117")
    subparsers = parser.add_subparsers()

    find_pods_subparser = subparsers.add_parser("find-pods")
    find_pods_subparser.add_argument(
        "--cluster",
        type=str,
        required=True,
        help="YP cluster address",
    )
    find_pods_subparser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
    )
    find_pods_subparser.set_defaults(func=find_pods)

    add_label_subparser = subparsers.add_parser("add-label")
    add_label_subparser.add_argument(
        "--cluster",
        type=str,
        required=True,
        help="YT cluster address",
    )
    add_label_subparser.add_argument(
        "--yp-path",
        type=str,
        required=True,
        help="Path to yp Cypress node",
    )
    add_label_subparser.add_argument(
        "--spec-field-name",
        type=str,
        choices=["spec.etc", "spec.other"],
        required=True,
    )
    add_label_subparser.add_argument(
        "--status-field-name",
        type=str,
        choices=["status.etc", "status.other"],
        required=True,
    )
    add_label_subparser.add_argument(
        "--no-delay",
        action="store_true",
        default=False,
    )
    add_label_subparser.add_argument("--dry-run", action="store_true", default=False)
    add_label_subparser.set_defaults(func=add_label)

    arguments = parser.parse_args()
    arguments.func(arguments)


if __name__ == "__main__":
    main()
