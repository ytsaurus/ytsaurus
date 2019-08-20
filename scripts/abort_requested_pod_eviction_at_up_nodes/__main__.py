#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yp.local import DbManager, backup_yp, get_db_version

from yt.wrapper import YtClient

from yt.yson.yson_types import YsonUint64

import yt.logger

from yt.packages.six import iteritems, itervalues

import argparse
import collections
import time


PodDescription = collections.namedtuple(
    "PodDescription",
    [
        "id",
        "eviction_state",
        "node_id",
    ],
)


def format_pod_description(pod_description, node_id_to_hfsm_state):
    return "pod_id: {}, pod_eviction_state: {}, node_id: {}, node_hfsm_state: {}".format(
        pod_description.id,
        pod_description.eviction_state,
        pod_description.node_id,
        node_id_to_hfsm_state.get(pod_description.node_id, None),
    )


def main(arguments):
    if arguments.dry_run:
        yt.logger.info("Dry run mode enabled")

    yt_client = YtClient(arguments.yt_proxy)

    db_version = get_db_version(yt_client, arguments.yp_path)
    db_manager = DbManager(yt_client, arguments.yp_path, db_version)

    backup_yp(yt_client, arguments.yp_path)

    db_manager.unmount_tables()

    pods_with_requested_eviction = dict()
    for row in db_manager.read_table("pods"):
        pod_id = row["meta.id"]

        eviction_state = row["status.etc"]["eviction"]["state"]
        if eviction_state != "requested":
            continue

        node_id = row["status.etc"]["scheduling"].get("node_id", None)

        assert pod_id not in pods_with_requested_eviction
        pods_with_requested_eviction[pod_id] = PodDescription(
            id=pod_id,
            eviction_state=eviction_state,
            node_id=node_id,
        )

    used_node_ids = set()
    for pod_description in itervalues(pods_with_requested_eviction):
        if pod_description.node_id is not None:
            used_node_ids.add(pod_description.node_id)

    node_id_to_hfsm_state = dict()
    for row in db_manager.read_table("nodes"):
        node_id = row["meta.id"]
        hfsm_state = row["status.etc"]["hfsm"]["state"]
        if node_id not in used_node_ids:
            continue
        assert node_id not in node_id_to_hfsm_state
        node_id_to_hfsm_state[node_id] = hfsm_state

    pod_ids_to_abort_eviction = set()
    yt.logger.info("Listing pods")
    for pod_id, pod_description in iteritems(pods_with_requested_eviction):
        should_abort = (pod_description.node_id is not None) and \
            (node_id_to_hfsm_state.get(pod_description.node_id, None) == "up")
        yt.logger.info(
            "Pod (%s, should_abort: %s)",
            format_pod_description(pod_description, node_id_to_hfsm_state),
            should_abort,
        )
        if should_abort:
            pod_ids_to_abort_eviction.add(pod_id)

    def abort_pod_eviction(row):
        pod_id = row["meta.id"]
        if pod_id in pod_ids_to_abort_eviction:
            node_id = pods_with_requested_eviction[pod_id].node_id
            row["status.etc"]["eviction"] = dict(
                last_updated=YsonUint64(int(time.time() * (10 ** 6))),
                message="Eviction aborted due to node \"{}\" being in \"up\" state".format(
                    node_id,
                ),
                reason="none",
                state="none",
            )
        yield row

    if not arguments.dry_run:
        db_manager.run_map("pods", abort_pod_eviction)

    db_manager.mount_unmounted_tables()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Abort requested pod eviction at up nodes")
    parser.add_argument(
        "--yt-proxy",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--yp-path",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
