#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yp.scripts.library.batch_client import create_batch_yp_client
from yp.scripts.library.scheduler_history import SchedulerObjectsHistory

from yp.client import YpClient, find_token

from yt.wrapper.errors import YtTabletTransactionLockConflict
from yt.wrapper.retries import run_with_retries

import argparse
import logging
import random
import time


def configure_logging():
    logging.basicConfig(format="%(asctime)-15s %(levelname)s %(message)s", level=logging.DEBUG)


def run_with_common_errors_retries(action):
    def backoff_action(*args):
        logging.exception("Exception occurred, backing off")

    run_with_retries(
        action=action,
        retry_count=6,
        backoff=2,
        exceptions=(YtTabletTransactionLockConflict,),
        backoff_action=backoff_action,
    )


def main_impl(yp_client, arguments):
    configure_logging()

    scheduler_objects = SchedulerObjectsHistory.load_from_path(
        arguments.scheduler_objects_file_path
    )

    scheduler_objects.set_pod_sets_node_segment(arguments.node_segment_id)
    scheduler_objects.set_pod_sets_account_id(arguments.account_id)
    scheduler_objects.make_schedulable()
    scheduler_objects.erase_meta_types()

    pod_set_ids = map(lambda pod_set: pod_set["meta"]["id"], scheduler_objects.pod_sets)

    assert arguments.round_count > 0
    for round_index in xrange(arguments.round_count):
        logging.info("Starting %d round", round_index + 1)

        scheduler_objects.generate_meta_ids()

        logging.info("Shuffling YP pods")
        random.shuffle(scheduler_objects.pods)

        with create_batch_yp_client(yp_client, batch_size=100) as batch_yp_client:
            logging.info("Creating pod sets")
            for pod_set in scheduler_objects.pod_sets:
                run_with_common_errors_retries(
                    lambda: batch_yp_client.create_object("pod_set", pod_set)
                )

            logging.info("Creating pods")
            for pod in scheduler_objects.pods:
                run_with_common_errors_retries(lambda: batch_yp_client.create_object("pod", pod))

            run_with_common_errors_retries(lambda: batch_yp_client.flush())

        logging.info("Waiting for YP master to schedule")
        time.sleep(arguments.schedule_time / 1000.0)

        logging.info("Removing pod sets")
        for pod_set_id in pod_set_ids:
            logging.debug("Removing pod set (id: %s)", pod_set_id)
            run_with_common_errors_retries(lambda: yp_client.remove_object("pod_set", pod_set_id))


def main(arguments):
    with YpClient(address=arguments.cluster, config=dict(token=find_token())) as yp_client:
        main_impl(yp_client, arguments)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Stress test YP scheduler")
    parser.add_argument(
        "--cluster", type=str, required=True, help="YP cluster address",
    )
    parser.add_argument(
        "--scheduler-objects-file-path",
        type=str,
        required=True,
        help="Path to the file containing pods and pod sets for one test round",
    )
    parser.add_argument(
        "--node-segment-id",
        type=str,
        required=True,
        help="Set node segment id restriction for all pod sets in scheduler objects file",
    )
    parser.add_argument(
        "--account-id",
        type=str,
        required=True,
        help="Set account id restriction for all pod sets in scheduler objects file",
    )
    parser.add_argument(
        "--round-count", type=int, required=True, help="Count of test rounds",
    )
    parser.add_argument(
        "--schedule-time",
        type=int,
        required=True,
        help="Time (in milliseconds) to wait for YP master to schedule pods from one round",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
