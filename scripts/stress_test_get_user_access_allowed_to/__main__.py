#!/usr/bin/env python
# -*- coding: utf-8 -*-

from yp.scripts.library.batch_client import create_batch_yp_client

from yp.client import YpClient, generate_uuid

import argparse
import json
import logging
import time


# !!! ATTENTION !!! This script is intended to work on local and experimental cluster.
#                   Particularly, it does not remove created objects after testing.


def configure_logging():
    logging.basicConfig(format="%(asctime)-15s %(levelname)s %(message)s", level=logging.DEBUG)


def main_impl(yp_client, arguments):
    configure_logging()

    TESTING_USER_ID = "test-user-" + generate_uuid()

    logging.info("Creating user")
    yp_client.create_object("user", attributes=dict(meta=dict(id=TESTING_USER_ID)))

    logging.info("Creating %d objects", arguments.object_count)
    object_ids = []
    with create_batch_yp_client(yp_client, batch_size=200) as batch_yp_client:
        for object_index in xrange(arguments.object_count):
            object_id = "test-network-project-" + generate_uuid()
            batch_yp_client.create_object(
                "network_project",
                attributes=dict(
                    meta=dict(
                        id=object_id,
                        acl=[
                            dict(action="allow", subjects=[TESTING_USER_ID], permissions=["read"],),
                        ],
                        inherit_acl=False,
                    ),
                    spec=dict(project_id=object_index),
                ),
            )
            object_ids.append(object_id)
        batch_yp_client.flush()

    logging.info("Wait for YP access control state")
    time.sleep(5)

    for request_index in xrange(arguments.request_count):
        logging.info("Request #%d", request_index + 1)

        request_start_time = time.time()
        response = yp_client.get_user_access_allowed_to(
            [dict(user_id=TESTING_USER_ID, object_type="network_project", permission="read",)]
        )
        request_elapsed_time = time.time() - request_start_time
        logging.info("Request took %f seconds", request_elapsed_time)

        try:
            assert len(response) == 1
            actual_object_ids = response[0]["object_ids"]
            actual_object_ids = set(actual_object_ids)
            for expect_object_id in object_ids:
                assert expect_object_id in actual_object_ids
        except AssertionError:
            logging.error("Incorrect response:\n%s\n", json.dumps(response, indent=4))
            raise


def main(arguments):
    with YpClient(address=arguments.cluster, config=dict(enable_ssl=False)) as yp_client:
        main_impl(yp_client, arguments)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Stress test YP get-user-access-allowed-to Api method"
    )
    parser.add_argument(
        "--cluster", type=str, required=True, help="YP cluster address",
    )
    parser.add_argument(
        "--object-count", type=int, default=20 * 1000, help="Object count in the response",
    )
    parser.add_argument(
        "--request-count", type=int, default=10, help="Request count",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
