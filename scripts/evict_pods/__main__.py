from yp.client import YpClient, find_token
from yp.common import wait, WaitFailed

import yt.yson as yson

import argparse
import logging
import sys


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", "-a", required=True, help="YP address")
    parser.add_argument("--message", "-m", required=True, help="Eviction message")
    return parser.parse_args()


def configure_logger():
    FORMAT = "%(asctime)s\t%(levelname)s\t%(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = [handler]


def is_empty(value):
    return value is None or value == "" or isinstance(value, yson.YsonEntity)


def evict_pod(yp_client, pod_id, message):
    enable_scheduling, old_node_id = yp_client.get_object("pod", pod_id, selectors=[
        "/spec/enable_scheduling",
        "/status/scheduling/node_id",
    ])
    if not enable_scheduling:
        logging.error("Pod '{}' has /spec/enable_scheduling = %false")
        sys.exit(1)
    if is_empty(old_node_id):
        logging.error("Pod '{}' is not assigned to any node".format(pod_id))
        sys.exit(1)

    logging.info("Evicting pod '{}' from node '{}'".format(pod_id, old_node_id))
    yp_client.request_pod_eviction(pod_id, message)

    logging.info("Waiting for eviction to be acknowledged")
    try:
        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/eviction/state"])[0]
                        != "requested")
    except WaitFailed:
        logging.error("Pod eviction is not acknowledged (pod_id: '{}')".format(pod_id))
        sys.exit(1)

    logging.info("Waiting for pod eviction")
    try:
        wait(lambda: yp_client.get_object("pod", pod_id, selectors=["/status/eviction/state"])[0]
                        != "acknowledged")
    except WaitFailed:
        logging.error("Pod is not evicted (pod_id: '{}')".format(pod_id))
        sys.exit(1)

    logging.info("Waiting for pod to be rescheduled")
    try:
        wait(lambda: not is_empty(yp_client.get_object("pod",
                                                       pod_id,
                                                       selectors=["/status/scheduling/node_id"])[0]))
    except WaitFailed:
        error = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/error"])
        logging.error("Pod is not rescheduled (pod_id: '{}', error: {})".format(pod_id, error))
        sys.exit(1)

    new_node_id = yp_client.get_object("pod", pod_id, selectors=["/status/scheduling/node_id"])[0]
    if new_node_id == old_node_id:
        logging.warning("Pod is scheduled to the same node (pod_id: '{}', node_id: '{}')"
                        .format(pod_id, new_node_id))
    else:
        logging.info("Pod '{}' is successfully rescheduled to node '{}'".format(pod_id, new_node_id))


def main(args):
    configure_logger()

    with YpClient(address=args.address, config=dict(token=find_token())) as yp_client:
        for line in sys.stdin:
            pod_id = line.strip()
            evict_pod(yp_client, pod_id, args.message)


if __name__ == "__main__":
    main(parse_arguments())
