from yp.client import YpClient, find_token
from yp.common import YpNoSuchObjectError

import yt.yson as yson

import argparse
import logging
import sys
import time


BUSY_WAIT_DELAY = 5


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
    enable_scheduling, old_node_id, pod_set_id = yp_client.get_object("pod", pod_id, selectors=[
        "/spec/enable_scheduling",
        "/status/scheduling/node_id",
        "/meta/pod_set_id",
    ])
    if not enable_scheduling:
        logging.error("Pod '{}' has /spec/enable_scheduling = %false")
        sys.exit(1)
    if is_empty(old_node_id):
        logging.error("Pod '{}' is not assigned to any node".format(pod_id))
        sys.exit(1)

    old_pod_set_size = len(yp_client.select_objects("pod", selectors=["/meta/id"],
                                                    filter='[/meta/pod_set_id] = "{}"'.format(pod_set_id)))

    logging.info("Evicting pod '{}' from node '{}'".format(pod_id, old_node_id))
    yp_client.request_pod_eviction(pod_id, message)

    while True:
        time.sleep(BUSY_WAIT_DELAY)
        try:
            eviction_state, scheduling_status = yp_client.get_object("pod", pod_id, selectors=[
                "/status/eviction/state",
                "/status/scheduling",
            ])
        except YpNoSuchObjectError:
            all_pods = yp_client.select_objects("pod", selectors=["/meta/id", "/status/scheduling"],
                                                filter='[/meta/pod_set_id] = "{}"'.format(pod_set_id))
            if old_pod_set_size < len(all_pods):
                logging.info("Pod is gone, waiting for pod set to be populated "
                             "(pod_id: '{}', pod_set_id: '{}')".format(pod_id, pod_set_id))
                continue
            unscheduled_pod_ids = []
            for sibling_pod_id, scheduling_status in all_pods:
                if is_empty(scheduling_status["node_id"]):
                    unscheduled_pod_ids.append(sibling_pod_id)
            if unscheduled_pod_ids:
                logging.info("Waiting for pod(s) to be scheduled: '{}'"
                             "(original pod_id: '{}', pod_set_id: '{}'"
                             .format("', '".join(unscheduled_pod_ids), pod_id, pod_set_id))
                continue
            else:
                logging.info("Pod set '{}' has all pods scheduled (original pod_id: '{}')"
                             .format(pod_set_id, pod_id))
                break
        else:
            if eviction_state != "none":
                logging.info("Waiting for pod eviction (state: '{}', pod_id: '{}')"
                             .format(eviction_state, pod_id))
                continue
            node_id = scheduling_status.get("node_id")
            if is_empty(node_id):
                logging.info("Waiting for pod to be scheduled (message: '{}', error: {})"
                             .format(scheduling_status.get("message"), scheduling_status.get("error")))
                continue
            if node_id == old_node_id:
                logging.warning("Pod is scheduled to the same node (pod_id: '{}', node_id: '{}')"
                                .format(pod_id, node_id))
            else:
                logging.info("Pod '{}' is successfully rescheduled to node '{}'"
                             .format(pod_id, node_id))
            break


def main(args):
    configure_logger()

    with YpClient(address=args.address, config=dict(token=find_token())) as yp_client:
        for line in sys.stdin:
            pod_id = line.strip()
            evict_pod(yp_client, pod_id, args.message)


if __name__ == "__main__":
    main(parse_arguments())
