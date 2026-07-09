import argparse
import logging
import os
import signal
import random
import requests
import time
import traceback
import yt.wrapper as yt

from yt.yt.flow.library.python.client.flow_view import get_flow_view

from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("chaos_monkey")


def setup_logging():
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def parse_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("pipeline", type=str, help="`cluster://path` address of pipeline")
    argparser.add_argument("--period", type=float, required=False, default=60.0, help="Period in seconds")
    argparser.add_argument("--worker-lease-kill-probability", type=float, required=False, default=0.0)
    argparser.add_argument("--worker-kill-probability", type=float, required=False, default=0.0)
    argparser.add_argument("--controller-lease-kill-probability", type=float, required=False, default=0.0)
    argparser.add_argument("--controller-kill-probability", type=float, required=False, default=0.0)
    argparser.add_argument(
        "--monitoring-port", type=int, required=False, default=80, help="Port with monitoring and admin handlers"
    )
    argparser.add_argument("--one-shot", action="store_true", help="Make chaos one time and finish")
    args = argparser.parse_args()
    assert "://" in args.pipeline
    args.cluster_name, args.path = args.pipeline.split("://")
    args.path = "//" + args.path
    return args


def get_yt_client(args):
    return yt.YtClient(proxy=args.cluster_name, config=yt.default_config.get_config_from_env())


def get_flow_view_json(args):
    flow_view = get_flow_view(get_yt_client(args), args.path)
    return yt.yson.convert.yson_to_json(flow_view)


def kill_leases(yt_client, leases):
    def kill(lease_id):
        try:
            yt_client.abort_transaction(lease_id)
            return None
        except Exception:
            return traceback.format_exc()

    with ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(kill, leases)

    for lease_id, result in zip(leases, results):
        if result is not None:
            logging.error("Failed to kill lease %s: %s", lease_id, result)


def kill_hosts(args, hosts):
    def kill(host):
        try:
            response = requests.get(f"http://{host}:{args.monitoring_port}/admin/die")
            if response.status_code != 200:
                return f"Failed to kill host (Host: {host}, StatusCode: {response.status_code})"
            return None
        except Exception:
            return traceback.format_exc()

    with ThreadPoolExecutor(max_workers=500) as executor:
        results = executor.map(kill, hosts)

    for host, result in zip(hosts, results):
        if result is not None:
            logger.error("Failed to kill host %s: %s", host, result)


def kill_worker_leases(args, flow_view, yt_client):
    layout = flow_view["state"]["execution_spec"]["layout"]
    partitions = layout["partitions"]
    jobs = layout["jobs"]

    leases_to_kill = []
    for partition in partitions.values():
        if partition["state"] != "executing":
            continue
        job_id = partition["current_job_id"]
        job = jobs[job_id]
        lease_id = job["lease_id"]
        if random.random() < args.worker_lease_kill_probability:
            logger.info("Killing lease (LeaseId: %s, ComputationId: %s)", lease_id, partition["computation_id"])
            leases_to_kill.append(lease_id)

    kill_leases(yt_client, leases_to_kill)
    if len(leases_to_kill) > 0:
        logger.info("Finished killing leases (successfully or not)")


def kill_workers(args, flow_view):
    hosts_to_kill = []
    for worker in flow_view["state"]["workers"]:
        if random.random() < args.worker_kill_probability:
            host = worker.split(":")[0]
            logger.info("Killing host %s", host)
            hosts_to_kill.append(host)
    kill_hosts(args, hosts_to_kill)
    if len(hosts_to_kill) > 0:
        logger.info("Finished killing hosts (successfully or not)")


def kill_controller_or_lease(args, yt_client):
    need_kill_lease = random.random() < args.controller_lease_kill_probability
    need_kill_host = random.random() < args.controller_kill_probability

    if not need_kill_lease and not need_kill_host:
        return
    try:
        logger.info("Killing controller")
        controller_locks = yt_client.get(f"{args.path}/leader_controller_lock/@locks")
        if len(controller_locks) == 0:
            logger.warning("Can't kill controller - no controller found")
            return
        transaction_id = controller_locks[0]["transaction_id"]
        controller_host = yt_client.get(f"//sys/transactions/{transaction_id}/@hostname")
        if need_kill_lease:
            logger.info("Killing controller lease %s of controller %s", transaction_id, controller_host)
            yt_client.abort_transaction(transaction_id)
            logger.info("Lease %s was successfully killed", transaction_id)
        if need_kill_host:
            logger.info("Killing controller host %s", controller_host)
            kill_hosts(args, [controller_host])
            logger.info("Finished killing controller host (successfully or not)")
    except Exception:
        logger.exception("Failed to kill controller")


def main():
    signal.signal(signal.SIGINT, lambda signal, frame: os._exit(0))  # Exit silently on keyboard interrupt.
    setup_logging()

    args = parse_args()

    logger.info("Run chaos monkey tool with arguments %s", args)

    yt_client = get_yt_client(args)

    while True:
        try:
            flow_view = get_flow_view_json(args)
            kill_worker_leases(args, flow_view, yt_client)
            kill_workers(args, flow_view)
            kill_controller_or_lease(args, yt_client)
        except Exception:
            logger.exception("Failed to perform chaos iteration")
        if args.one_shot:
            return
        time.sleep(args.period)


if __name__ == "__main__":
    main()
