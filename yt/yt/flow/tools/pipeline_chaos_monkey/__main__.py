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
    argparser.add_argument(
        "--vanilla-operation",
        type=str,
        required=False,
        default=None,
        help="Id or alias of the vanilla operation running the pipeline; its jobs are aborted instead of killing hosts. "
        "By default, the alias of the vanilla launcher is used if such an operation runs",
    )
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


def kill_hosts(addresses):
    def kill(address):
        try:
            response = requests.get(f"http://{address}/admin/die")
            if response.status_code != 200:
                return f"Failed to kill host (Address: {address}, StatusCode: {response.status_code})"
            return None
        except Exception:
            return traceback.format_exc()

    with ThreadPoolExecutor(max_workers=500) as executor:
        results = executor.map(kill, addresses)

    for address, result in zip(addresses, results):
        if result is not None:
            logger.error("Failed to kill host %s: %s", address, result)


def find_vanilla_operation(args, yt_client):
    """The client of the cluster and the id of the vanilla operation running the pipeline, or None."""
    try:
        manifest = yt_client.get(f"{args.path}/@current_vanilla_operation")
    except yt.YtResponseError as error:
        if not error.is_resolve_error():
            raise
        manifest = None

    cluster = manifest["cluster"] if manifest else args.cluster_name
    if cluster == args.cluster_name:
        operation_client = yt_client
    else:
        config = yt.default_config.get_config_from_env()
        if manifest.get("proxy_role"):
            # The role of the pipeline cluster may not exist on the cluster of the operation.
            config["proxy"]["http_proxy_role"] = manifest["proxy_role"]
        operation_client = yt.YtClient(proxy=cluster, config=config)
    explicit = args.vanilla_operation is not None
    if explicit:
        alias = args.vanilla_operation
    elif manifest:
        alias = manifest["alias"]
    else:
        # Launches that predate the manifest attribute use the default alias of the launcher.
        alias = f"*flow-runner {args.cluster_name}:{args.path}"

    try:
        if alias.startswith("*"):
            operation = operation_client.get_operation(
                operation_alias=alias, include_runtime=True, attributes=["id", "state"]
            )
        else:
            operation = operation_client.get_operation(alias, include_runtime=True, attributes=["id", "state"])
    except Exception as error:
        if explicit:
            raise
        # The attribute outlives its operation, and an alias resolves only while the operation runs.
        logger.warning("Vanilla operation %s is not running (%s); killing hosts instead", alias, error)
        return None
    if operation["state"] in ("completed", "failed", "aborted"):
        if explicit:
            raise RuntimeError(f"Vanilla operation {alias} is {operation['state']}")
        logger.warning("Vanilla operation %s is %s; killing hosts instead", alias, operation["state"])
        return None
    return operation_client, operation["id"]


def list_vanilla_jobs(vanilla_operation, task_name):
    operation_client, operation_id = vanilla_operation
    job_ids = []
    offset = 0
    while True:
        jobs = operation_client.list_jobs(operation_id, job_state="running", offset=offset, limit=1000)["jobs"]
        job_ids += [job["id"] for job in jobs if job.get("task_name") == task_name]
        if len(jobs) < 1000:
            return job_ids
        offset += len(jobs)


def abort_jobs(yt_client, job_ids):
    def abort(job_id):
        try:
            yt_client.abort_job(job_id)
            return None
        except Exception:
            return traceback.format_exc()

    with ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(abort, job_ids)

    for job_id, result in zip(job_ids, results):
        if result is not None:
            logger.error("Failed to abort job %s: %s", job_id, result)


def get_host(address):
    """The host of a `host:port` or `[ipv6]:port` address."""
    host, separator, _ = address.rpartition(":")
    return host if separator else address


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


def kill_workers(args, flow_view, vanilla_operation):
    if vanilla_operation is not None:
        jobs_to_abort = []
        for job_id in list_vanilla_jobs(vanilla_operation, "worker"):
            if random.random() < args.worker_kill_probability:
                logger.info("Aborting worker job %s", job_id)
                jobs_to_abort.append(job_id)
        abort_jobs(vanilla_operation[0], jobs_to_abort)
        if len(jobs_to_abort) > 0:
            logger.info("Finished aborting worker jobs (successfully or not)")
        return

    hosts_to_kill = []
    for worker in flow_view["state"]["workers"]:
        if random.random() < args.worker_kill_probability:
            host = get_host(worker)
            logger.info("Killing host %s", host)
            hosts_to_kill.append(f"{host}:{args.monitoring_port}")
    kill_hosts(hosts_to_kill)
    if len(hosts_to_kill) > 0:
        logger.info("Finished killing hosts (successfully or not)")


def kill_controller_or_lease(args, yt_client, vanilla_operation):
    need_kill_lease = random.random() < args.controller_lease_kill_probability
    need_kill_host = random.random() < args.controller_kill_probability

    if not need_kill_lease and not need_kill_host:
        return
    if need_kill_host and vanilla_operation is not None:
        need_kill_host = False
        job_ids = list_vanilla_jobs(vanilla_operation, "controller")
        if job_ids:
            job_id = random.choice(job_ids)
            logger.info("Aborting controller job %s", job_id)
            abort_jobs(vanilla_operation[0], [job_id])
        else:
            logger.warning("No running controller job to abort")
        if not need_kill_lease:
            return
    try:
        logger.info("Killing controller")
        controller_host = None
        if need_kill_host:
            try:
                announcements = list(yt_client.lookup_rows(f"{args.path}/flow_control", [{"key": "leader_controller"}]))
                if announcements:
                    monitoring_address = announcements[0]["value"]["monitoring_address"]
                    if monitoring_address.startswith("["):
                        host, separator, port = monitoring_address[1:].partition("]:")
                        if separator and ":" not in host:
                            monitoring_address = f"{host}:{port}"
                    controller_host = monitoring_address
            except Exception:
                logger.exception("Failed to read controller announcement; trying legacy host lookup")

        transaction_id = None
        if need_kill_lease or (need_kill_host and not controller_host):
            try:
                controller_locks = yt_client.get(f"{args.path}/leader_controller_lock/@locks")
                if controller_locks:
                    transaction_id = controller_locks[0]["transaction_id"]
                else:
                    logger.warning("Can't find controller lease - no controller lock found")
            except Exception:
                logger.exception("Failed to find controller lease")

        if need_kill_host and not controller_host and transaction_id:
            try:
                host = yt_client.get(f"//sys/transactions/{transaction_id}/@hostname")
                controller_host = f"{host}:{args.monitoring_port}"
            except Exception:
                logger.exception("Failed to find controller host")

        if need_kill_lease and transaction_id:
            logger.info("Killing controller lease %s", transaction_id)
            yt_client.abort_transaction(transaction_id)
            logger.info("Lease %s was successfully killed", transaction_id)
        if need_kill_host and controller_host:
            logger.info("Killing controller host %s", controller_host)
            kill_hosts([controller_host])
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
            # A relaunch replaces the vanilla operation, so it is looked up on every iteration.
            vanilla_operation = find_vanilla_operation(args, yt_client)
            if vanilla_operation is not None:
                logger.info("Pipeline runs in vanilla operation %s", vanilla_operation[1])
            flow_view = get_flow_view_json(args)
            kill_worker_leases(args, flow_view, yt_client)
            kill_workers(args, flow_view, vanilla_operation)
            kill_controller_or_lease(args, yt_client, vanilla_operation)
        except Exception:
            logger.exception("Failed to perform chaos iteration")
        if args.one_shot:
            return
        time.sleep(args.period)


if __name__ == "__main__":
    main()
