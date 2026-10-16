# flake8: noqa
import yt.wrapper as yt_
from yt.wrapper import YtError
import argparse
import threading
import logging
import random
import string
import sys
import copy
from time import sleep
from .log import get_logger
from .runner import MultiRunner, ThreadWrapper
from .common import create_client, random_string, ValidationError, make_client_factory
from .log import TraceContext
from .inserter import Inserter
from .selecter import Selecter
from .smooth_move import SmoothMovementRunner, SmoothMovementHelper
from .create import create_sorted_table, create_ordered_table
from .setup import setup_clusters, setup_cells, setup_sys_clusters, setup_chaos_cluster, setup_long_cache_at_rpc_proxies
from .disturbance import Disturber, freeze_unfreeze, unmount_mount, build_cell_snapshot, restart_cell, \
    alter_replica
from .create_chaos import create_chaos_table_with_replicas
from .create_replicated import create_replicated_table_with_replicas

def get_data_proxy(args):
    if args.mode == "chaos":
        return args.proxy
    if args.replica_proxy:
        return args.replica_proxy
    return None

def parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--start-over', action='store_true')
    parser.add_argument('--proxy')
    parser.add_argument('--replica-proxy')
    parser.add_argument('--ordered', action='store_true')
    parser.add_argument("--mode", choices=["default", "replicated", "chaos"], default="default")
    parser.add_argument("--no-setup-cluster", action="store_false", dest="setup_cluster")

    args = parser.parse_args()

    if args.ordered and replica_proxy:
        raise ValueError("--ordered and --replica-proxy are mutually exclusive")

    if args.ordered and args.mode == "chaos":
        raise ValueError("--ordered and --chaos are mutually exclusive")

    if (args.mode == "replicated") != (args.replica_proxy is not None):
        raise ValueError("--replica-proxy must be present only for replicated mode")

    return args


def init_globals(proxy):
    # Initialize abc stuff in the main thread.
    try:
        SmoothMovementHelper("asrt")
    except Exception:
        pass

    # Initialize rpc driver in the main thread.
    create_client("rpc", proxy=proxy).list("/")



def main():
    args = parse_args()

    proxy = args.proxy
    replica_proxy = args.replica_proxy
    data_proxy = get_data_proxy(args)

    init_globals(proxy)

    client = create_client(proxy=proxy)
    cluster_name = client.get("//sys/@cluster_connection/cluster_name")

    if args.mode == "replicated":
        replica_client = create_client(proxy=replica_proxy)
        replica_cluster_name = replica_client.get("//sys/@cluster_connection/cluster_name")

    if args.start_over:
        if args.setup_cluster:
            setup_clusters(args)
    else:
        if len(client.get("//sys/tablet_cells")) == 0:
            logger.error("You probably want to start with --start-over flag")
            exit(1)


    tables = [
        "//tmp/t1",
        "//tmp/t2",
    ]

    schema_attributes = {
        "with_hash": True,
        "with_hunks": False,
    }
    reshard_args = {
        "tablet_count": 10,
        "uniform": not args.ordered,
    }

    if args.start_over:
        if args.mode == "default":
            create_table = create_ordered_table if args.ordered else create_sorted_table
            for table in tables:
                create_table(
                    table, schema_attributes=schema_attributes,
                    ignore_existing=True, mount=True, reshard_args=reshard_args,
                    force=True, client=client)
        elif args.mode == "replicated":
            replica_client = create_client(proxy=replica_proxy)
            for table in tables:
                create_replicated_table_with_replicas(
                    client, replica_client, replica_cluster_name, table)
        elif args.mode == "chaos":
            client = create_client(proxy=proxy)
            for table in tables:
                create_chaos_table_with_replicas(client, cluster_name, table)
        else:
            assert False, f"Invalid mode {args.mode}"

    runner = MultiRunner()
    replica_paths = [f"{t}_{mode}" for t in tables for mode in ("sync", "async")]
    queue_replica_paths = [f"{t}_queue_{mode}" for t in tables for mode in ("sync", "async")]

    for i, table in enumerate(tables):
        inserter = Inserter(table, period=0, client_factory=make_client_factory(backend="http", proxy=proxy))
        inserter.initialize()

        # 5 rows per insertion.
        runner.start_thread(inserter, 5, name=f"Ins{i}")

        # Select from the main table.
        for j in range(3):
            runner.start_thread(
                Selecter(inserter, period=0, client_factory=make_client_factory(backend="rpc", proxy=proxy)),
                name=f"Sel{i}/{j}")

        # Select from replicas.
        if args.mode == "replicated":
            runner.start_thread(
                Selecter(
                    inserter, period=0, table_path=f"{table}_sync",
                    client_factory=make_client_factory(backend="rpc", proxy=replica_proxy)),
                name=f"SelSync{i}")
            runner.start_thread(
                Selecter(
                    inserter, period=0, table_path=f"{table}_async", replica_consistency=None,
                    client_factory=make_client_factory(backend="rpc", proxy=replica_proxy)),
                name=f"SelAsync{i}")

    SMOOTH_MOVEMENT_PERIOD = 2

    for j in range(8):
        if args.mode == "default":
            runner.start_thread(SmoothMovementRunner(
                tables, period=SMOOTH_MOVEMENT_PERIOD, client=client),
                name=f"Mov{i}")
        elif args.mode == "replicated":
            runner.start_thread(SmoothMovementRunner(
                replica_paths, period=SMOOTH_MOVEMENT_PERIOD, client=create_client(proxy=replica_proxy)),
                name=f"MovReplica.{j}")
        elif args.mode == "chaos":
            paths = replica_paths if j % 2 == 0 else queue_replica_paths
            runner.start_thread(SmoothMovementRunner(
                paths, period=SMOOTH_MOVEMENT_PERIOD, client=client),
                name=f"MovReplica.{j}")
        else:
            assert False, f"Invalid mode {args.mode}"

    disturbances = []

    disturbances.append(build_cell_snapshot(proxy=proxy, period=20))
    disturbances.append(restart_cell(proxy=proxy, period=120))
    if args.mode == "replicated":
        disturbances.append(build_cell_snapshot(proxy=args.replica_proxy, period=20))
        disturbances.append(restart_cell(proxy=args.replica_proxy, period=120))

    if args.mode in ("default", "replicated"):
        for path in tables:
            disturbances.append(unmount_mount(path, period=150, proxy=args.proxy))
            disturbances.append(freeze_unfreeze(path, period=200, proxy=args.proxy))
    if args.mode == "chaos":
        for path in replica_paths + queue_replica_paths:
            disturbances.append(unmount_mount(path, period=150, proxy=args.proxy))
            disturbances.append(freeze_unfreeze(path, period=200, proxy=args.proxy))
    if args.mode == "replicated":
        for path in replica_paths:
            disturbances.append(unmount_mount(path, period=150, proxy=args.replica_proxy))
            disturbances.append(freeze_unfreeze(path, period=200, proxy=args.replica_proxy))

    for i in range(2):
        runner.start_thread(Disturber(disturbances), name=f"Dis{i}")

    runner.wait(1000)

if __name__ == "__main__":
    logger = get_logger()
    #  console_handler.setFormatter(
    #  print(logger.handlers)
    #  exit(0)

    main()



