#!/usr/bin/env python

from yt.transfer_manager.client import TransferManager
from yt.wrapper.client import Yt
import yt.wrapper as yt

import sys
import argparse
import Queue
from threading import Thread, Semaphore
import time

SRC_TEST_TABLE_NAME = "//tmp/tm_load_tester_table"
DST_TEST_TABLES_PATH = "//tmp/tm_load_tester"

SRC_YT_CLUSTER = "plato"
DST_YT_CLUSTER = "aristotle"

def waiting_thread(client, token, queue, semaphore):
    dst_client = Yt(proxy=DST_YT_CLUSTER, token=token)

    running_tasks = {}
    while True:
        tasks_to_remove = []
        for task, destination in running_tasks.iteritems():
            state = client.get_task_info(task)["state"]
            if state in ["aborted", "failed", "completed"]:
                print >>sys.stderr, "Task {0} {1}".format(task, state)
                tasks_to_remove.append(task)
                dst_client.remove(destination, force=True)
                semaphore.release()

        for task in tasks_to_remove:
            del running_tasks[task]

        while True:
            try:
                task, destination = queue.get_nowait()
            except Queue.Empty:
                break

            running_tasks[task] = destination

        time.sleep(0.3)

def run_tasks(client, task_limit):
    yt.config["proxy"]["url"] = SRC_YT_CLUSTER
    yt.write_table(SRC_TEST_TABLE_NAME, ["key=a\tvalue=b\n"], format="yamr")

    dst_yt_client = Yt(proxy=DST_YT_CLUSTER, token=yt.config["token"])
    dst_yt_client.create("map_node", DST_TEST_TABLES_PATH, ignore_existing=True)

    queue = Queue.Queue()
    semaphore = Semaphore(task_limit)

    thread = Thread(target=waiting_thread, args=(client, yt.config["token"], queue, semaphore))
    thread.daemon = True
    thread.start()

    print >>sys.stderr, "Starting tasks creation. Task limit: {0}".format(task_limit)
    while True:
        semaphore.acquire()
        destination = dst_yt_client.create_temp_table(path=DST_TEST_TABLES_PATH)
        task_id = client.add_task(SRC_YT_CLUSTER, SRC_TEST_TABLE_NAME, DST_YT_CLUSTER, destination)
        queue.put((task_id, destination))

        time.sleep(0.5)

def main():
    options_parser = argparse.ArgumentParser(add_help=False)
    options_parser.add_argument("--url", help="Transfer Manager url")

    parser = argparse.ArgumentParser(parents=[options_parser])
    subparsers = parser.add_subparsers(metavar="command", dest="command")

    load_parser = subparsers.add_parser("make-load", help="Keep running specified number of tasks")
    load_parser.add_argument("--task-limit", type=int, default=10)

    options, remaining_args = options_parser.parse_known_args()

    client = TransferManager(url=options.url)

    args = parser.parse_args(remaining_args)
    if args.command == "make-load":
        run_tasks(client, args.task_limit)

if __name__ == "__main__":
    main()
