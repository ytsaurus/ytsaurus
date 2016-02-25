#!/usr/bin/env python

from yt.transfer_manager.client import TransferManager
import yt.wrapper as yt

import sys
import argparse
import Queue
from threading import Thread, Semaphore
import random
import string
import time

TEST_TABLE_NAME = "//tmp/tm_load_tester_table"

def waiting_thread(client, queue, semaphore):
    running_tasks = []
    while True:
        tasks_to_remove = []
        for task in running_tasks:
            state = client.get_task_info(task)["state"]
            if state in ["aborted", "failed", "completed"]:
                print >>sys.stderr, "Task {0} {1}".format(task, state)
                tasks_to_remove.append(task)
                semaphore.release()

        for task in tasks_to_remove:
            running_tasks.remove(task)

        while True:
            try:
                task = queue.get_nowait()
            except Queue.Empty:
                break

            running_tasks.append(task)

        time.sleep(0.3)

def run_tasks(client, task_limit):
    yt.config["proxy"]["url"] = "plato"
    yt.write_table(TEST_TABLE_NAME, ["key=a\tvalue=b\n"], format="yamr")

    queue = Queue.Queue()
    semaphore = Semaphore(task_limit)

    thread = Thread(target=waiting_thread, args=(client, queue, semaphore))
    thread.daemon = True
    thread.start()

    print >>sys.stderr, "Starting tasks creation. Task limit: {0}".format(task_limit)
    while True:
        semaphore.acquire()
        destination = "tmp/yt_" + "".join(random.sample(string.ascii_letters + string.digits, 7))
        task_id = client.add_task("plato", TEST_TABLE_NAME, "sakura", destination,
                                  params={"mr_user": "userdata", "copy_method": "push"})
        queue.put(task_id)

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
