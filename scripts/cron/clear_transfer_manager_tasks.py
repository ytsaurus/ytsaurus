#!/usr/bin/python

import yt.logger as logger
import yt.wrapper as yt
from dateutil.parser import parse
from collections import namedtuple
from datetime import datetime, timedelta

import argparse
import logging

Task = namedtuple("Oper", ["start_time", "finish_time", "id", "user", "state"]);

logger.set_formatter(logging.Formatter('%(asctime)-15s\t{}\t%(message)s'.format(yt.config.http.PROXY)))

def clean_tasks(transfer_manager_path, count, total_count, failed_timeout, max_tasks_per_user, robots):
    if robots is None:
        robots = []

    tasks_path = transfer_manager_path + "/tasks"

    tasks = yt.get(tasks_path)
    tasks = [Task(
        parse(v.get("start_time", v["creation_time"])).replace(tzinfo=None),
        v.get("finish_time", None),
        k,
        v.get("user", "unknown"),
        v["state"])
            for k, v in tasks.iteritems()];

    saved = 0
    to_remove = []

    def is_casual(task):
        return task.state in ["completed", "aborted"] and len(yt.get("%s/%s/jobs" % (tasks_path, task.id))) == 0

    def is_final(state):
        return state in ["completed", "aborted", "failed"]

    tasks.sort(key=lambda task: task.start_time, reverse=True)

    users = {}
    for task in tasks:
        if not is_final(task.state):
            continue

        if task.user not in users:
            users[task.user] = 0
        users[task.user] += 1

        time_since = datetime.utcnow() - task.start_time
        is_old = (time_since > failed_timeout)

        is_regular = (task.user in robots) and not (task.state == "failed")

        is_user_limit_exceeded = users[task.user] > max_tasks_per_user

        if is_regular or is_old or (saved >= total_count) or is_user_limit_exceeded or (saved >= count and task.user in users and is_casual(task)):
            to_remove.append(task.id)
        else:
            saved += 1


    for task in to_remove:
        if not yt.exists("%s/%s" % (tasks_path, task)):
            continue
        if not is_final(yt.get("%s/%s/state" % (tasks_path, task))):
            logger.info("Task (%s) is no more in final state", task)
            continue
        logger.info("Removing task %s", task)
        yt.remove("%s/%s" % (tasks_path, task), recursive=True)


def main():
    parser = argparse.ArgumentParser(description='Clean transfer manager tasks.')
    parser.add_argument('--transfer-manager-path', default="//sys/transfer_manager",
                        help="Path to transfer manager in cypress")
    parser.add_argument('--count', metavar='N', type=int, default=500,
                       help='leave no more than N completed (without stderr) or aborted tasks')
    parser.add_argument('--total-count', metavar='N', type=int, default=2000,
                       help='leave no more that N tasks totally')
    parser.add_argument('--failed-timeout', metavar='N', type=int, default=30,
                       help='remove all failed task older than N days')
    parser.add_argument('--max-tasks-per-user', metavar='N', type=int, default=200,
                       help='remove old task of user if limit exceeded')
    parser.add_argument('--robot', action="append",  help='robot users that run tasks very often and can be ignored')

    args = parser.parse_args()
    clean_tasks(args.transfer_manager_path, args.count, args.total_count, timedelta(days=args.failed_timeout), args.max_tasks_per_user, args.robot)

if __name__ == "__main__":
    main()

