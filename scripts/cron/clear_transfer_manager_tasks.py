#!/usr/bin/python

import yt.logger as logger
from dateutil.parser import parse
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from threading import Thread
import argparse
import logging
import requests

Task = namedtuple("Oper", ["start_time", "finish_time", "id", "user", "state"]);

logger.set_formatter(logging.Formatter('%(asctime)-15s\t%(message)s'))


def is_casual(task):
    return task.state in ["completed", "aborted"]


def is_final(state):
    return state in ["completed", "aborted", "failed"]


def remove_tasks(tasks_to_remove, url, token):
    for task in tasks_to_remove:
        rsp = requests.get("%s/tasks/%s/" % (url, task))
        if rsp.status_code != 200:
            logger.info("Task %s is already removed", task)
            continue
        if not is_final(rsp.json()["state"]):
            logger.info("Task (%s) is no more in final state", task)
            continue
        logger.info("Removing task %s", task)
        rsp = requests.delete("%s/tasks/%s/" % (url, task),
                              headers={"Authorization": "OAuth " + token})
        if rsp.status_code != 200:
            logger.error("Cannot remove task %s: %s", task, rsp.content)
            # d3rp@: replace break to continue, YTADMIN-3292
            continue


def clean_tasks(url, token, count, total_count, failed_timeout, max_regular_tasks_per_user, max_failed_tasks_per_user,
                robots, thread_count):
    if robots is None:
        robots = []

    logger.info("Request tasks")
    rsp = requests.get(url + "/tasks/")
    logger.info("Tasks recieved")

    tasks = [Task(
        parse(v.get("start_time", v["creation_time"])).replace(tzinfo=None),
        v.get("finish_time", None),
        v["id"],
        v.get("user", "unknown"),
        v["state"])
             for v in rsp.json()]

    saved = 0
    to_remove = []

    tasks.sort(key=lambda task: task.start_time, reverse=True)

    users_regular = defaultdict(int)
    users_failed = defaultdict(int)
    users = set()
    for task in tasks:
        if not is_final(task.state):
            continue

        users.add(task.user)

        time_since = datetime.utcnow() - task.start_time
        is_old = (time_since > failed_timeout)

        is_regular = (task.user in robots) and not (task.state == "failed")
        if task.state == "failed":
            users_failed[task.user] += 1
            is_user_limit_exceeded = users_failed[task.user] > max_failed_tasks_per_user
        else:
            users_regular[task.user] += 1
            is_user_limit_exceeded = users_regular[task.user] > max_regular_tasks_per_user

        if is_regular or is_old or (saved >= total_count) or is_user_limit_exceeded or (
                    saved >= count and task.user in users and is_casual(task)):
            to_remove.append(task.id)
        else:
            saved += 1

    threads = []

    tasks_per_thread = 1 + len(to_remove) / thread_count
    for thread_index in xrange(thread_count):
        start_index = thread_index * tasks_per_thread
        end_index = min(len(to_remove), (thread_index + 1) * tasks_per_thread)
        if start_index >= end_index:
            break
        tasks_for_thread = to_remove[start_index:end_index]
        thread = Thread(target=remove_tasks, args=(tasks_for_thread, url, token))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


def main():
    parser = argparse.ArgumentParser(description='Clean transfer manager tasks.')
    parser.add_argument('url', help="Url of transfer manager")
    parser.add_argument('--token', help="Token of administer user")
    parser.add_argument('--count', metavar='N', type=int, default=500,
                        help='leave no more than N completed (without stderr) or aborted tasks')
    parser.add_argument('--total-count', metavar='N', type=int, default=2000,
                        help='leave no more that N tasks totally')
    parser.add_argument('--failed-timeout', metavar='N', type=int, default=30,
                        help='remove all failed task older than N days')
    parser.add_argument('--max-regular-tasks-per-user', metavar='N', type=int, default=50,
                        help='remove old task of user if limit exceeded')
    parser.add_argument('--max-failed-tasks-per-user', metavar='N', type=int, default=50,
                        help='remove old task of user if limit exceeded')
    parser.add_argument('--robot', action="append", help='robot users that run tasks very often and can be ignored')
    parser.add_argument('--thread-count', metavar='N', type=int, default=10,
                        help='number of thread that remove tasks')

    args = parser.parse_args()
    clean_tasks(args.url, args.token, args.count, args.total_count, timedelta(days=args.failed_timeout),
                args.max_regular_tasks_per_user, args.max_failed_tasks_per_user, args.robot, args.thread_count)


if __name__ == "__main__":
    main()
