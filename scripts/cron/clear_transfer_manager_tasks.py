#!/usr/bin/python

import yt.logger as logger

import json
import argparse
import logging
import requests
from dateutil.parser import parse
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta

Task = namedtuple("Task", ["start_time", "finish_time", "id", "user", "state"]);

logger.set_formatter(logging.Formatter('%(asctime)-15s\t%(message)s'))

def is_casual(task):
    return task.state in ["completed", "aborted", "skipped"]

def is_final(state):
    return state in ["completed", "aborted", "failed", "skipped"]

def remove_tasks(tasks_to_remove, url, token):
    for task in tasks_to_remove:
        logger.info("Task %s will be removed", task)

    headers = {
        "Authorization": "OAuth " + token,
        "Content-Type": "application/json"
    }

    rsp = requests.delete("{0}/tasks/".format(url),
                          data=json.dumps(tasks_to_remove),
                          headers=headers)

    if str(rsp.status_code).startswith("5"):
        logger.error("Batch deletion failed. Transfer Manager is not available")
    elif str(rsp.status_code).startswith("4"):
        logger.error("Batch deletion failed with error: %s", rsp.json()["message"])
    else:
        logger.info("Successfully deleted %d tasks", len(tasks_to_remove))

def create_task(task_dict):
    start_time = parse(
        task_dict.get("start_time", task_dict["creation_time"])).replace(tzinfo=None)

    finish_time = task_dict.get("finish_time", None)
    user = task_dict.get("user", "unknown")
    id_ = task_dict["id"]
    state = task_dict["state"]

    return Task(start_time, finish_time, id_, user, state)

def clean_tasks(url, token, count, total_count, failed_timeout, max_regular_tasks_per_user,
                max_failed_tasks_per_user, robots, batch_size):
    if robots is None:
        robots = []

    logger.info("Requesting all tasks")
    rsp = requests.get("{0}/tasks/".format(url))
    logger.info("Tasks recieved")

    tasks = [create_task(task_dict) for task_dict in rsp.json()]
    tasks.sort(key=lambda task: task.start_time, reverse=True)

    saved = 0
    to_remove = []

    users_regular = defaultdict(int)
    users_failed = defaultdict(int)
    users = set()

    for task in tasks:
        if not is_final(task.state):
            continue

        users.add(task.user)

        time_since = datetime.utcnow() - task.start_time
        is_old = (time_since > failed_timeout)

        is_regular = (task.user in robots) and (task.state != "failed")

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

    logger.info("%d tasks will be removed", len(to_remove))

    batch_start_index = 0
    while batch_start_index < len(to_remove):
        batch_end_index = batch_start_index + batch_size
        batch = to_remove[batch_start_index:batch_end_index]

        if batch:
            remove_tasks(batch, url, token)

        batch_start_index += batch_size

def main():
    parser = argparse.ArgumentParser(description="Clean transfer manager tasks.")
    parser.add_argument("url", help="Transfer Manager url")
    parser.add_argument("--token", help="token of administer user")
    parser.add_argument("--count", metavar="N", type=int, default=500,
                        help="leave no more than N completed (without stderr) or aborted tasks")
    parser.add_argument("--total-count", metavar="N", type=int, default=2000,
                        help="leave no more that N tasks totally")
    parser.add_argument("--failed-timeout", metavar="N", type=int, default=30,
                        help="remove all failed task older than N days")
    parser.add_argument("--max-regular-tasks-per-user", metavar="N", type=int, default=50,
                        help="remove old task of user if limit exceeded")
    parser.add_argument("--max-failed-tasks-per-user", metavar="N", type=int, default=50,
                        help="remove old task of user if limit exceeded")
    parser.add_argument("--robot", action="append", help="robot users that run tasks very often and can be ignored")
    parser.add_argument("--batch-size", metavar="N", type=int, default=100,
                        help="number of tasks to remove per one delete request")

    args = dict(vars(parser.parse_args()))
    args["failed_timeout"] = timedelta(days=args["failed_timeout"])
    args["robots"] = args.pop("robot")

    clean_tasks(**args)

if __name__ == "__main__":
    main()
