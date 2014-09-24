#!/usr/bin/python

import sys
import yt.logger as logger
import yt.wrapper as yt
from dateutil.parser import parse
from collections import namedtuple
from datetime import datetime, timedelta

import argparse
import logging

Oper = namedtuple("Oper", ["start_time", "finish_time", "id", "user", "state", "spec"]);

logger.set_formatter(logging.Formatter('%(asctime)-15s\t{}\t%(message)s'.format(yt.config.http.PROXY)))

def is_casual(op):
    return op.state in ["completed", "aborted"] and len(yt.get("//sys/operations/%s/jobs" % op.id)) == 0

def clean_operations(count, total_count, failed_timeout, max_operations_per_user, robots, log):
    """Clean all operations started no more than #days days ago,
       leaving no more than #count most recent operations."""
    if robots is None:
        robots = []

    operations = yt.get("//sys/operations", attributes=['state', 'start_time', 'finish_time', 'spec', 'authenticated_user'])
    operations = [Oper(
        parse(v.attributes["start_time"]).replace(tzinfo=None),
        v.attributes.get("finish_time", None),
        k,
        v.attributes.get("authenticated_user", "unknown"),
        v.attributes["state"],
        v.attributes['spec'])
            for k, v in operations.iteritems()];
    operations.sort(reverse=True)

    saved = 0
    to_remove = []

    def is_final(state):
        return state in ["completed", "aborted", "failed"]

    operations.sort(key=lambda op: op.start_time, reverse=True)

    users = {}
    for op in operations:
        if not is_final(op.state):
            continue

        # Ignore just completed operation to avoid conflict with snapshot upload transaction.
        if datetime.utcnow() - parse(op.finish_time).replace(tzinfo=None) < timedelta(seconds=30):
            continue

        if op.user not in users:
            users[op.user] = 0
        users[op.user] += 1

        time_since = datetime.utcnow() - op.start_time
        is_old = (time_since > failed_timeout)

        is_regular = (op.user in robots) and not (op.state == "failed")

        is_user_limit_exceeded = users[op.user] > max_operations_per_user

        if is_regular or is_old or (saved >= total_count) or is_user_limit_exceeded or (saved >= count and op.user in users and is_casual(op)):
            to_remove.append(op.id)
        else:
            saved += 1


    if log is not None:
        log_output = open(log, "a")

    for op in to_remove:
        if not yt.exists("//sys/operations/%s" % op):
            continue
        if not is_final(yt.get("//sys/operations/%s/@state" % op)):
            logger.error("Trying to remove operation (%s) that is not in final state", op)
            sys.exit(1)
        if log is not None:
            log_output.write(yt.get("//sys/operations/%s/@" % op, format=yt.create_format("<format=text>json")))
            log_output.write("\n")
        logger.info("Removing operation %s", op)
        yt.remove("//sys/operations/%s" % op, recursive=True)

    if log is not None:
        log_output.close()


def main():
    parser = argparse.ArgumentParser(description='Clean operations from cypress.')
    parser.add_argument('--count', metavar='N', type=int, default=100,
                       help='leave no more than N completed (without stderr) or aborted operations')
    parser.add_argument('--total-count', metavar='N', type=int, default=2000,
                       help='leave no more that N operations totally')
    parser.add_argument('--failed-timeout', metavar='N', type=int, default=2,
                       help='remove all failed operation older than N days')
    parser.add_argument('--max-operations-per-user', metavar='N', type=int, default=200,
                       help='remove old operations of user if limit exceeded')
    parser.add_argument('--robot', action="append",  help='robot users that run operations very often and can be ignored')
    parser.add_argument('--log', help='file to save operation specs')

    args = parser.parse_args()
    clean_operations(args.count, args.total_count, timedelta(days=args.failed_timeout), args.max_operations_per_user, args.robot, args.log)

if __name__ == "__main__":
    main()
