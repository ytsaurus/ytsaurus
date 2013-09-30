#!/usr/bin/python

import sys
import yt.logger as logger
import yt.wrapper as yt
from dateutil.parser import parse
from collections import namedtuple
from datetime import datetime, timedelta

import argparse
import logging

Oper = namedtuple("Oper", ["time", "id", "user", "state", "spec"]);

logger.set_formatter(logging.Formatter('%(asctime)-15s\t{}\t%(message)s'.format(yt.config.http.PROXY)))

def is_casual(op):
    return op.state in ["completed", "aborted"] and len(yt.get("//sys/operations/%s/jobs" % op.id)) == 0

def clean_operations(count, total_count, failed_timeout, robots, log):
    """Clean all operations started no more than #days days ago,
       leaving no more than #count most recent operations."""
    if robots is None:
        robots = []

    operations = yt.get("//sys/operations", attributes=['state', 'start_time', 'spec', 'authenticated_user'])
    operations = [Oper(
        parse(v.attributes["start_time"]).replace(tzinfo=None),
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

    operations.sort(key=lambda op: op.time, reverse=True)

    users = set()
    for op in operations:
        if not is_final(op.state):
            continue

        time_since = datetime.utcnow() - op.time
        is_old = (time_since > failed_timeout)

        is_regular = (op.user in robots)

        if is_regular or is_old or (saved >= total_count) or (saved >= count and op.user in users and is_casual(op)):
            to_remove.append(op.id)
        else:
            saved += 1

        users.add(op.user)

    if log is not None:
        log_output = open(log, "a")

    for op in to_remove:
        if not yt.exists("//sys/operations/%s" % op):
            continue
        if not is_final(yt.get("//sys/operations/%s/@state" % op)):
            logger.error("Trying to remove operation (%s) that is not in final state", op)
            sys.exit(1)
        if log is not None:
            log_output.write(yt.get("//sys/operations/%s/@" % op, format=yt.Format("<format=text>json")))
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
    parser.add_argument('--robot', action="append",  help='robot users that run operations very often and can be ignored')
    parser.add_argument('--log', help='file to save operation specs')

    args = parser.parse_args()
    clean_operations(args.count, args.total_count, timedelta(days=args.failed_timeout), args.robot, args.log)

if __name__ == "__main__":
    main()
