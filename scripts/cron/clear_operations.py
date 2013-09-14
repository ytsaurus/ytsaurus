#!/usr/bin/python

import sys
import yt.wrapper as yt
from dateutil.parser import parse
from collections import namedtuple
from datetime import datetime, timedelta

import argparse
import logging

Oper = namedtuple("Oper", ["time", "id", "state", "spec"]);

logger = logging.getLogger("Cron")
logger.setLevel(level="INFO")

formatter = logging.Formatter('%(asctime)-15s\t{}\t%(message)s'.format(yt.config.http.PROXY))
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setFormatter(formatter)

def clean_operations(count, total_count, failed_timeout, robots, log):
    """Clean all operations started no more than #days days ago,
       leaving no more than #count most recent operations."""

    operations = yt.get("//sys/operations", attributes=['state', 'start_time', 'spec'])
    operations = [Oper(parse(v.attributes["start_time"]).replace(tzinfo=None), k, \
        v.attributes["state"], v.attributes['spec']) for k, v in operations.iteritems()];
    operations.sort(reverse=True)
    
    saved = 0
    to_remove = []

    def is_final(state):
        return state in ["completed", "aborted", "failed"]

    operations.sort(key=lambda op: op.time, reverse=True)

    for op in operations:
        if not is_final(op.state):
            continue
        
        is_casual = (op.state in ["completed", "aborted"]) and (len(yt.get("//sys/operations/%s/jobs" % op.id)) == 0)

        time_since = datetime.utcnow() - op.time 
        is_old = (time_since > failed_timeout)

        is_regular = (op.spec.get("authenticated_user", "unknown") in robots)

        if is_regular or (is_casual and saved >= count) or is_old or (saved >= total_count):
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
            log_output.write(yt.get("//sys/operations/%s/@" % op, format=yt.Format("<format=text>json")))
            log_output.write("\n")
        logger.info("Removing operation %s", op)
        yt.remove("//sys/operations/%s" % op, recursive=True)

    if log is not None:
        log_output.close()
    

def main():
    parser = argparse.ArgumentParser(description='Clean operations from cypress.')
    parser.add_argument('--count', metavar='N', type=int, default=50,
                       help='leave no more than N completed (without stderr) or aborted operations')
    parser.add_argument('--total-count', metavar='N', type=int, default=2000,
                       help='leave no more that N operations totally')
    parser.add_argument('--failed_timeout', metavar='N', type=int, default=2,
                       help='remove all failed operation older than N days')
    parser.add_argument('--robot', action="append",  help='robot users that run operations very often and can be ignored')
    parser.add_argument('--log', help='file to save operation specs')

    args = parser.parse_args()
    clean_operations(args.count, args.total_count, timedelta(days=args.failed_timeout), args.robot, args.log)

if __name__ == "__main__":
    main()
