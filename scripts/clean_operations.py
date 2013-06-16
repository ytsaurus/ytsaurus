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

formatter = logging.Formatter('%(asctime)-15s: %(message)s')
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setFormatter(formatter)

def clean_operations(count, failed_timeout):
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

    for op in operations:
        is_casual = (op.state in ["completed", "aborted"]) and (len(yt.get("//sys/operations/%s/jobs" % op.id)) == 0)

        time_since = datetime.utcnow() - op.time 
        is_old = (time_since > failed_timeout)

        is_regular = (op.spec.get("system_user", "unknown") in ["crawler", "cron", "odin"])

        if is_casual:
            if is_regular or saved >= count:
                to_remove.append(op.id)
            else:
                saved += 1
        
        if is_final(op.state) and is_old:
            to_remove.append(op.id)
   
    for op in to_remove:
        if not yt.exists("//sys/operations/%s" % op):
            continue
        if not is_final(yt.get("//sys/operations/%s/@state" % op)):
            logger.error("Trying to remove operation (%s) that is not in final state", op)
            sys.exit(1)
        logger.info("Removing operation %s" % op)
        yt.remove("//sys/operations/%s" % op, recursive=True)
    

def main():

    parser = argparse.ArgumentParser(description='Clean operations from cypress.')
    parser.add_argument('--count', metavar='N', type=int, default=50,
                       help='leave history no more than N operations')
    parser.add_argument('--failed_timeout', metavar='N', type=int, default=2,
                       help='remove all failed operation older than N days')

    args = parser.parse_args()
    clean_operations(args.count, timedelta(days=args.failed_timeout))

if __name__ == "__main__":
    main()
