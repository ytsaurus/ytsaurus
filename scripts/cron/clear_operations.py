#!/usr/bin/python

from yt.operations_archive import *

import yt.wrapper as yt

import yt.logger as logger
from datetime import timedelta
from logging import Formatter

import argparse

logger.set_formatter(Formatter("%(asctime)-15s\t{}\t%(message)s".format(yt.config["proxy"]["url"])))

yt.config["proxy"]["heavy_request_timeout"] = 20 * 1000

def main():
    parser = argparse.ArgumentParser(description="Clean operations from cypress.")
    parser.add_argument("--soft-limit", metavar="N", type=int, default=100,
                        help="leave no more than N completed (without stderr) or aborted operations")
    parser.add_argument("--hard-limit", metavar="N", type=int, default=2000,
                        help="leave no more that N operations totally")
    parser.add_argument("--grace-timeout", metavar="N", type=int, default=120,
                        help="do not touch operations within N seconds of their completion to avoid races")
    parser.add_argument("--archive-timeout", metavar="N", type=int, default=2,
                        help="remove all failed operation older than N days")
    parser.add_argument("--execution-timeout", metavar="N", type=int, default=280,
                        help="max execution time N seconds")
    parser.add_argument("--max-operations-per-user", metavar="N", type=int, default=200,
                        help="remove old operations of user if limit exceeded")
    parser.add_argument("--robot", action="append",
                        help="robot users that run operations very often and can be ignored")
    parser.add_argument("--archive", action="store_true", default=False,
                        help="whether save cleared operations to tablets")
    parser.add_argument("--archive-jobs", action="store_true", default=False,
                        help="whether save jobs and stderrs to tablets")
    parser.add_argument("--push-metrics", action="store_true", default=False,
                        help="whether push metrics to solomon")
    parser.add_argument("--scheme-type", default="old",
                        help="(deprecated) scheme type of operations archive, possible values: 'old', 'new'")
    parser.add_argument("--thread-count", metavar="N", type=int, default=24,
                        help="parallelism level for operation cleansing")
    parser.add_argument("--stderr-thread-count", metavar="N", type=int, default=96,
                        help="parallelism level for downloading stderrs")
    parser.add_argument("--remove-threshold", metavar="N", type=int, default=20000,
                        help="remove operations without archiving after N threshold")

    args = parser.parse_args()

    clean_operations(
        args.soft_limit,
        args.hard_limit,
        timedelta(seconds=args.grace_timeout),
        timedelta(days=args.archive_timeout),
        timedelta(seconds=args.execution_timeout),
        args.max_operations_per_user,
        args.robot if args.robot is not None else [],
        args.archive,
        args.archive_jobs,
        args.thread_count,
        args.stderr_thread_count,
        args.push_metrics,
        args.remove_threshold,
        yt)

if __name__ == "__main__":
    main()
