#!/usr/bin/python

from yt.wrapper.http_helpers import get_token, get_proxy_url
from yt.common import datetime_to_string, date_string_to_timestamp_mcs

from yt.operations_archive import *

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson
import yt.json as json
import yt.packages.requests as requests

from dateutil.parser import parse
from collections import namedtuple, Counter
# Import is necessary due to: http://bugs.python.org/issue7980
import _strptime
from datetime import datetime, timedelta
from threading import Thread, Lock
from logging import Formatter

import argparse
from time import sleep, mktime
from collections import deque

logger.set_formatter(Formatter("%(asctime)-15s\t{}\t%(message)s".format(yt.config["proxy"]["url"])))

yt.config["proxy"]["heavy_request_timeout"] = 20 * 1000

# Push metrics

def push_to_solomon(values_map, cluster, ts):
    data = {
        "commonLabels": {
            "project": "yt",
            "cluster": cluster,
            "service": "operations_archive",
            "host": "none"
        },
        "sensors": [{
            "labels": {
                "sensor": name
            },
            "ts": datetime_to_string(ts),
            "value": value
        } for name, value in values_map.iteritems()]
    }

    try:
        rsp = requests.post("http://api.solomon.search.yandex.net/push/json", headers={"Content-Type": "application/json"}, data=json.dumps(data), allow_redirects=True, timeout=20)
        if not rsp.ok:
            logger.info(rsp.content)
    except:
        logger.exception("Failed to push metrics to Solomon")

def clean_operations(soft_limit, hard_limit, grace_timeout, archive_timeout, execution_timeout,
                     max_operations_per_user, robots, archive, archive_jobs, thread_count,
                     stderr_thread_count, push_metrics, remove_threshold, client):

    now = datetime.utcnow()
    end_time_limit = now + execution_timeout
    archiving_time_limit = now + execution_timeout * 7 / 8

    #
    # Step 1: Fetch data from Cypress.
    #

    def maybe_parse_time(value):
        if value is None:
            return None
        else:
            return parse(value).replace(tzinfo=None)

    metrics = Counter({name: 0 for name in [
        "failed_to_archive_count",
        "archived_count",
        "failed_to_archive_job_count",
        "archived_job_count",
        "failed_to_archive_stderr_count",
        "archived_stderr_count",
        "archived_stderr_size",
        "failed_to_archive_stderr_count"]})

    timers = {name: Timer() for name in [
        "getting_job_counts",
        "getting_operations_list",
        "archiving_operations",
        "archiving_stderrs",
        "removing_operations"]}

    with timers["getting_operations_list"]:
        operations = client.list(
            "//sys/operations",
            max_size=100000,
            attributes=["state", "start_time", "finish_time", "authenticated_user"])
    operations = [Operation(
        maybe_parse_time(op.attributes.get("start_time", None)),
        maybe_parse_time(op.attributes.get("finish_time", None)),
        str(op),
        op.attributes["authenticated_user"],
        op.attributes["state"]) for op in operations]
    operations.sort(key=lambda op: op.start_time, reverse=True)

    metrics["initial_count"] = len(operations)

    #
    # Step 2: Filter out irrelevant operations.
    #

    def can_consider_archiving(op):
        # Ignore operations in progress or in transient states.
        if op.state not in ["completed", "aborted", "failed"]:
            return False
        # Ignore fresh operations to avoid conflicts with scheduler.
        if now - op.finish_time < grace_timeout:
            return False
        return True

    operations_to_consider = filter(can_consider_archiving, operations)
    metrics["considered_count"] = len(operations_to_consider)

    #
    # Step 3: Select operations to archive.
    #

    operations_with_job_counts = []
    with timers["getting_job_counts"]:
        consider_queue = NonBlockingQueue(operations_to_consider)
        run_batching_queue_workers(consider_queue, JobsCountGetter, thread_count, (client, operations_with_job_counts,))
        wait_for_queue(consider_queue, "get_job_count", archiving_time_limit)

    user_counts = Counter()
    number_of_retained_operations = 0

    def can_archive(op, job_count):
        if op.user in robots and op.state != "failed":
            return True
        if (now - op.start_time) > archive_timeout:
            return True
        if number_of_retained_operations >= hard_limit:
            return True
        if user_counts[op.user] > max_operations_per_user:
            return True
        if number_of_retained_operations >= soft_limit and op.state != "failed" and job_count == 0:
            return True
        return False

    operations_to_archive = []
    for op, job_count in operations_with_job_counts:
        user_counts[op.user] += 1
        if can_archive(op, job_count):
            operations_to_archive.append(op.id)
        else:
            number_of_retained_operations += 1

    metrics["approved_to_archive_count"] = len(operations_to_archive)

    now_before_clean = datetime.utcnow()

    if archive:
        operation_archiving_time_limit = datetime.utcnow() + (archiving_time_limit - datetime.utcnow()) / 2

        logger.info("Archiving %d operations", len(operations_to_archive))
        version = yt.get("{}/@".format(OPERATIONS_ARCHIVE_PATH)).get("version", 0)

        archive_jobs = archive_jobs and version >= 3

        archive_queue = NonBlockingQueue(operations_to_archive)
        remove_queue = NonBlockingQueue()
        stderr_queue = NonBlockingQueue()
        failed_to_archive = []

        thread_safe_metrics = ThreadSafeCounter(metrics)
        with timers["archiving_operations"]:
            run_batching_queue_workers(
                archive_queue,
                OperationArchiver,
                thread_count,
                (client, remove_queue, stderr_queue, version, archive_jobs, thread_safe_metrics),
                batch_size=32,
                failed_items=failed_to_archive)
            failed_to_archive.extend(wait_for_queue(archive_queue, "archive_operation", operation_archiving_time_limit))

        if archive_jobs:
            logger.info("Archiving %d stderrs", len(stderr_queue))

            with timers["archiving_stderrs"]:
                insert_queue = NonBlockingQueue()
                run_queue_workers(stderr_queue, StderrDownloader, stderr_thread_count, (client, insert_queue, version, thread_safe_metrics))
                run_batching_queue_workers(insert_queue, StderrInserter, thread_count, (client, thread_safe_metrics,))

                failed_stderr_count = 0
                failed_stderr_count += len(wait_for_queue(stderr_queue, "fetch_stderr", archiving_time_limit))
                failed_stderr_count += len(wait_for_queue(insert_queue, "insert_jobs", archiving_time_limit))
                thread_safe_metrics.add("failed_to_archive_stderr_count", failed_stderr_count)

        if len(failed_to_archive) > remove_threshold:
            remove_queue.put_many(failed_to_archive[remove_threshold:])
    else:
        remove_queue = NonBlockingQueue(operations_to_archive)

    remove_count = len(remove_queue)
    logger.info("Removing %d operations", len(remove_queue))

    failed_to_remove = []
    with timers["removing_operations"]:
        run_batching_queue_workers(remove_queue, OperationCleaner, thread_count, args=(client,), batch_size=8, failed_items=failed_to_remove)
        failed_to_remove.extend(wait_for_queue(remove_queue, "remove_operations", end_time_limit))

    now_after_clean = datetime.utcnow()

    total_time = (now_after_clean - now).total_seconds()

    logger.info(
        "; ".join([
            "Done",
            "processed %s operations in %.2fs",
            "%s were considered %s in %.2fs",
            "%s were %s in %.2fs",
            "%s were retained"]),
        len(operations),
        total_time,
        len(operations_to_consider),
        "archiving" if archive else "removing",
        (now_before_clean - now).total_seconds(),
        remove_count,
        "archived" if archive else "removed",
        (now_after_clean - now_before_clean).total_seconds(),
        number_of_retained_operations)

    logger.info("Times: (%s)", "; ".join(["{}: {}".format(name, str(timer)) for name, timer in timers.iteritems()]))

    metrics["total_time_ms"] = total_time * 1000
    metrics["per_operation_time_ms"] = total_time * 1000 / len(operations) if len(operations) > 0 else 0
    metrics["removed_count"] = remove_count - len(failed_to_remove)
    metrics["failed_to_remove_count"] = len(failed_to_remove)

    logger.info("Metrics: (%s)", "; ".join(["{}: {}".format(name, value) for name, value in metrics.iteritems()]))

    if push_metrics:
        for name, timer in timers.iteritems():
            metrics["{}_time_ms".format(name)] = timer.value() * 1000

        cluster_name = yt.config.config["proxy"]["url"].split(".")[0]
        push_to_solomon(metrics, cluster_name, now)

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
