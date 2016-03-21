#!/usr/bin/python

import prepare_operation_tablets
import yt.tools.operations_archive as operations_archive

from yt.wrapper.http import get_session
import yt.packages.requests.adapters as requests_adapters

from yt.wrapper.common import run_with_retries
import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson

from dateutil.parser import parse
from collections import namedtuple, Counter
# Import is necessary due to: http://bugs.python.org/issue7980
import _strptime
from datetime import datetime, timedelta
from threading import Thread
from logging import Formatter

import argparse
import sys
import time

Operation = namedtuple("Operation", ["start_time", "finish_time", "id", "user", "state", "spec"])

logger.set_formatter(Formatter("%(asctime)-15s\t{}\t%(message)s".format(yt.config.http.PROXY)))

class Try(object):
    def __init__(self, exc, obj):
        self.exc = exc
        self.obj = obj

    @property
    def value(self):
        if self.exc:
            # In this case self.obj represents exc_info, which is tuple (type, value, traceback)
            raise self.obj[0], self.obj[1], self.obj[2]
        else:
            return self.obj

    @staticmethod
    def success(value):
        return Try(False, value)

    @staticmethod
    def failure(value):
        return Try(True, value)


def parallel_map_impl(fn, kwargs, result, items, failure_limit=1):
    failures_count = 0
    for item in items:
        try:
            local_result = Try.success(fn(item, **kwargs))
        except:
            logger.exception("Handled exception")
            local_result = Try.failure(sys.exc_info())
            failures_count += 1
            if failures_count == failure_limit:
                return

        result.append(local_result)


def parallel_map(fn, kwargs, items, thread_count):
    items_per_thread = 1 + (len(items) / thread_count)

    results = []
    threads = []

    for thread_index in range(thread_count):
        begin_index = thread_index * items_per_thread
        end_index = min(len(items), (thread_index + 1) * items_per_thread)
        if begin_index >= end_index:
            break

        result = []
        thread = Thread(target=parallel_map_impl, args=(fn, kwargs, result, items[begin_index:end_index]))
        thread.start()

        results.append(result)
        threads.append(thread)

    for thread in threads:
        thread.join()

    return [item.value for result in results for item in result]

def get_filter_factors(op, attributes):
    brief_spec = attributes.get("brief_spec", {})
    return " ".join([
        op,
        attributes.get("key", ""),
        attributes.get("authenticated_user", ""),
        attributes.get("state", ""),
        attributes.get("operation_type", ""),
        attributes.get("pool", ""),
        brief_spec.get("title", ""),
        str(brief_spec.get('input_table_paths', [''])[0]),
        str(brief_spec.get('input_table_paths', [''])[0])
    ]).lower()

def clean_operation(op_id, scheme_type, archive=False):
    if archive:
        if not yt.exists(operations_archive.BY_ID_ARCHIVE) or not yt.exists(operations_archive.BY_START_TIME_ARCHIVE):
            if (scheme_type != "new"):
                raise Exception("Old scheme type is not supported")
            prepare_operation_tablets.prepare_tables(yt.config["proxy"]["url"])

        logger.info("Archiving operation %s", op_id)
        data = yt.get("//sys/operations/{}/@".format(op_id))

        keys = ["state", "authenticated_user", "operation_type",
                "progress", "brief_progress", "spec", "brief_spec", "result"]
        by_id_row = {}
        for key in keys:
            if key in data:
                by_id_row[key] = data[key]

        def datestr_to_timestamp(time_str):
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            return int(time.mktime(dt.timetuple()) * 1000000 + dt.microsecond)

        if scheme_type == "new":
            id_parts = op_id.split("-")

            id_hi = long(id_parts[3], 16) << 32 | int(id_parts[2], 16)
            id_lo = long(id_parts[1], 16) << 32 | int(id_parts[0], 16)

            by_id_row["id_hi"] = yson.YsonUint64(id_hi)
            by_id_row["id_lo"] = yson.YsonUint64(id_lo)
            by_id_row["start_time"] = datestr_to_timestamp(data["start_time"])
            by_id_row["finish_time"] = datestr_to_timestamp(data["finish_time"])
            by_id_row["filter_factors"] = get_filter_factors(op_id, data)

            by_start_time_row = {
                "id_hi": by_id_row["id_hi"],
                "id_lo": by_id_row["id_lo"],
                "start_time": by_id_row["start_time"],
                "dummy": 0
            }
        else:
            for key in ["start_time", "finish_time", "result"]:
                by_id_row[key] = data[key]
            by_id_row["id"] = op_id
            by_id_row["filter_factors"] = get_filter_factors(op_id, data)
            by_start_time_row = {
                "id": by_id_row["id"],
                "start_time": by_id_row["start_time"],
                "dummy": "null"
            }

        run_with_retries(lambda: yt.insert_rows(operations_archive.BY_ID_ARCHIVE, [by_id_row], raw=False))
        run_with_retries(lambda: yt.insert_rows(operations_archive.BY_START_TIME_ARCHIVE, [by_start_time_row], raw=False))
    else:
        logger.info("Removing operation %s", op_id)

    try:
        yt.remove("//sys/operations/{}".format(op_id), recursive=True)
    except yt.YtResponseError as err:
        if not err.is_resolve_error():
            raise


def clean_operations(soft_limit, hard_limit, grace_timeout, archive_timeout,
                     max_operations_per_user, robots, log, archive, scheme_type, thread_count):
    if scheme_type not in ["old", "new"]:
        raise Exception("Incorrect scheme type (%s not in ['old', 'new'])")

    #
    # Step 1: Fetch data from Cypress.
    #

    # XXX(ignat): Hack to increase requests connection pool size.
    get_session().mount("http://", requests_adapters.HTTPAdapter(pool_connections=thread_count, pool_maxsize=thread_count))

    if archive:
        yt.config.VERSION = "v3"

    def maybe_parse_time(value):
        if value is None:
            return None
        else:
            return parse(value).replace(tzinfo=None)

    now = datetime.utcnow()

    operations = yt.list("//sys/operations",
                         max_size=100000,
                         attributes=["state", "start_time", "finish_time", "spec", "authenticated_user"])
    operations = [Operation(
        maybe_parse_time(op.attributes.get("start_time", None)),
        maybe_parse_time(op.attributes.get("finish_time", None)),
        str(op),
        op.attributes["authenticated_user"],
        op.attributes["state"],
        op.attributes["spec"]) for op in operations]
    operations.sort(key=lambda op: op.start_time, reverse=True)

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

    #
    # Step 3: Select operations to archive.
    #

    def get_number_of_jobs(op):
        try:
            return yt.get("//sys/operations/{}/jobs/@count".format(op.id))
        except yt.YtResponseError as err:
            if err.is_resolve_error():
                return 0
            else:
                raise

    job_counts = parallel_map(get_number_of_jobs, {}, operations_to_consider, thread_count)
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
    for op, job_count in zip(operations_to_consider, job_counts):
        if not can_consider_archiving(op):
            continue
        user_counts[op.user] += 1
        if can_archive(op, job_count):
            operations_to_archive.append(op.id)
        else:
            number_of_retained_operations += 1

    now_before_clean = datetime.utcnow()

    parallel_map(clean_operation, {"scheme_type": scheme_type, "archive": archive}, operations_to_archive, thread_count)

    now_after_clean = datetime.utcnow()

    logger.info(
        "; ".join([
            "Done",
            "processed %s operations in %.2fs",
            "%s were considered %s",
            "%s were %s in %.2fs",
            "%s were retained"]),
        len(operations),
        (now_before_clean - now).total_seconds(),
        len(operations_to_consider),
        "archiving" if archive else "removing",
        len(operations_to_archive),
        "archived" if archive else "removed",
        (now_after_clean - now_before_clean).total_seconds(),
        number_of_retained_operations)


def main():
    parser = argparse.ArgumentParser(description="Clean operations from cypress.")
    parser.add_argument("--soft-limit", metavar="N", type=int, default=100,
                        help="leave no more than N completed (without stderr) or aborted operations")
    parser.add_argument("--hard-limit", metavar="N", type=int, default=2000,
                        help="leave no more that N operations totally")
    parser.add_argument("--grace-timeout", metavar="N", type=int, default=30,
                        help="do not touch operations within N seconds of their completion to avoid races")
    parser.add_argument("--archive-timeout", metavar="N", type=int, default=2,
                        help="remove all failed operation older than N days")
    parser.add_argument("--max-operations-per-user", metavar="N", type=int, default=200,
                        help="remove old operations of user if limit exceeded")
    parser.add_argument("--robot", action="append",
                        help="robot users that run operations very often and can be ignored")
    parser.add_argument("--log", help="file to save operation specs")
    parser.add_argument("--archive", action="store_true", default=False,
                        help="whether save cleared operations to tablets")
    parser.add_argument("--scheme-type", default="old",
                        help="scheme type of operations archive, possible values: 'old', 'new'")
    parser.add_argument("--thread-count", metavar="N", type=int, default=24,
                        help="parallelism level for operation cleansing")

    args = parser.parse_args()

    clean_operations(
        args.soft_limit,
        args.hard_limit,
        timedelta(seconds=args.grace_timeout),
        timedelta(days=args.archive_timeout),
        args.max_operations_per_user,
        args.robot if args.robot is not None else [],
        args.log,
        args.archive,
        args.scheme_type,
        args.thread_count)

if __name__ == "__main__":
    main()
