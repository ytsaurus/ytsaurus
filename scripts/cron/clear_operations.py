#!/usr/bin/python

from yt.wrapper.http_helpers import get_token, get_proxy_url
from yt.common import datetime_to_string, date_string_to_timestamp_mcs

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson
import yt.json as json


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

import yt.packages.requests as requests

OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive"
BY_ID_ARCHIVE_PATH = "{}/ordered_by_id".format(OPERATIONS_ARCHIVE_PATH)
BY_START_TIME_ARCHIVE_PATH = "{}/ordered_by_start_time".format(OPERATIONS_ARCHIVE_PATH)
STDERRS_PATH = "{}/stderrs".format(OPERATIONS_ARCHIVE_PATH)
JOBS_PATH = "{}/jobs".format(OPERATIONS_ARCHIVE_PATH)

Operation = namedtuple("Operation", ["start_time", "finish_time", "id", "user", "state"])

logger.set_formatter(Formatter("%(asctime)-15s\t{}\t%(message)s".format(yt.config["proxy"]["url"])))

yt.config["proxy"]["heavy_request_timeout"] = 20 * 1000

class Timer(object):
    def __init__(self):
        self.elapsed = timedelta(0)
        self.start = None

    def __str__(self):
        return "{0:.3f}s".format(self.elapsed.total_seconds())

    def value(self):
        return self.elapsed.total_seconds()

    def __enter__(self):
        if self.start is not None:
            raise Exception("Recursive usage of class Timer")
        else:
            self.start = datetime.utcnow()

    def __exit__(self, type, value, traceback):
        self.elapsed += datetime.utcnow() - self.start
        self.start = None

class ThreadSafeCounter(object):
    def __init__(self, counter):
        self.counter = counter
        self.mutex = Lock()

    def add(self, name, count):
        with self.mutex:
            self.counter[name] += count

# Standard python Queues have thread blocking methods such as join.
class NonBlockingQueue(object):
    def __init__(self, iterable=None):
        self.data = deque(iterable) if iterable is not None else deque()
        self.executing_count = 0
        self.closed = False
        self.active = False
        self.mutex = Lock()

    def put(self, value):
        self.put_many([value])

    def put_many(self, values):
        with self.mutex:
            self.data.extend(values)

    def get(self, default=None):
        with self.mutex:
            if not self.data:
                return default
            else:
                self.executing_count += 1
                return self.data.popleft()

    def task_done(self, count=1):
        with self.mutex:
            if self.executing_count >= count:
                self.executing_count -= count
            else:
                raise ValueError("Extra task_done called")

    def __len__(self):
        return len(self.data)

    def close(self):
        self.closed = True

    def activate(self):
        self.active = True

    def is_finished(self):
        with self.mutex:
            return self.executing_count == 0 and len(self.data) == 0 and self.active

    def is_done(self):
        with self.mutex:
            return self.executing_count == 0 and len(self.data) == 0 and self.active or self.closed

    def clear(self):
        result = []
        with self.mutex:
            while len(self.data) > 0:
                result.append(self.data.pop())
        return result

def queue_worker(queue, worker_cls, args=()):
    worker = worker_cls(*args)
    while not queue.is_done():
        value = queue.get()
        if value is None:
            sleep(0.1)
        else:
            try:
                worker(value)
            except:
                logger.exception("Exception caught")
            finally:
                queue.task_done()

def batching_queue_worker(queue, worker_cls, args=(), batch_size=32, failed_items=None):
    worker = worker_cls(*args)
    while not queue.is_done():
        values = []
        while len(values) < batch_size:
            value = queue.get()
            if value is None:
                break
            else:
                values.append(value)
        if not values:
            sleep(0.1)
        else:
            try:
                worker(values[:])
            except:
                logger.exception("Exception caught")
                if failed_items is not None:
                    failed_items.extend(values[:])
            finally:
                queue.task_done(len(values))

def run_workers(worker, args, thread_count):
    for _ in range(thread_count):
       thread = Thread(target=worker, args=args)
       thread.daemon = True
       thread.start()

def run_queue_workers(queue, worker_cls, thread_count, args=()):
    run_workers(queue_worker, (queue, worker_cls, args), thread_count)

def run_batching_queue_workers(queue, worker_cls, thread_count, args=(), batch_size=32, failed_items=None):
    run_workers(batching_queue_worker, (queue, worker_cls, args, batch_size, failed_items), thread_count)

def wait_for_queue(queue, name, end_time=None, sleep_timeout=0.2):
    queue.activate()
    counter = 0
    result = []
    while True:
        if queue.is_finished():
            break
        else:
            sleep(sleep_timeout)
            if counter % max(int(1.0 / sleep_timeout), 1) == 0:
                logger.info(
                    "Waiting for processing items in queue '%s' (left items: %d, items in progress: %d, time left: %s)",
                    name,
                    len(queue),
                    queue.executing_count,
                    str(end_time - datetime.utcnow()) if end_time is not None else "inf")
        counter += 1
        if end_time is not None and datetime.utcnow() > end_time:
            logger.info("Waiting timeout is expired for queue '%s'", name)
            queue.close()
            result.extend(queue.clear())
    logger.info("Joined queue '%s'", name)
    result.extend(queue.clear())
    return result

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

# Clear operations scpecific

def get_filter_factors(op, attributes):
    brief_spec = attributes.get("brief_spec", {})
    return " ".join([
        op,
        attributes.get("key", ""),
        attributes.get("authenticated_user", ""),
        attributes.get("state", ""),
        attributes.get("operation_type", ""),
        brief_spec.get("pool", ""),
        brief_spec.get("title", ""),
        str(brief_spec.get("input_table_paths", [""])[0]),
        str(brief_spec.get("input_table_paths", [""])[0])
    ]).lower()

def datestr_to_timestamp_legacy(time_str):
    dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(mktime(dt.timetuple()) * 1000000 + dt.microsecond)

def id_to_parts_old(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[3], 16) << 32 | int(id_parts[2], 16)
    id_lo = long(id_parts[1], 16) << 32 | int(id_parts[0], 16)
    return id_hi, id_lo

def id_to_parts_new(id):
    id_parts = id.split("-")
    id_hi = long(id_parts[2], 16) << 32 | int(id_parts[3], 16)
    id_lo = long(id_parts[0], 16) << 32 | int(id_parts[1], 16)
    return id_hi, id_lo

def id_to_parts(id, version):
    return (id_to_parts_new if version >= 6 else id_to_parts_old)(id)

class JobsCountGetter(object):
    def __init__(self, operations_with_job_counts):
        self.operations_with_job_counts = operations_with_job_counts
        self.yt = yt.YtClient(config=yt.config.config)

    def __call__(self, operations):
        responses = self.yt.execute_batch(requests=[{
                "command": "get",
                "parameters": {
                    "path": "//sys/operations/{}/jobs/@count".format(op.id)
                }
            } for op in operations])

        for op, rsp in zip(operations, responses):
            #if "error" not in rsp:
            #    logger.info("job_count %s %s", rsp["output"], str(type(rsp["output"])))
            self.operations_with_job_counts.append((op, 0 if "error" in rsp else rsp["output"]))

class OperationCleaner(object):
    def __init__(self):
        self.yt = yt.YtClient(config=yt.config.config)

    def __call__(self, op_ids):
        for op_id in op_ids:
            logger.info("Removing operation %s", op_id)

        responses = self.yt.execute_batch(requests=[{
                "command": "remove",
                "parameters": {
                    "path": "//sys/operations/{}".format(op_id),
                    "recursive": True
                }
            } for op_id in op_ids])

        errors = []
        for rsp in responses:
            if "error" in rsp:
                errors.append(yt.YtResponseError(rsp["error"]))
                raise yt.YtResponseError(rsp["error"])

        if errors:
            raise yt.YtError("Failed to remove operations", inner_errors=[errors])

class OperationArchiver(object):
    ATTRIBUTES = [
        "brief_statistics",
        "error",
        "job_type",
        "state",
        "address",
        "uncompressed_data_size",
        "error",
        "size",
        "start_time",
        "finish_time",
        "uncompressed_data_size"
    ]

    def __init__(self, clean_queue, stderr_queue, version, archive_jobs, metrics):
        self.clean_queue = clean_queue
        self.stderr_queue = stderr_queue
        self.version = version
        self.archive_jobs = archive_jobs
        self.metrics = metrics
        self.yt = yt.YtClient(config=yt.config.config)

        if not self.yt.exists(BY_ID_ARCHIVE_PATH) or not self.yt.exists(BY_START_TIME_ARCHIVE_PATH):
            raise Exception("Operations archive tables do not exist")

    def get_archive_rows(self, op_id, data):
        logger.info("Archiving operation %s", op_id)

        index_columns = ["state", "authenticated_user", "operation_type"]
        value_columns = ["progress", "brief_progress", "spec", "brief_spec", "result"]

        if self.version >= 5:
            value_columns.append("events")
        if self.version >= 10:
            value_columns.append("alerts")
        if self.version >= 13:
            value_columns.append("slot_index")

        by_id_row = {}
        for key in index_columns + value_columns:
            if key in data:
                by_id_row[key] = data.get(key)

        id_hi, id_lo = id_to_parts(op_id, self.version)

        if self.version == 0:
            datestr_to_timestamp = datestr_to_timestamp_legacy
        else:
            datestr_to_timestamp = date_string_to_timestamp_mcs

        by_id_row["id_hi"] = yson.YsonUint64(id_hi)
        by_id_row["id_lo"] = yson.YsonUint64(id_lo)
        by_id_row["start_time"] = datestr_to_timestamp(data["start_time"])
        by_id_row["finish_time"] = datestr_to_timestamp(data["finish_time"])
        by_id_row["filter_factors"] = get_filter_factors(op_id, data)

        by_start_time_row = {
            "id_hi": by_id_row["id_hi"],
            "id_lo": by_id_row["id_lo"],
            "start_time": by_id_row["start_time"]
        }

        if self.version < 2:
            by_start_time_row["dummy"] = 0
        else:
            by_start_time_row["filter_factors"] = get_filter_factors(op_id, data)
            for key in index_columns:
                if key in data:
                    by_start_time_row[key] = data[key]

        return by_id_row, by_start_time_row

    def get_insert_rows(self, op_id, jobs):
        op_id_hi, op_id_lo = id_to_parts(op_id, self.version)
        rows = []
        for job_id, value in jobs.iteritems():
            job_id_hi, job_id_lo = id_to_parts(job_id, self.version)
            attributes = value.attributes

            row = {}
            row["operation_id_hi"] = yson.YsonUint64(op_id_hi)
            row["operation_id_lo"] = yson.YsonUint64(op_id_lo)
            row["job_id_hi"] = yson.YsonUint64(job_id_hi)
            row["job_id_lo"] = yson.YsonUint64(job_id_lo)
            row["error"] = attributes.get("error")
            if "job_type" not in attributes:
                logger.info("XXX op_id: %s, job_id: %s", op_id, job_id)
            row["type" if self.version >= 6 else "job_type"] = attributes["job_type"]
            row["state"] = attributes["state"]
            row["address"] = attributes["address"]
            row["start_time"] = date_string_to_timestamp_mcs(attributes["start_time"])
            row["finish_time"] = date_string_to_timestamp_mcs(attributes["finish_time"])

            if "stderr" in value:
                self.stderr_queue.put((op_id, job_id))
                stderr = value["stderr"]
                if self.version >= 4:
                    row["stderr_size"] = yson.YsonUint64(stderr.attributes["uncompressed_data_size"])

            rows.append(row)

        return rows

    def do_archive_jobs(self, op_ids):
        responses = self.yt.execute_batch(requests=[{
                "command": "get",
                "parameters": {
                    "path": "//sys/operations/{}/jobs".format(op_id),
                    "attributes": self.ATTRIBUTES
                }
            } for op_id in op_ids])

        archived_op_ids = []
        rows = []
        failed_count = 0
        for op_id, rsp in zip(op_ids, responses):
            if "error" in rsp:
                failed_count += 1
                logger.info("Failed to get jobs for operations %s", op_id)
            else:
                archived_op_ids.append(op_id)
                rows.extend(self.get_insert_rows(op_id, rsp["output"]))

        logger.info("Inserting %d jobs", len(rows))

        try:
            self.yt.insert_rows(JOBS_PATH, rows, update=True)
        except:
            failed_count += len(rows)
            raise
        finally:
            self.metrics.add("failed_to_archive_job_count", failed_count)

        self.metrics.add("archived_job_count", len(rows))
        self.clean_queue.put_many(archived_op_ids)

    def __call__(self, op_ids):
        responses = self.yt.execute_batch(requests=[{
                "command": "get",
                "parameters": {
                    "path": "//sys/operations/{}/@".format(op_id)
                }
            } for op_id in op_ids])

        by_id_rows = []
        by_start_time_rows = []
        archived_op_ids = []
        failed_count = 0
        for op_id, rsp in zip(op_ids, responses):
            if "error" in rsp:
                failed_count += 1
                logger.info("Failed to get attributes of operations %s", op_id)
            else:
                by_id_row, by_start_time_row = self.get_archive_rows(op_id, rsp["output"])
                by_id_rows.append(by_id_row)
                by_start_time_rows.append(by_start_time_row)
                archived_op_ids.append(op_id)

        try:
            self.yt.insert_rows(BY_ID_ARCHIVE_PATH, by_id_rows, update=True)
            self.yt.insert_rows(BY_START_TIME_ARCHIVE_PATH, by_start_time_rows, update=True)
        except:
            failed_count += len(by_id_rows)
            raise
        finally:
            self.metrics.add("failed_to_archive_count", failed_count)

        self.metrics.add("archived_count", len(archived_op_ids))

        if self.archive_jobs:
            self.do_archive_jobs(archived_op_ids)
        else:
            self.clean_queue.put_many(archived_op_ids)

class StderrInserter(object):
    def __init__(self, metrics):
        self.metrics = metrics
        self.yt = yt.YtClient(config=yt.config.config)

    def __call__(self, rowset):
        logger.info("Inserting %d stderrs", len(rowset))

        self.yt.insert_rows(STDERRS_PATH, rowset, update=True)

        self.metrics.add("archived_stderr_count", len(rowset))
        self.metrics.add("archived_stderr_size", sum(len(row["stderr"]) for row in rowset))

class StderrDownloader(object):
    def __init__(self, insert_queue, version, metrics):
        self.insert_queue = insert_queue
        self.version = version
        self.metrics = metrics

    def __call__(self, element):
        (op_id, job_id) = element
        token = get_token()
        proxy_url = get_proxy_url(yt.config.config["proxy"]["url"])
        path = "http://{}/api/v3/read_file?path=//sys/operations/{}/jobs/{}/stderr".format(proxy_url, op_id, job_id)

        rsp = requests.get(path, headers={"Authorization": "OAuth {}".format(token)}, allow_redirects=True, timeout=20)

        if not rsp.content:
            return

        op_id_hi, op_id_lo = id_to_parts(op_id, self.version)
        id_hi, id_lo = id_to_parts(job_id, self.version)

        row = {}
        row["operation_id_hi"] = yson.YsonUint64(op_id_hi)
        row["operation_id_lo"] = yson.YsonUint64(op_id_lo)
        row["job_id_hi"] = yson.YsonUint64(id_hi)
        row["job_id_lo"] = yson.YsonUint64(id_lo)
        row["stderr"] = rsp.content
        self.insert_queue.put(row)

def clean_operations(soft_limit, hard_limit, grace_timeout, archive_timeout, execution_timeout,
                     max_operations_per_user, robots, archive, archive_jobs, thread_count,
                     stderr_thread_count, push_metrics, remove_threshold):

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
        operations = yt.list(
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
        run_batching_queue_workers(consider_queue, JobsCountGetter, thread_count, (operations_with_job_counts,))
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
                (remove_queue, stderr_queue, version, archive_jobs, thread_safe_metrics),
                batch_size=32,
                failed_items=failed_to_archive)
            failed_to_archive.extend(wait_for_queue(archive_queue, "archive_operation", operation_archiving_time_limit))

        if archive_jobs:
            logger.info("Archiving %d stderrs", len(stderr_queue))

            with timers["archiving_stderrs"]:
                insert_queue = NonBlockingQueue()
                run_queue_workers(stderr_queue, StderrDownloader, stderr_thread_count, (insert_queue, version, thread_safe_metrics))
                run_batching_queue_workers(insert_queue, StderrInserter, thread_count, (thread_safe_metrics,))

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
        run_batching_queue_workers(remove_queue, OperationCleaner, thread_count, batch_size=8, failed_items=failed_to_remove)
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
        args.remove_threshold)

if __name__ == "__main__":
    main()
