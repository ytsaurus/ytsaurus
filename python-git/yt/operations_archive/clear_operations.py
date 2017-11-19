from .queues import (Timer, ThreadSafeCounter, NonBlockingQueue,
                     queue_worker, run_workers, run_queue_workers,
                     run_batching_queue_workers, wait_for_queue)

from yt.wrapper.http_helpers import get_token, get_proxy_url
from yt.common import date_string_to_timestamp_mcs, datetime_to_string

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson
import yt.json as json

import yt.packages.requests as requests

from dateutil.parser import parse
from collections import namedtuple, Counter
from itertools import izip
# Import is necessary due to: http://bugs.python.org/issue7980
import _strptime
from datetime import datetime, timedelta

from time import mktime

OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive"
BY_ID_ARCHIVE_PATH = "{}/ordered_by_id".format(OPERATIONS_ARCHIVE_PATH)
BY_START_TIME_ARCHIVE_PATH = "{}/ordered_by_start_time".format(OPERATIONS_ARCHIVE_PATH)
STDERRS_PATH = "{}/stderrs".format(OPERATIONS_ARCHIVE_PATH)
JOBS_PATH = "{}/jobs".format(OPERATIONS_ARCHIVE_PATH)

Operation = namedtuple("Operation", ["start_time", "finish_time", "id", "user", "state"])

def iter_chunks(iterable, size):
   chunk = []
   for item in iterable:
       chunk.append(item)
       if len(chunk) == size:
           yield chunk
           chunk = []
   if chunk:
       yield chunk

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
        str(brief_spec.get("output_table_paths", [""])[0])
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

def get_op_path(op_id):
    return "//sys/operations/{}".format(op_id)

def get_op_new_path(op_id):
    return "//sys/operations/{}/{}".format("%02x" % (long(op_id.split("-")[3], 16) % 256), op_id)

class NullMetrics(object):
    def add(self, *args):
        pass

class JobsCountGetter(object):
    def __init__(self, client, operations_with_job_counts):
        self.operations_with_job_counts = operations_with_job_counts
        self.yt = client

    def __call__(self, operations):
        responses = self.yt.execute_batch(requests=[{
                "command": "get",
                "parameters": {
                    "path": "//sys/operations/{}/jobs/@count".format(op.id)
                }
            } for op in operations])

        for op, rsp in zip(operations, responses):
            self.operations_with_job_counts.append((op, 0 if "error" in rsp else rsp["output"]))

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

    def __init__(self, client, clean_queue, stderr_queue, version, archive_jobs, metrics=NullMetrics()):
        self.clean_queue = clean_queue
        self.stderr_queue = stderr_queue
        self.version = version
        self.archive_jobs = archive_jobs
        self.metrics = metrics
        self.yt = client

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

        if self.version >= 15:
            by_start_time_row["pool"] = by_id_row["brief_spec"]["pool"]

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

    def do_insert_rows(self, path, rows):
        atomicity = "none" if self.version >= 16 else "full"
        self.yt.insert_rows(path, rows, update=True, atomicity=atomicity)

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
            self.do_insert_rows(JOBS_PATH, rows)
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

class StderrDownloader(object):
    def __init__(self, client, insert_queue, version, metrics=NullMetrics()):
        self.insert_queue = insert_queue
        self.version = version
        self.metrics = metrics
        self.yt = client

    def __call__(self, element):
        op_id, job_id = element
        token = get_token()
        proxy_url = get_proxy_url(self.yt.config["proxy"]["url"])
        stderr = ""

        if proxy_url is not None:
            path = "http://{}/api/v3/read_file?path=//sys/operations/{}/jobs/{}/stderr".format(proxy_url, op_id, job_id)

            rsp = requests.get(path, headers={"Authorization": "OAuth {}".format(token)}, allow_redirects=True, timeout=20)

            if not rsp.content:
                return

            stderr = rsp.content
        else:
            stderr = self.yt.read_file("//sys/operations/{}/jobs/{}/stderr".format(op_id, job_id)).read()

        op_id_hi, op_id_lo = id_to_parts(op_id, self.version)
        id_hi, id_lo = id_to_parts(job_id, self.version)

        row = {}
        row["operation_id_hi"] = yson.YsonUint64(op_id_hi)
        row["operation_id_lo"] = yson.YsonUint64(op_id_lo)
        row["job_id_hi"] = yson.YsonUint64(id_hi)
        row["job_id_lo"] = yson.YsonUint64(id_lo)
        row["stderr"] = stderr
        self.insert_queue.put(row)

class StderrInserter(object):
    def __init__(self, client, metrics=NullMetrics()):
        self.metrics = metrics
        self.yt = client

    def __call__(self, rowset):
        logger.info("Inserting %d stderrs", len(rowset))

        self.yt.insert_rows(STDERRS_PATH, rowset, update=True)

        self.metrics.add("archived_stderr_count", len(rowset))
        self.metrics.add("archived_stderr_size", sum(len(row["stderr"]) for row in rowset))

class OperationCleaner(object):
    def __init__(self, client):
        self.yt = client

    def __call__(self, op_ids):
        for op_id in op_ids:
            logger.info("Removing operation %s", op_id)

        requests = []
        for op_id in op_ids:
            for path in (get_op_path(op_id), get_op_new_path(op_id)):
                requests.append(
                    {
                        "command": "remove",
                        "parameters": {
                            "path": path,
                            "recursive": True
                        }
                    })

        responses = self.yt.execute_batch(requests)

        errors = []
        for rsp in responses:
            if "error" not in rsp:
                continue

            error = yt.YtResponseError(rsp["error"])
            if error.is_resolve_error():
                continue

            errors.append(error)

        if errors:
            raise yt.YtError("Failed to remove operations", inner_errors=errors)

class OperationNewCleaner(object):
    def __init__(self, client):
        self.yt = client

    def __call__(self, op_paths):
        for to_remove in iter_chunks(op_paths, 200):
            for op_path in to_remove:
                logger.info("Removing node %s", op_path)

            requests = []
            for op_path in to_remove:
                requests.append(
                    {
                        "command": "remove",
                        "parameters": {
                            "path": "//sys/operations/" + op_path,
                        }
                    })

            responses = self.yt.execute_batch(requests)

            unresolved = 0
            errors = []
            for rsp in responses:
                if "error" not in rsp:
                    continue

                error = yt.YtResponseError(rsp["error"])
                if error.is_resolve_error():
                    unresolved += 1
                    continue

                errors.append(error)

            logger.warning("Number of paths that was failed to resolve: %d", unresolved)
            if errors:
                logger.warning("%s", str(yt.YtError("Failed to remove operation new nodes", inner_errors=errors)))

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

def request_operations_recursive(root_operation_ids, prefixes):
    candidates_to_remove = []

    for prefixes_to_request in iter_chunks(prefixes, 32):
        list_responses = yt.execute_batch([
            {
                "command": "list",
                "parameters": {
                    "path": "//sys/operations/{}".format(prefix)
                }
            }
            for prefix in prefixes_to_request])

        for prefix, response in izip(prefixes_to_request, list_responses):
            if "error" in response:
                error = yt.YtResponseError(response["error"])
                if not error.is_resolve_error():
                    raise
            else:
                for op in response["output"]:
                    # It is important to make additional existance check due to possible races.
                    if op not in root_operation_ids:
                        candidates_to_remove.append((prefix, op))

    exists_responses = []
    for exists_requests in iter_chunks(candidates_to_remove, 200):
        exists_responses += yt.execute_batch([
            {
                "command": "exists",
                "parameters": {
                    "path": "//sys/operations/{}".format(op)
                }
            }
            for prefix, op in exists_requests])

    to_remove = []
    for candidate, response in izip(candidates_to_remove, exists_responses):
        if "error" in response:
            raise yt.YtResponseError(response["error"])
        if not response["output"]:
            to_remove.append("/".join(candidate))

    return NonBlockingQueue(to_remove)

def clear_operations(soft_limit, hard_limit, grace_timeout, archive_timeout, execution_timeout,
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
        "failed_to_archive_stderr_count",
        "removed_count_stale",
        "failed_to_remove_count_stale"]})

    timers = {name: Timer() for name in [
        "getting_job_counts",
        "getting_operations_list",
        "archiving_operations",
        "archiving_stderrs",
        "removing_operations",
        "removing_new_operations"]}

    with timers["getting_operations_list"]:
        prefixes = set(["%02x" % prefix for prefix in xrange(256)])

        operations = client.list(
            "//sys/operations",
            max_size=100000,
            attributes=["state", "start_time", "finish_time", "authenticated_user"])
        operations = [Operation(
            maybe_parse_time(op.attributes.get("start_time", None)),
            maybe_parse_time(op.attributes.get("finish_time", None)),
            str(op),
            op.attributes["authenticated_user"],
            op.attributes["state"])
            for op in operations if str(op) not in prefixes]
        operations.sort(key=lambda op: op.start_time, reverse=True)

        remove_new_queue = request_operations_recursive(set(op.id for op in operations), prefixes)

    failed_to_remove_stale = []
    remove_count_stale = len(remove_new_queue)

    logger.info("Removing %d stale operation nodes", remove_count_stale)
    with timers["removing_new_operations"]:
        run_batching_queue_workers(remove_new_queue, OperationNewCleaner, thread_count, args=(client,), batch_size=8, failed_items=failed_to_remove_stale)
        failed_to_remove_stale.extend(wait_for_queue(remove_new_queue, "remove_operations_new", end_time_limit))

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
        version = client.get("{}/@".format(OPERATIONS_ARCHIVE_PATH)).get("version", 0)

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
            "%s (of %s) stale were removed",
            "%s were retained"]),
        len(operations),
        total_time,
        len(operations_to_consider),
        "archiving" if archive else "removing",
        (now_before_clean - now).total_seconds(),
        remove_count,
        "archived" if archive else "removed",
        (now_after_clean - now_before_clean).total_seconds(),
        remove_count_stale - len(failed_to_remove_stale),
        remove_count_stale,
        number_of_retained_operations)

    logger.info("Times: (%s)", "; ".join(["{}: {}".format(name, str(timer)) for name, timer in timers.iteritems()]))

    metrics["total_time_ms"] = total_time * 1000
    metrics["per_operation_time_ms"] = total_time * 1000 / len(operations) if len(operations) > 0 else 0
    metrics["removed_count"] = remove_count - len(failed_to_remove)
    metrics["removed_count_stale"] = remove_count_stale - len(failed_to_remove_stale)
    metrics["failed_to_remove_count"] = len(failed_to_remove)
    metrics["failed_to_remove_count_stale"] = len(failed_to_remove_stale)

    logger.info("Metrics: (%s)", "; ".join(["{}: {}".format(name, value) for name, value in metrics.iteritems()]))

    if push_metrics:
        for name, timer in timers.iteritems():
            metrics["{}_time_ms".format(name)] = timer.value() * 1000

        cluster_name = client.config["proxy"]["url"].split(".")[0]
        push_to_solomon(metrics, cluster_name, now)

