from yt.wrapper.http_helpers import get_token, get_proxy_url
from yt.common import date_string_to_timestamp_mcs

import yt.logger as logger
import yt.wrapper as yt
import yt.yson as yson
import yt.json as json

from collections import namedtuple
# Import is necessary due to: http://bugs.python.org/issue7980
import _strptime
from datetime import datetime, timedelta

from time import mktime

import yt.packages.requests as requests

OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive"
BY_ID_ARCHIVE_PATH = "{}/ordered_by_id".format(OPERATIONS_ARCHIVE_PATH)
BY_START_TIME_ARCHIVE_PATH = "{}/ordered_by_start_time".format(OPERATIONS_ARCHIVE_PATH)
STDERRS_PATH = "{}/stderrs".format(OPERATIONS_ARCHIVE_PATH)
JOBS_PATH = "{}/jobs".format(OPERATIONS_ARCHIVE_PATH)

Operation = namedtuple("Operation", ["start_time", "finish_time", "id", "user", "state"])

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

class StderrDownloader(object):
    def __init__(self, client, insert_queue, version, metrics=NullMetrics()):
        self.insert_queue = insert_queue
        self.version = version
        self.metrics = metrics
        self.yt = client

    def __call__(self, element):
        op_id, job_id = element
        token = get_token()
        proxy_url = get_proxy_url(self.yt.config.config["proxy"]["url"])
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

