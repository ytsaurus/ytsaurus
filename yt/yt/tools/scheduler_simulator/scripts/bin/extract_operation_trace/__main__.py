#!/usr/bin/env python3

from yt.tools.scheduler_simulator.scripts.lib import (
    compose_path_templated,
    create_default_parser,
    extract_timeframe,
    format_time,
    has_resource_limits,
    is_job_completion_event,
    is_operation_completion_event,
    is_operation_materialized_event,
    JobInfoExtractorInTimeframe,
    parse_time,
    print_info,
    set_default_config,
    truncate_to_timeframe,
)

import yt.wrapper as yt

import zlib
import pickle
from copy import deepcopy
from functools import partial


class JobInfoExtractor(JobInfoExtractorInTimeframe):

    def process_job_event(self, row):
        if not is_job_completion_event(row) or not has_resource_limits(row):
            return

        if row["event_type"] == "job_aborted" and row["reason"] == "preemption":
            return

        if "job_type" not in row:
            row["job_type"] = "unknown"

        yield {
            "operation_id": row["operation_id"],
            "duration": self.get_duration_in_timeframe(row),
            "resource_limits": row["resource_limits"],
            "job_id": row["job_id"],
            "job_type": row["job_type"],
            "state": row["event_type"].replace("job_", "", 1)}

    def process_operation_event(self, row):
        if is_operation_materialized_event(row):
            row["timestamp"] = parse_time(row["timestamp"])
            row["timestamp"] = max(row["timestamp"], self.lower_time_limit)
            row["timestamp"] = format_time(row["timestamp"])
            yield row
        elif is_operation_completion_event(row):
            truncate_to_timeframe(row, self.lower_time_limit, self.upper_time_limit)
            row["start_time"] = format_time(row["start_time"])
            row["finish_time"] = format_time(row["finish_time"])
            yield row


def group_by_operation(key, rows):
    job_descriptions = []
    operation = None
    prepare_time = None

    for row in rows:
        if "event_type" in row:
            if is_operation_completion_event(row):
                operation = deepcopy(row)
            elif is_operation_materialized_event(row):
                prepare_time = row["timestamp"]
        else:
            job_descriptions.append((
                row["duration"],
                row["resource_limits"]["memory"],
                row["resource_limits"]["cpu"],
                row["resource_limits"]["user_slots"],
                row["resource_limits"]["network"],
                row["job_id"],
                row["job_type"],
                row["state"]))

    if operation is not None and job_descriptions:
        result = deepcopy(operation)
        result["state"] = result["event_type"].replace("operation_", "", 1)
        result["job_descriptions"] = zlib.compress(pickle.dumps(job_descriptions))

        if prepare_time:
            result["start_time"] = prepare_time

        fields = (
            "operation_id",
            "job_descriptions",
            "start_time",
            "finish_time",
            "authenticated_user",
            "spec",
            "operation_type",
            "state",
            "error",
            "in_timeframe")

        yield {field: result[field] for field in fields}


def main():
    set_default_config(yt.config)

    parser = create_default_parser({
        "description": "Extract trace from scheduler logs",
        "name": "scheduler_operations"})

    args = parser.parse_args()
    compose_path = partial(compose_path_templated, args)

    raw_logs = compose_path("raw")
    logs = compose_path("")

    print_info("Input: {}".format(args.input))
    print_info("Raw logs: {}".format(raw_logs))
    print_info("Logs: {}".format(logs))

    lower_limit, upper_limit = extract_timeframe(args)
    print_info("Timeframe: {} UTC - {} UTC".format(lower_limit, upper_limit))

    yt.run_map(
        JobInfoExtractor(lower_limit, upper_limit),
        args.input,
        raw_logs,
        spec={"data_size_per_job": 2 * 1024 ** 3})

    yt.run_map_reduce(
        None,
        group_by_operation,
        raw_logs,
        logs,
        reduce_by="operation_id",
        spec={"reduce_job_io": {"table_writer": {"max_row_weight": 128 * 1024 * 1024}}})


if __name__ == "__main__":
    main()
