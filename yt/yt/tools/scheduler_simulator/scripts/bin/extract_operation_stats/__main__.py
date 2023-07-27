#!/usr/bin/env python3

from yt.tools.scheduler_simulator.scripts.lib import (
    compose_path_templated,
    create_default_parser,
    extract_timeframe,
    has_resource_limits,
    is_job_completion_event,
    is_operation_completion_event,
    JobInfoExtractorInTimeframe,
    print_info,
    set_default_config,
    to_seconds,
    truncate_to_timeframe,
)

import yt.wrapper as yt

from functools import partial


class JobInfoExtractor(JobInfoExtractorInTimeframe):
    def process_job_event(self, row):
        if not is_job_completion_event(row) or not has_resource_limits(row):
            return

        if row["event_type"] == "job_aborted" and row["reason"] == "preemption":
            row["event_type"] = "job_preempted"

        if "job_type" not in row:
            row["job_type"] = "unknown"

        yield {
            "operation_id": row["operation_id"],
            "duration": self.get_duration_in_timeframe(row),
            "state": row["event_type"].replace("job_", "", 1)}

    def process_operation_event(self, row):
        if not is_operation_completion_event(row):
            return

        row["duration"] = self.get_duration_in_timeframe(row)

        truncate_to_timeframe(row, self.lower_time_limit, self.upper_time_limit)
        row["start_time"] = to_seconds(row["start_time"] - self.lower_time_limit)
        row["finish_time"] = to_seconds(row["finish_time"] - self.lower_time_limit)

        yield row


def group_by_operation(key, rows):
    result = {
        "job_count": 0,
        "preempted_job_count": 0,
        "jobs_total_duration": 0,
        "job_max_duration": 0,
        "preempted_jobs_total_duration": 0}

    for row in rows:
        if "event_type" in row:
            result["id"] = row["operation_id"]
            result["start_time"] = row["start_time"]
            result["finish_time"] = row["finish_time"]
            result["in_timeframe"] = row["in_timeframe"]
            result["real_duration"] = row["duration"]
            result["operation_type"] = row["operation_type"]
            result["operation_state"] = row["event_type"].replace("operation_", "", 1)
        else:
            if row["state"] == "preempted":
                result["preempted_job_count"] += 1
                result["jobs_total_duration"] += row["duration"]
                result["preempted_jobs_total_duration"] += row["duration"]
            elif row["state"] == "aborted":
                result["jobs_total_duration"] += row["duration"]
            else:
                result["job_count"] += 1
                result["jobs_total_duration"] += row["duration"]
                result["job_max_duration"] = max(result["job_max_duration"], row["duration"])

    if "id" in result and result["job_count"]:
        yield result


def main():
    set_default_config(yt.config)

    parser = create_default_parser({
        "description": "Extract operation stats from scheduler logs",
        "name": "scheduler_operation_stats"})

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
