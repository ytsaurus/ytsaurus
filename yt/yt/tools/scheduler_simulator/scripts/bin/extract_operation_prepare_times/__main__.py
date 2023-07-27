#!/usr/bin/env python3

from yt.tools.scheduler_simulator.scripts.lib import (
    compose_path_templated,
    create_default_parser,
    extract_timeframe,
    format_time,
    is_operation_completion_event,
    is_operation_prepared_event,
    JobInfoExtractorInTimeframe,
    parse_time,
    print_info,
    set_default_config,
    to_seconds,
)

import yt.wrapper as yt

from functools import partial


class JobInfoExtractor(JobInfoExtractorInTimeframe):
    # Empty generator.

    def process_job_event(self, row):
        return
        yield

    def process_operation_event(self, row):
        if is_operation_completion_event(row) or is_operation_prepared_event(row):
            yield row


def group_by_operation(key, rows):
    start_time = None
    finish_time = None
    prepare_time = None

    for row in rows:
        if is_operation_completion_event(row):
            start_time = parse_time(row["start_time"])
            finish_time = parse_time(row["finish_time"])
        if is_operation_prepared_event(row):
            prepare_time = parse_time(row["timestamp"])

    if start_time and finish_time and prepare_time:
        preparation_duration = to_seconds(prepare_time - start_time)
        yield {
            "id": key["operation_id"],
            "preparation_duration": preparation_duration,
            "start_time": format_time(start_time),
            "finish_time": format_time(finish_time),
            "prepare_time": format_time(prepare_time)}


def main():
    set_default_config(yt.config)

    parser = create_default_parser({
        "description": "Extract preparation times of operations from scheduler logs",
        "name": "scheduler_operation_prepare_times"})

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
