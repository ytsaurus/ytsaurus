#!/usr/bin/env python3

from yt.tools.scheduler_simulator.scripts.lib import (
    is_job_event, is_job_completion_event, is_operation_event, is_operation_completion_event,
    parse_time, set_default_config, create_default_parser, extract_timeframe, print_info,
    compose_path_templated)

from yt.wrapper.ypath import ypath_join
import yt.wrapper as yt

import re
from functools import partial
from datetime import timedelta


class TransactionRetryException(Exception):
    pass


def get_log_files(table_name, attributes):
    pattern = re.compile(table_name + r"(\.\d+)?$")
    return [obj
            for obj in yt.list("", attributes=["type"] + attributes)
            if obj.attributes.get("type") == "table"
            and pattern.match(str(obj))]


def get_log_file_id(log_file):
    if "." not in log_file:
        return 0
    else:
        return int(log_file.split(".")[1])


def lock_log_files(table_name):
    print_info("Trying to lock event log files")
    yt.lock("", mode="snapshot")

    log_objects = get_log_files(table_name, ["revision"])

    batch_client = yt.create_batch_client()
    for log_obj in log_objects:
        yt.lock(str(log_obj), mode="snapshot", client=batch_client)
    batch_client.commit_batch()

    if get_log_files(table_name, attributes=["revision"]) != log_objects:
        raise TransactionRetryException("Failed to lock event log files")
    print_info("Successfully locked event log files")

    log_files = map(str, log_objects)
    return sorted(log_files, key=get_log_file_id)


def is_completion_event(row):
    return (is_job_event(row) and is_job_completion_event(row) or
            is_operation_event(row) and is_operation_completion_event(row))


def create_extract_entries_mapper(filters):
    def mapper(row):
        if not all(predicate(row) for predicate in filters):
            return
        yield yt.create_table_switch(1 if row["event_type"] == "nodes_info" else 0)
        yield row

    return mapper


def create_filter_mapper(filters):
    def mapper(row):
        if not all(predicate(row) for predicate in filters):
            return
        yield row

    return mapper


def create_nodes_info_cleanup_mapper(node_predicate):
    def mapper(row):
        row["nodes"] = {node_address: node_info
                        for node_address, node_info in row["nodes"].items()
                        if node_predicate(node_address)}
        yield row

    return mapper


def create_time_filter(lower_limit, upper_limit, completion_events_upper_limit):
    def predicate(row):
        timestamp = parse_time(row["timestamp"])
        return (lower_limit <= timestamp <= upper_limit or
                is_completion_event(row) and lower_limit <= timestamp <= completion_events_upper_limit)

    return predicate


def create_event_type_filter(allowed_event_types):
    def predicate(row):
        return row["event_type"] in allowed_event_types

    return predicate


def create_node_predicate(args, nodes_info):
    allow_node_regexes = map(re.compile, args.allow_node_regexes)
    prohibit_node_regexes = map(re.compile, args.prohibit_node_regexes)
    prohibit_node_tags = set(args.prohibit_node_tags)

    prohibited_nodes = set(node_address for node_address, node_info in nodes_info.items()
                           if any(tag in prohibit_node_tags for tag in node_info["tags"]))
    allowed_nodes = set(nodes_info) - prohibited_nodes

    def predicate(node_address):
        if node_address in allowed_nodes or any(regex.match(node_address) for regex in allow_node_regexes):
            return True
        if node_address in prohibited_nodes or any(regex.match(node_address) for regex in prohibit_node_regexes):
            return False

        raise ValueError("Unknown node: '{}'".format(node_address))

    return predicate


def create_node_filter(node_predicate):
    def predicate(row):
        return "node_address" not in row or node_predicate(row["node_address"])

    return predicate


def create_user_filter(prohibited_users):
    def predicate(row):
        return "authenticated_user" not in row or row["authenticated_user"] not in prohibited_users

    return predicate


def read_first_row(table_name, columns=None):
    gen = yt.read_table(yt.TablePath(table_name, exact_index=0, columns=columns))
    return next(gen)


def read_first_timestamp(log_file):
    return parse_time(read_first_row(log_file, columns=["timestamp"])["timestamp"])


# returns the index of the first element that does not satisfy the predicate
def upper_bound(list, predicate):
    if not list:
        raise ValueError("upper_bound in empty list")
    if not predicate(list[0]):
        return 0
    if predicate(list[-1]):
        raise ValueError("all values satisfy the predicate")

    step = 1
    while step < len(list) and predicate(list[step]):
        step *= 2

    cur = 0
    step /= 2
    while step > 0:
        if cur + step < len(list) and predicate(list[cur + step]):
            cur += step
        step /= 2

    return cur + 1


def find_first_log(log_files, upper_limit):
    try:
        return upper_bound(log_files, lambda log_file: read_first_timestamp(log_file) > upper_limit)
    except ValueError:
        raise ValueError("invalid upper limit")


def find_last_log(log_files, lower_limit):
    try:
        return upper_bound(log_files, lambda log_file: read_first_timestamp(log_file) > lower_limit)
    except ValueError:
        raise ValueError("invalid lower limit")


def find_input_tables(args):
    input_log_files = lock_log_files(args.table_name)
    if not input_log_files:
        raise ValueError("No event log files found")

    print_info("Searching for event log files for the extended timeframe: {} UTC - {} UTC".format(
        args.extended_lower_limit, args.completion_events_upper_limit))
    first_log_idx = find_first_log(input_log_files, args.completion_events_upper_limit)
    last_log_idx = find_last_log(input_log_files, args.extended_lower_limit)

    input_log_files = input_log_files[first_log_idx:last_log_idx + 1]
    for log_file in input_log_files:
        print_info("Input table: {}".format(ypath_join(yt.config["prefix"], log_file)))
        print_info("First row timestamp: {}".format(read_first_timestamp(log_file)))

    return input_log_files


def gather_nodes_info(nodes_info_logs_path):
    row_count = yt.get_attribute(nodes_info_logs_path, "row_count")
    ranges = [
        {
            "lower_limit": {"row_index": 0},
            "upper_limit": {"row_index": min(5, row_count)},
        },
        {
            "lower_limit": {"row_index": max(row_count // 2 - 5, 0)},
            "upper_limit": {"row_index": min(row_count // 2 + 5, row_count)},
        },
        {
            "lower_limit": {"row_index": max(row_count - 5, 0)},
            "upper_limit": {"row_index": row_count}
        },
    ]
    log_rows = yt.read_table(yt.TablePath(
        nodes_info_logs_path,
        sorted_by=["timestamp"],
        ranges=ranges,
        columns=["nodes"]))

    nodes_info = {}
    for log_row in log_rows:
        for node_address, node_attributes in log_row["nodes"].items():
            if node_address not in nodes_info:
                nodes_info[node_address] = node_attributes

    # Enrich the information from logs with the information from //sys/cluster_nodes.
    for node_address, node_object in yt.get("//sys/cluster_nodes", attributes=["tags"]).items():
        if node_address not in nodes_info:
            nodes_info[node_address] = node_object.attributes

    return nodes_info


def extract_event_log(args):
    compose_path = partial(compose_path_templated, args)

    with yt.Transaction():
        input_log_files = find_input_tables(args)

        default_raw_logs = compose_path("default_raw")
        nodes_info_raw_logs = compose_path("nodes_info_raw")
        raw_logs = [default_raw_logs, nodes_info_raw_logs]

        default_logs = compose_path("default")
        nodes_info_logs = compose_path("nodes_info")
        logs = [default_logs, nodes_info_logs]

        print_info("Extracting log entries")
        print_info("Raw logs: {}".format(raw_logs))
        print_info("Logs: {}".format(logs))

        yt.run_map(
            create_extract_entries_mapper(
                filters=[
                    create_time_filter(args.extended_lower_limit,
                                       args.extended_upper_limit,
                                       args.completion_events_upper_limit),
                    create_event_type_filter(set(args.event_type)),
                    create_user_filter(set(args.exclude_users))
                ],
            ),
            source_table=input_log_files,
            destination_table=raw_logs,
            memory_limit=2 * 1024 ** 3)

        yt.run_sort(nodes_info_raw_logs, sort_by="timestamp")
        nodes_info = gather_nodes_info(nodes_info_raw_logs)
        node_predicate = create_node_predicate(args, nodes_info)

        yt.run_map(
            create_filter_mapper(filters=[create_node_filter(node_predicate)]),
            source_table=default_raw_logs,
            destination_table=default_logs,
            memory_limit=2 * 1024 ** 3)

        yt.run_map(
            create_nodes_info_cleanup_mapper(node_predicate),
            source_table=nodes_info_raw_logs,
            destination_table=nodes_info_logs,
            memory_limit=2 * 1024 ** 3)


def main():
    set_default_config(yt.config)

    parser = create_default_parser({
        "description": "Extract and filter event log entries",
        "name": "scheduler_event_log"})
    parser.add_argument("--table_directory", default="//sys/scheduler")
    parser.add_argument("--table_name", default="event_log")
    parser.add_argument("--hours_for_completion", default=10, type=int)
    parser.add_argument("--hours_extension", default=1, type=int)
    parser.add_argument("--retry_count", default=3, type=int)
    parser.add_argument("--event_type", default=["operation_completed", "operation_aborted", "operation_failed",
                                                 "job_completed", "job_aborted", "job_failed",
                                                 "operation_prepared", "operation_materialized",
                                                 "fair_share_info", "nodes_info", "operation_started"])
    parser.add_argument("--exclude_users", default=[], nargs="*")
    parser.add_argument("--prohibit_node_tags", default=[], nargs="*")
    parser.add_argument("--allow_node_regexes", default=[], nargs="*")
    parser.add_argument("--prohibit_node_regexes", default=[], nargs="*")

    args = parser.parse_args()

    args.lower_limit, args.upper_limit = extract_timeframe(args)
    args.extended_lower_limit = args.lower_limit - timedelta(hours=args.hours_extension)
    args.extended_upper_limit = args.upper_limit + timedelta(hours=args.hours_extension)
    args.completion_events_upper_limit = args.extended_upper_limit + timedelta(hours=args.hours_for_completion)

    yt.config["prefix"] = ypath_join(args.table_directory, "")
    yt.config["read_retries"]["allow_multiple_ranges"] = True

    print_info("Timeframe: {} UTC - {} UTC".format(args.lower_limit, args.upper_limit))

    retry_num = 0
    while True:
        try:
            extract_event_log(args)
            break
        except TransactionRetryException as retryException:
            print_info(retryException.message)
            if retry_num == args.retry_count:
                raise Exception("Maximum number of retries exceeded ({}). Aborting".format(args.retry_count))
            else:
                retry_num += 1
                print_info("Retry {} / {}".format(retry_num, args.retry_count if args.retry_count >= 0 else "inf"))


if __name__ == "__main__":
    main()
