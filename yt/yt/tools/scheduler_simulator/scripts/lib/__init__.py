from yt.wrapper.ypath import ypath_join

import argparse
import sys

from datetime import datetime, timedelta


def print_info(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def parse_time(time_str):
    # "2012-10-19T11:22:58.190448Z"
    return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")


def format_time(time):
    "2012-10-19T11:22:58.190448Z"
    return datetime.strftime(time, "%Y-%m-%dT%H:%M:%S.%fZ")


def parse_args_time(time_str):
    "2012-10-19/11:22:58"
    return datetime.strptime(time_str, "%Y-%m-%d/%H:%M:%S")


def to_seconds(delta):
    return delta.total_seconds()


def extract_timeframe(args):
    # event_log stores time in UTC
    delta_UTC = timedelta(hours=args.delta_with_utc)
    upper_limit = args.finish_time - delta_UTC
    if args.start_time is not None:
        lower_limit = args.start_time - delta_UTC
    else:
        lower_limit = upper_limit - timedelta(hours=args.hours)
    return lower_limit, upper_limit


def create_default_parser(defaults):
    parser = argparse.ArgumentParser(description=defaults["description"])

    parser.add_argument("--input", action="append")
    parser.add_argument("--output_dir", default="//tmp/")
    parser.add_argument("--name", default=defaults["name"])
    parser.add_argument("--hours", type=int, default=12)
    parser.add_argument("--start_time", type=parse_args_time)
    parser.add_argument("--finish_time", type=parse_args_time, default=datetime.now())
    parser.add_argument("--delta_with_utc", type=int, default=3)

    parser.add_argument('--filter_by_timestamp', dest='filter_by_timestamp', action='store_true')
    parser.add_argument('--no_filter_by_timestamp', dest='filter_by_timestamp', action='store_false')
    parser.set_defaults(filter_by_timestamp=True)

    return parser


def compose_path_templated(args, suffix):
    path = ypath_join(args.output_dir, args.name)
    if len(suffix) != 0:
        path = path + "_" + suffix
    return path


class TimeframeEventFilter(object):
    def __init__(self, lower_time_limit, upper_time_limit):
        self.lower_time_limit = lower_time_limit
        self.upper_time_limit = upper_time_limit

    def __call__(self, row):
        start_time = parse_time(row["start_time"]) if "start_time" in row else datetime.min
        finish_time = parse_time(row["finish_time"]) if "finish_time" in row else datetime.max

        # Get all events that overlap with our timeframe.
        return (finish_time > self.lower_time_limit and start_time < self.upper_time_limit)


def truncate_to_timeframe(row, lower_time_limit, upper_time_limit):
    row["start_time"] = parse_time(row["start_time"])
    row["finish_time"] = parse_time(row["finish_time"])

    row["in_timeframe"] = True
    if row["start_time"] < lower_time_limit:
        row["in_timeframe"] = False
        row["start_time"] = lower_time_limit

    if row["finish_time"] > upper_time_limit:
        row["in_timeframe"] = False
        row["finish_time"] = upper_time_limit


def is_job_event(row):
    return row["event_type"].startswith("job_")


def is_operation_event(row):
    return row["event_type"].startswith("operation_")


def is_job_completion_event(row):
    return row["event_type"] not in ["job_started"]


def is_job_aborted_event(row):
    return row["event_type"] == "job_aborted"


def is_operation_completion_event(row):
    return row["event_type"] in ["operation_aborted", "operation_completed", "operation_failed"]


def is_operation_prepared_event(row):
    return row["event_type"] == "operation_prepared"


def is_operation_materialized_event(row):
    return row["event_type"] == "operation_materialized"


def is_operation_started_event(row):
    return row["event_type"] == "operation_started"


def has_resource_limits(row):
    return "resource_limits" in row


class JobInfoExtractorBase(object):
    def __init__(self, event_filter):
        self.event_filter = event_filter

    def __call__(self, row):
        if not self.event_filter(row):
            return

        if is_job_event(row):
            for item in self.process_job_event(row):
                yield item

        if is_operation_event(row):
            for item in self.process_operation_event(row):
                yield item


class JobInfoExtractorInTimeframe(JobInfoExtractorBase):
    def __init__(self, lower_time_limit, upper_time_limit):
        self.lower_time_limit = lower_time_limit
        self.upper_time_limit = upper_time_limit
        super(JobInfoExtractorInTimeframe, self).__init__(
            TimeframeEventFilter(lower_time_limit, upper_time_limit))

    def get_duration_in_timeframe(self, row):
        start_time = max(parse_time(row["start_time"]), self.lower_time_limit)
        finish_time = min(parse_time(row["finish_time"]), self.upper_time_limit)
        return to_seconds(finish_time - start_time)


def time_filter(lower_limit, upper_limit, row):
    timestamp = parse_time(row["timestamp"])
    return lower_limit < timestamp < upper_limit


def set_default_config(yt_config):
    yt_config["pickling"]["module_filter"] = lambda module: "hashlib" not in getattr(module, "__name__", "")
    max_row_weight = 2**27
    yt_config["table_writer"]["max_row_weight"] = max_row_weight
