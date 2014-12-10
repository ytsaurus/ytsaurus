#!/usr/bin/python

import yt.wrapper as yt
from yt.fennel import fennel

import sys
import datetime
import argparse


class FilterAndRemoveMicroseconds(object):
    def __init__(self, date, remove_microseconds):
        self._date = date
        self._remove_microseconds = remove_microseconds

    def __call__(self, item):
        timestamp = item["timestamp"]
        dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        moscow_dt = dt + datetime.timedelta(hours=3)
        if moscow_dt.date() == self._date:
            if self._remove_microseconds:
                iso, microseconds = fennel.normalize_timestamp(timestamp)
                item["timestamp"] = fennel.revert_timestamp(iso, 0)
            yield item


def main():
    parser = argparse.ArgumentParser(description="filter and remove microseconds")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--remove-microseconds", default=False)
    args = parser.parse_args()

    date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
    yt.config.format.TABULAR_DATA_FORMAT = yt.JsonFormat(process_table_index=True)
    yt.run_map(FilterAndRemoveMicroseconds(date, args.remove_microseconds),
               args.input,
               args.output,
               spec={"max_failed_job_count": 1})


if __name__ == "__main__":
    main()
