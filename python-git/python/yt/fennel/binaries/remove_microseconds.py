#!/usr/bin/python

import yt.wrapper as yt
from yt.fennel import fennel

import sys
import datetime


class FilterAndRemoveMicroseconds(object):
    def __init__(self, date):
        self._date = date

    def __call__(self, item):
        timestamp = item["timestamp"]
        dt = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        if dt.date() == self._date:
            iso, microseconds = fennel.normalize_timestamp(timestamp)
            item["timestamp"] = fennel.revert_timestamp(iso, 0)
            yield item


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.stderr.write("Usage: {0} table_input table_output\n".format(sys.argv[0]));
    else:
        table_input = sys.argv[1]
        table_output = sys.argv[2]
        date = datetime.datetime.strptime(sys.argv[3], "%Y-%m-%d").date()
        yt.config.format.TABULAR_DATA_FORMAT = yt.JsonFormat(process_table_index=True)
        yt.run_map(FilterAndRemoveMicroseconds(date),
                   table_input,
                   table_output,
                   spec={"max_failed_job_count": 1})
