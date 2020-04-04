#!/usr/bin/env python

import yt.wrapper as yt

from copy import deepcopy

def round_time(time):
    "2012-10-19T11:22:58.190448Z"
    time = time.rsplit(".")[0]
    sec = int(time[-1])
    return time[:-1] + str(sec - (sec % 5))

def round(row):
    if row["timestamp"] >= "2014-06-01":
        row["timestamp"] = round_time(row["timestamp"])
        yield row

def get_key(event_type):
    if event_type == "fair_share_info":
        return "residue"
    else:
        return "duration"

def group(key, rows):
    result = deepcopy(key)
    key_name = get_key(key["event_type"])
    result[key_name] = 0.0
    for row in rows:
        result[key_name] += row[key_name]

    yield result

def main():
    yt.run_map_reduce(round, group, "//home/ignat/event_log_filtered", "//home/ignat/event_log_grouped",
                      reduce_by=["timestamp", "event_type", "reason"], format=yt.YsonFormat())
    yt.run_sort("//home/ignat/event_log_grouped", sort_by=["timestamp"])

if __name__ == "__main__":
    main()
