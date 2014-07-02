#!/usr/bin/env python

import sys
import json
from copy import deepcopy
from datetime import datetime

def filter(dict, keys):
    res = deepcopy(dict)
    for key in res.keys():
        if key not in keys:
            del res[key]
    return res

def parse_time(time):
    "2012-10-19T11:22:58.190448Z"
    pattern = "%Y-%m-%dT%H:%M:%S"
    time = time.rsplit(".")[0]
    return datetime.strptime(time, pattern)

def get_residue(obj):
    fair_share = obj["fair_share_ratio"]
    usage = obj["usage_ratio"]
    if usage < fair_share:
        return fair_share - usage
    return 0.0

if __name__ == "__main__":
    for line in sys.stdin:
        obj = json.loads(line)

        if "event_type" not in obj:
            continue

        res = {}
        res["event_type"] = obj["event_type"]
        res["timestamp"] = obj["timestamp"]
        
        if obj["event_type"] in ["job_aborted", "job_completed", "job_failed"]:
            duration = (parse_time(obj["finish_time"]) - parse_time(obj["start_time"])).total_seconds()
            res["duration"] = duration
            if obj["event_type"] == "job_aborted":
                res["reason"] = obj["reason"]
        elif obj["event_type"] == "fair_share_info":
            if not isinstance(obj["operations"].itervalues().next(), dict):
                continue
            residue = sum(sum(map(get_residue, obj[type].itervalues())) for type in ["operations", "pools"])
            res["residue"] = residue
        else:
            continue

        json.dump(res, sys.stdout)
        sys.stdout.write("\n")
