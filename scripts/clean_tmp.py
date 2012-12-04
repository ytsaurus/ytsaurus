#!/usr/bin/env python

import yt.wrapper as yt

from datetime import datetime

if __name__ == "__main__":
    "2012-10-19T11:22:58.190448Z"
    pattern = "%Y-%m-%dT%H:%M:%S"
    now = datetime.now()
    for obj in yt.search("//tmp", node_type=["table", "file"], attributes=["creation_time"]):
        time_str = obj.attributes["creation_time"]
        time_str = time_str.rsplit(".")[0]
        if (now - datetime.strptime(time_str, pattern)).days > 7:
            print "Removing", obj
            yt.remove(obj)

