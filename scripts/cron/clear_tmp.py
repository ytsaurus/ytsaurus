#!/usr/bin/env python

import yt.wrapper as yt

from datetime import datetime
import logging

logger = logging.getLogger("Cron")
logger.setLevel(level="INFO")

formatter = logging.Formatter('%(asctime)-15s: %(message)s')
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setFormatter(formatter)

def main():
    "2012-10-19T11:22:58.190448Z"
    pattern = "%Y-%m-%dT%H:%M:%S"
    now = datetime.now()
    for obj in yt.search("//tmp", node_type=["table", "file"], attributes=["creation_time"]):
        time_str = obj.attributes["creation_time"]
        time_str = time_str.rsplit(".")[0]
        if (now - datetime.strptime(time_str, pattern)).days > 7:
            logger.info("Removing %s", obj)
            yt.remove(obj)
    for obj in yt.search("//tmp", node_type=["link_node"], attributes=["broken"]):
        if obj.attributes["broken"] == "true":
            logger.info("Removing %s", obj)
            yt.remove(obj)

if __name__ == "__main__":
    main()

