#!/usr/bin/env python

import yt.wrapper as yt

from datetime import datetime

import os
import argparse
import logging

logger = logging.getLogger("Cron")
logger.setLevel(level="INFO")

formatter = logging.Formatter('%(asctime)-15s\t{}\t%(message)s'.format(yt.config.http.PROXY))
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setFormatter(formatter)

now = datetime.now()
def get_age(obj):
    "2012-10-19T11:22:58.190448Z"
    pattern = "%Y-%m-%dT%H:%M:%S"

    time_str = obj.attributes["access_time"]
    time_str = time_str.rsplit(".")[0]
    return now - datetime.strptime(time_str, pattern)

def main():
    parser = argparse.ArgumentParser(description='Clean operations from cypress.')
    parser.add_argument('--max-count', type=int, default=50000)
    parser.add_argument('--max-size', type=int, default=50 * 1024 ** 4)
    parser.add_argument('--days', type=int, default=7)
    args = parser.parse_args()

    # collect links
    links = yt.search("//tmp", node_type="link")

    # collect table and files
    objects = []
    for obj in yt.search("//tmp", node_type=["table", "file"], attributes=["access_time", "locks", "hash", "resource_usage"]):
        if any(map(lambda l: l["mode"] in ["exclusive", "shared"], obj.attributes["locks"])):
            continue
        objects.append((get_age(obj), obj))
    objects.sort()

    to_remove = []
    count = 0
    size = 0
    for age, obj in objects:
        count += 1
        size += int(obj.attributes["resource_usage"]["disk_space"])
        # filter object by age, total size and count
        if age.days > args.days or size > args.max_size or count > args.max_count:
            if "hash" in obj.attributes:
                link = os.path.join(os.path.dirname(obj), "hash", obj.attributes["hash"])
                if link in links:
                    to_remove.append(link)
            to_remove.append(obj)

    # log and remove
    for obj in to_remove:
        info = ""
        if hasattr(obj, "attributes"):
            info = "(size=%s) (access_time=%s)" % (obj.attributes["resource_usage"]["disk_space"], obj.attributes["access_time"])
        logger.info("Removing %s %s", obj, info)
        yt.remove(obj, force=True)

    # check broken links
    for obj in yt.search("//tmp", node_type=["link"], attributes=["broken"]):
        if obj.attributes["broken"] == "true":
            logger.warning("Removing broken link %s", obj)
            yt.remove(obj, force=True)

if __name__ == "__main__":
    main()

