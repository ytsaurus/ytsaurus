#!/usr/bin/python2

from yt.wrapper.retries import run_with_retries

import yt.logger as logger

import yt.packages.requests as requests
from yt.packages.requests import HTTPError, ConnectionError, Timeout

import yt.wrapper as yt

import simplejson as json
import argparse
from datetime import datetime
from socket import error as SocketError

SOLOMON_PUSH_URL = "http://api.solomon.search.yandex.net/push/json"

PUSH_RETRY_COUNT = 5
PUSH_REQUEST_TIMEOUT = 10

def datetime_to_timestamp(dt):
    return (dt - datetime(1970, 1, 1)).total_seconds()

UTC_NOW = datetime.utcnow()
UTC_NOW_TS = datetime_to_timestamp(UTC_NOW)

def main():
    parser = argparse.ArgumentParser(description="Pushes info about regular process to Solomon. "
                                                 "The script is intended to be used in conjunction with "
                                                 "nightly process watcher script.")
    parser.add_argument("--tasks-root", required=True, help="root path for tasks")
    parser.add_argument("--service", required=True, help="service name")
    args = parser.parse_args()

    cluster = yt.config["proxy"]["url"]
    suffix_start = cluster.find(yt.config["proxy"]["default_suffix"])
    if suffix_start != -1:
        cluster = cluster[:suffix_start]

    sensors = []
    for attribute in ("total_table_count", "task_count"):
        value = yt.get_attribute(args.tasks_root, attribute, None)
        if value is None:
            continue

        logger.info("Metric %s has value %d", attribute, value)

        sensors.append({
            "labels": {"sensor": attribute},
            "ts": UTC_NOW_TS,
            "value": int(value)
        })

    if not sensors:
        logger.info("No data to push, exiting")
        return

    data = {}
    data["commonLabels"] = {
        "project": "yt",
        "service": args.service,
        "host": "none",
        "cluster": cluster,
    }
    data["sensors"] = sensors

    def push_to_solomon():
        headers = {"Content-Type": "application/json"}
        rsp = requests.post(
            SOLOMON_PUSH_URL,
            data=json.dumps(data),
            headers=headers,
            timeout=PUSH_REQUEST_TIMEOUT)
        rsp.raise_for_status()

    run_with_retries(
        push_to_solomon,
        retry_count=PUSH_RETRY_COUNT,
        exceptions=(Timeout, ConnectionError, SocketError, HTTPError))

    logger.info("Successfully pushed data to Solomon")

if __name__ == "__main__":
    main()
