#!/usr/bin/env python

from yt.wrapper.client import Yt
from yt.wrapper.http_helpers import get_retriable_errors
from yt.common import YtError
from yt.wrapper.common import GB

from yt.packages.six.moves import xrange
import yt.packages.requests as requests


import simplejson as json
import logging
import argparse
import time
import os
import sys
from datetime import datetime
from copy import deepcopy

from socket import error as SocketError
from requests import HTTPError, ConnectionError, Timeout

STATFACE_PUSH_URL = "https://upload.stat.yandex-team.ru/_api/report/data"
SOLOMON_PUSH_URL = "http://api.solomon.search.yandex.net/push/json"

PUSH_RETRIES_COUNT = 5
PUSH_REQUEST_TIMEOUT = 20000

STATFACE_REPORT_NAME = "YT/AccountsResourceUsage"

def datetime_to_timestamp(dt):
    return (dt - datetime(1970, 1, 1)).total_seconds()

TS = time.time()
NOW = datetime.fromtimestamp(int(TS - (TS % 600)))
NOW_STR = NOW.strftime("%Y-%m-%d %H:%M:%S")

UTC_NOW = datetime.utcnow()
UTC_NOW_TS = datetime_to_timestamp(UTC_NOW)

def get_account_statistics(account):
    resource_usage = account.attributes["resource_usage"]
    resource_limits = account.attributes["resource_limits"]

    result = {}
    result["account"] = account

    if "disk_space_per_medium" not in resource_limits:
        result["disk_space_in_gb"] = float(resource_usage["disk_space"]) / GB
        result["disk_space_limit_in_gb"] = float(resource_limits["disk_space"]) / GB
    else:
        result["disk_space_in_gb"] = {}
        result["disk_space_limit_in_gb"] = {}

        for medium in resource_limits["disk_space_per_medium"]:
            result["disk_space_in_gb"][medium] = float(resource_usage["disk_space_per_medium"][medium]) / GB
            result["disk_space_limit_in_gb"][medium] = float(resource_limits["disk_space_per_medium"][medium]) / GB

    result["node_count"] = resource_usage["node_count"]
    result["node_count_limit"] = resource_limits["node_count"]
    result["chunk_count"] = resource_usage["chunk_count"]
    result["chunk_count_limit"] = resource_limits["chunk_count"]

    if "tablet_static_memory" in resource_limits:
        result["tablet_static_memory_in_gb"] = float(resource_usage["tablet_static_memory"]) / GB
        result["tablet_static_memory_limit_in_gb"] = float(resource_limits["tablet_static_memory"]) / GB

    if "tablet_count" in resource_limits:
        result["tablet_count"] = resource_usage["tablet_count"]
        result["tablet_count_limit"] = resource_limits["tablet_count"]

    return result

def collect_accounts_data_for_cluster(cluster, request_retry_enable):
    client = Yt(proxy=cluster, config={"proxy": {"request_retry_enable": request_retry_enable}})
    result = []
    for account in client.list("//sys/accounts", attributes=["resource_usage", "resource_limits"]):
        account_data = get_account_statistics(account)
        result.append(account_data)
    return result

def push_data_with_retries(url, data, headers):
    for attempt in xrange(PUSH_RETRIES_COUNT):
        request_start_time = datetime.now()
        try:
            r = requests.post(url, data=data, headers=headers, timeout=PUSH_REQUEST_TIMEOUT)
            r.raise_for_status()
            return
        except (Timeout, ConnectionError, HTTPError, SocketError) as error:
            if attempt + 1 == PUSH_RETRIES_COUNT:
                raise
            logging.warning('HTTP POST request (url: %s) failed with error %s, message: "%s"',
                url, str(type(error)), error.message)
            now = datetime.now()
            backoff = max(0.0, PUSH_REQUEST_TIMEOUT / 1000.0 - (now - request_start_time).total_seconds())
            if backoff:
                logging.warning("Sleep for %.2lf seconds before next retry", backoff)
                time.sleep(backoff)
            logging.warning("New retry (%d) ...", attempt + 2)

def convert_data_to_statface_format(cluster, accounts_data):
    converted_accounts_data = deepcopy(accounts_data)
    for account_data in converted_accounts_data:
        # NOTE: Only default medium is supported in Statface.
        if isinstance(account_data["disk_space_in_gb"], dict):
            account_data["disk_space_in_gb"] = account_data["disk_space_in_gb"]["default"]
            account_data["disk_space_limit_in_gb"] = account_data["disk_space_limit_in_gb"]["default"]
        account_data["fielddate"] = NOW_STR
        account_data["cluster"] = cluster
    return converted_accounts_data

def push_cluster_data_to_statface(cluster, accounts_data, headers):
    data = {}
    data["name"] = STATFACE_REPORT_NAME
    data["scale"] = "i"
    data["json_data"] = json.dumps({
        "values": convert_data_to_statface_format(cluster, accounts_data)
    })
    push_data_with_retries(STATFACE_PUSH_URL, data, headers)

def make_sensor(value, **labels):
    sensor = {
        "ts": UTC_NOW_TS,
        "value": value,
        "labels": deepcopy(labels)
    }
    return sensor

def convert_data_to_solomon_format(accounts_data):
    sensors = []
    for account_data in accounts_data:
        account = account_data["account"]
        for key, value in account_data.items():
            if key == "account":
                continue

            if key in ("disk_space_in_gb", "disk_space_limit_in_gb") and isinstance(value, dict):
                for medium in value:
                    sensors.append(make_sensor(value[medium], account=str(account), sensor=key, medium=medium))
                # COMPAT: Default medium is also added as separate sensor without any additional tags.
                sensors.append(make_sensor(value["default"], account=str(account), sensor=key))
                continue

            sensors.append(make_sensor(value, account=str(account), sensor=key))

    return sensors

def push_cluster_data_to_solomon(cluster, accounts_data, headers):
    data = {}
    data["commonLabels"] = {
        "project": "yt",
        "cluster": cluster,
        "service": "accounts",
        "host": "none"
    }
    data["sensors"] = convert_data_to_solomon_format(accounts_data)
    push_data_with_retries(SOLOMON_PUSH_URL, json.dumps(data), headers)

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s\t%(levelname)s\t%(message)s")

    parser = argparse.ArgumentParser(description="Pushes accounts resource usage of various "
                                                 "YT clusters to statface and solomon.")

    parser.add_argument("--robot-login", default=os.environ.get("STATFACE_ROBOT_LOGIN"))
    parser.add_argument("--robot-password", default=os.environ.get("STATFACE_ROBOT_PASSWORD"))
    parser.add_argument("--robot-password-path", default=os.environ.get("STATFACE_ROBOT_PASSWORD_PATH"))
    parser.add_argument("--clusters-config-url", default="http://yt.yandex.net/config.json",
                        help="url to json with all available clusters")
    parser.add_argument("--push-to-statface", action="store_true", default=False)
    parser.add_argument("--push-to-solomon", action="store_true", default=False)
    args = parser.parse_args()

    if not (args.push_to_statface or args.push_to_solomon):
        print >>sys.stderr, "Push destination is not specified"
        sys.exit(1)

    request_retry_enable = False

    if args.robot_password is None and args.robot_password_path is not None:
        args.robot_password = open(args.robot_password_path).read().strip()

    if args.push_to_statface:
        if args.robot_login is None or args.robot_password is None:
            print >>sys.stderr, "Statface credentials are not set correctly"
            sys.exit(1)

        statface_headers = {
            "StatRobotUser": args.robot_login,
            "StatRobotPassword": args.robot_password
        }

        request_retry_enable = True

    if args.push_to_solomon:
        solomon_headers = {
            "Content-Type": "application/json"
        }

    logging.info("Retrieving clusters configuration from %s", args.clusters_config_url)
    clusters_configuration = requests.get(args.clusters_config_url).json()

    clusters = [name for name, value in clusters_configuration.iteritems()
                if value["type"] != "closing"]

    logging.info("Fetching accounts info from %d clusters", len(clusters))

    exceptions = tuple(list(get_retriable_errors()) + [YtError])

    for cluster in clusters:
        logging.info("Fetching accounts info from %s", cluster)
        try:
            accounts_data = collect_accounts_data_for_cluster(cluster, request_retry_enable)
            if args.push_to_statface:
                push_cluster_data_to_statface(cluster, accounts_data, statface_headers)
            if args.push_to_solomon:
                push_cluster_data_to_solomon(cluster, accounts_data, solomon_headers)
        except exceptions:
            logging.exception("Failed to fetch account info from %s", cluster)

    logging.info("Done")


if __name__ == '__main__':
    main()
