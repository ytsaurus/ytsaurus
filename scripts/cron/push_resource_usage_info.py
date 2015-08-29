#!/usr/bin/env python

from yt.wrapper.client import Yt
import simplejson as json

import argparse
import requests
from datetime import datetime

STATFACE_PUSH_URL = "https://stat.yandex-team.ru/_api/report/data"
REPORT_NAME = "YT/AccountsResourceUsage"

NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_account_statistics(account):
    resource_usage = account.attributes["resource_usage"]
    resource_limits = account.attributes["resource_limits"]

    result = {}
    result["account"] = account
    result["disk_space_in_gb"] = float(resource_usage["disk_space"]) / 1024 ** 3
    result["disk_space_limit_in_gb"] = float(resource_limits["disk_space"]) / 1024 ** 3
    result["node_count"] = resource_usage["node_count"]
    result["node_count_limit"] = resource_limits["node_count"]
    # Only for YT clusters with version >= 17.1
    if "chunk_count" in resource_usage:
        result["chunk_count"] = resource_usage["chunk_count"]
        result["chunk_count_limit"] = resource_limits["chunk_count"]

    if resource_limits["disk_space"] > 0:
        result["disk_space_usage"] = \
                float(resource_usage["disk_space"]) / resource_limits["disk_space"] * 100

    return result

def collect_accounts_data_for_cluster(cluster):
    client = Yt(proxy=cluster)
    result = []
    for account in client.list("//sys/accounts", attributes=["resource_usage", "resource_limits"]):
        account_data = get_account_statistics(account)
        account_data["fielddate"] = NOW
        account_data["cluster"] = cluster
        result.append(account_data)
    return result

def push_cluster_data(accounts_data, headers):
    data = {}
    data["name"] = REPORT_NAME
    data["scale"] = "h"
    data["json_data"] = json.dumps({"values": accounts_data})
    r = requests.post(STATFACE_PUSH_URL, data=data, headers=headers)
    r.raise_for_status()

def main():
    parser = argparse.ArgumentParser(description="Pushes accounts resource usage of various "
                                                 "YT clusters to statface.")

    parser.add_argument("--robot-login", default="robot_asaitgalin")
    parser.add_argument("--robot-password", default="vai4looP0i")
    parser.add_argument("--cluster", required=True, nargs="+", help="clusters list")
    args = parser.parse_args()

    headers = {
        "StatRobotUser": args.robot_login,
        "StatRobotPassword": args.robot_password
    }

    for cluster in args.clusters:
        accounts_data = collect_accounts_data_for_cluster(cluster)
        push_cluster_data(accounts_data, headers)

if __name__ == '__main__':
    main()
