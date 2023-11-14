#!/usr/bin/env python

from yt.wrapper.common import update

import subprocess
import argparse
import pickle
import time
import json
import os


def prepare_check_args(config_path, check_name, cluster, secrets):
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
    cluster_overrides = config.get("cluster_overrides", {}).get(cluster, {})
    checks_config = update(config.get("checks", {}), cluster_overrides)
    options = checks_config.get(check_name, {}).get("options", {})
    options["cluster_name"] = cluster
    yt_client_params = dict(proxy=cluster, token=secrets["yt_token"])
    return dict(
        service=check_name,
        task_id=1,
        timestamp=int(time.time()),
        yt_client_params=yt_client_params,
        options=options,
        check_log_server_socket_path=None,
        text_log_server_port=None,
        secrets=secrets)


# TODO: make YT wrapper token-finding code reusable and use it here.
def get_yt_token():
    token_path = os.path.join(os.environ.get("HOME", "~"), ".yt", "token")
    with open(token_path, "r") as f:
        return f.read().strip()


def run_check(check_path, check_args):
    proc_input = pickle.dumps(check_args, protocol=2)
    completed = subprocess.run([check_path], input=proc_input, stdout=subprocess.PIPE)
    try:
        return pickle.loads(completed.stdout)
    except Exception:
        raise ValueError("Failed to parse subprocess stdout as pickled structure: {}".format(repr(completed.stdout)))


def main():
    parser = argparse.ArgumentParser(description="Script to run Odin binary checks standalone.")
    parser.add_argument("--check-path", help="Path to binary check file", required=True)
    parser.add_argument("--config-path", help="Path to json config (yt/odin/checks/config/config.json)",
                        required=True)
    parser.add_argument("--cluster", required=False)
    parser.add_argument("--yt-token", required=False)
    parser.add_argument("--yp-token", required=False)
    parser.add_argument("--solomon-token", required=False)
    args = parser.parse_args()

    if args.yt_token is None:
        yt_token = get_yt_token()
    else:
        yt_token = args.yt_token
    secrets = dict(yt_token=yt_token, yp_token=args.yp_token, solomon_token=args.solomon_token)

    if args.cluster is None:
        cluster = os.environ["YT_PROXY"]
    else:
        cluster = args.cluster

    check_name = os.path.basename(args.check_path)
    check_args = prepare_check_args(args.config_path, check_name, cluster, secrets)
    result = run_check(args.check_path, check_args)
    print("Result:", result)


if __name__ == "__main__":
    main()
