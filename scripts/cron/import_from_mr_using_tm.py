#!/usr/bin/env python

from yt.wrapper.http import get_token
from yt.wrapper.cli_helpers import die
from yt.wrapper.client import Yt
import yt.packages.requests as requests

import yt.logger as logger
import yt.wrapper as yt

import sys
import traceback
import simplejson as json
from argparse import ArgumentParser

def get_transfer_manager_tasks(tm_url, params):
    return requests.get("http://{0}/tasks/".format(tm_url), params=params).json()

def start_transfer_manager_task(tm_url, source_cluster, destination_cluster, src, dst, params, headers):
    params["source_cluster"] = source_cluster
    params["destination_cluster"] = destination_cluster
    params["source_table"] = src
    params["destination_table"] = dst
    
    rsp = requests.post("http://{0}/tasks/".format(tm_url), data=json.dumps(params), headers=headers)
    if not str(rsp.status_code).startswith("2"):
        logger.info("Task result: " + rsp.content)
        message = rsp.json()["message"]
        if "Precheck" in message and "failed" in message:
            return False
        rsp.raise_for_status()
    return True

def main():
    parser = ArgumentParser()
    parser.add_argument("--tables-queue", help="YT path to list with tables")
    parser.add_argument("--destination-dir", required=True)
    parser.add_argument("--source-cluster", required=True)
    parser.add_argument("--destination-cluster", required=True)
    parser.add_argument("--copy-pool", required=True)
    parser.add_argument("--postprocess-pool", required=True)
    parser.add_argument("--transfer-manager-url", required=True)
    args = parser.parse_args()

    # TODO(ignat): Support force reimport for tables
    tasks = get_transfer_manager_tasks(args.transfer_manager_url,
                                       params={
                                           "fields[]": [
                                               "state",
                                               "destination_table",
                                               "destination_cluster"
                                           ]
                                       })
    importing_tables = [task["destination_table"] for task in tasks 
                        if (task["destination_cluster"] == args.destination_cluster and
                            task["state"] in ["pending", "running", "completed"])]

    destination_cluster = yt.config["proxy"]["url"].split(".")[0]
    yt_client = Yt(yt.config["proxy"]["url"], token=yt.config["token"])
    queue = yt_client.get(args.tables_queue)
    remaining_objects = []
    for object in queue:
        assert isinstance(object, dict)
        src = object["src"]
        dst = object["dst"]
        if dst in importing_tables:
            logger.info("Task to import %s to %s is already in transfer-manager, ignoring creation of new task", src, dst)
            continue
        if yt_client.exists(dst) and (yt_client.get(dst + "/@row_count") > 0 or yt_client.get(dst + "/@locks")):
            logger.info("Destination table is exist and non-empty, ignoring creation of new task", src, dst)
            continue
        params = {
            "pool": args.copy_pool,
            "copy_spec": {"pool": args.copy_pool},
            "postprocess_spec": {"pool": args.postprocess_pool},
            "mr_user": "userdata",
            "destination_compression_codec": "gzip_best_compression",
            "destination_erasure_codec": "lrc_12_2_2"
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": "OAuth " + get_token(yt_client)
        }
        result = start_transfer_manager_task(
            args.transfer_manager_url, args.source_cluster, destination_cluster, src, dst,
            params=params, headers=headers)
        if not result:
            remaining_objects.append(object)

    yt_client.set(args.tables_queue, [])

if __name__ == "__main__":
    try:
        main()
    except yt.YtError as error:
        die(str(error))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()

