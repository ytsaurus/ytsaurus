#!/usr/bin/python

import argparse
import time

import yt.environment.init_operation_archive
import yt.wrapper

def wait(condition, timeout=30):
    start_time = time.time()
    while not condition():
        time.sleep(0.1)
        if time.time() - start_time > timeout:
            raise Exception("wait timeout ({}) exceeded".format(timeout))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("yt_address")
    args = parser.parse_args()

    client = yt.wrapper.YtClient()
    client.config["proxy"]["url"] = args.yt_address
    client.config["proxy"]["enable_proxy_discovery"] = False

    cell_id = client.create("tablet_cell", attributes={"size": 1})
    wait(lambda: client.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")

    yt.environment.init_operation_archive.create_tables_latest_version(client)
