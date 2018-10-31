#!/usr/bin/python

import argparse
import time

import yt.environment.init_operation_archive
import yt.wrapper


def wait(pred):
    for _ in xrange(10):
        if pred():
            return
        time.sleep(1.0)
    raise Exception("Wait failed: timeout (30 sec) expired")


def wait_for_tablet_cell(client):
    def check_cells():
        cell_ids = client.list("//sys/tablet_cells")
        if len(cell_ids) == 0:
            return False
        cell = client.get("//sys/tablet_cells/{}/@".format(cell_ids[0]), attributes=["health"])
        if cell["health"] != "good":
            return False
        return True
    wait(check_cells)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("yt_address")
    args = parser.parse_args()

    client = yt.wrapper.YtClient()
    client.config["proxy"]["url"] = args.yt_address
    client.config["proxy"]["enable_proxy_discovery"] = False

    wait_for_tablet_cell(client)
    yt.environment.init_operation_archive.create_tables_latest_version(client)
