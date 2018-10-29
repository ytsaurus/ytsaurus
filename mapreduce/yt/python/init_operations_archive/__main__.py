#!/usr/bin/python

import argparse

import yt.environment.init_operation_archive
import yt.wrapper

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("yt_address")
    args = parser.parse_args()

    client = yt.wrapper.YtClient()
    client.config["proxy"]["url"] = args.yt_address
    client.config["proxy"]["enable_proxy_discovery"] = False

    yt.environment.init_operation_archive.create_tables_latest_version(client)
