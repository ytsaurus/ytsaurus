#!/usr/bin/env python

import yt.wrapper as yt
import yt.tools.operations_archive as operations_archive

import argparse

def prepare_tables(proxy):
    yt.config["proxy"]["url"] = proxy
    yt.config["api_version"] = "v3"
    yt.config["proxy"]["header_format"] = "yson"

    operations_archive.create_ordered_by_id_table(operations_archive.BY_ID_ARCHIVE)
    operations_archive.create_ordered_by_start_time_table(operations_archive.BY_START_TIME_ARCHIVE)

def main():
    parser = argparse.ArgumentParser(description="Prepare dynamic tables for operations archive")
    parser.add_argument("--proxy", metavar="PROXY")
    args = parser.parse_args()

    prepare_tables(args.proxy)

if __name__ == "__main__":
    main()

