#!/usr/bin/env python

from yt_odin.storage.db import init_yt_table

import yt.wrapper as yt

import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--tablet-cell-bundle", default="odin")
    args = parser.parse_args()

    client = yt.YtClient(proxy=args.proxy)
    init_yt_table(args.table, client, bundle=args.tablet_cell_bundle)


if __name__ == "__main__":
    main()
