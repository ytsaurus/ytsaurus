#!/usr/bin/env python

from yt_odin.storage.db import init_yt_table

import yt.wrapper as yt

import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--tablet-cell-bundle", default="odin")
    parser.add_argument("--primary-medium", default="default")
    args = parser.parse_args()

    client = yt.YtClient(proxy=args.proxy, token=args.token)
    init_yt_table(args.table, client, bundle=args.tablet_cell_bundle, primary_medium=args.primary_medium)


if __name__ == "__main__":
    main()
