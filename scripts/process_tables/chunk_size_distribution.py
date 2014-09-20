#!/usr/bin/env python

import yt.wrapper as yt

from argparse import ArgumentParser

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--proxy")
    parser.add_argument("--filter-out", action="append")

    args = parser.parse_args()

    if args.proxy is not None:
        yt.config.set_proxy(args.proxy)

    sizes = []
    tables_to_merge = []

    number_of_chunks = 0

    for table in yt.search("/", node_type="table", attributes=["compressed_data_size", "chunk_count", "modification_time"], exclude=args.filter_out):
        chunk_count = int(table.attributes["chunk_count"])
        if chunk_count == 0: continue

        weight = float(table.attributes["compressed_data_size"]) / float(chunk_count)
        sizes += [weight] * chunk_count

    for size in sizes:
        print size
