#!/usr/bin/env python

import yt.wrapper as yt

from argparse import ArgumentParser

import logging

from dateutil.parser import parse
from datetime import datetime, timedelta

logger = logging.getLogger("Cron")
logger.setLevel(level="INFO")

formatter = logging.Formatter('%(asctime)-15s: %(message)s')
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setFormatter(formatter)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--proxy")
    parser.add_argument("--queue-path")

    parser.add_argument("--minimum-number-of-chunks", type=int, default=10)
    parser.add_argument("--maximum-chunk-size", type=int, default=100 *1024 * 1024)
    parser.add_argument("--minimal-age", type=int, default=0)
    parser.add_argument("--filter-out", action="append")
    parser.add_argument("--print-only", action="store_true", default=False)

    args = parser.parse_args()

    if args.proxy is not None:
        yt.config.set_proxy(args.proxy)

    sizes = []
    tables_to_merge = []

    number_of_chunks = 0

    for table in yt.search("/",
                           node_type="table",
                           attributes=["compressed_data_size", "chunk_count", "modification_time", "suppress_nightly_merge"],
                           subtree_filter=lambda path, obj: not obj.attributes.get("suppress_nightly_merge", False),
                           exclude=args.filter_out):
        chunk_count = int(table.attributes["chunk_count"])
        if chunk_count == 0: continue

        modification_time = parse(table.attributes["modification_time"]).replace(tzinfo=None)
        if  datetime.utcnow() - modification_time < timedelta(hours=args.minimal_age):
            continue

        compressed_data_size = float(table.attributes["compressed_data_size"])
        weight = compressed_data_size / float(chunk_count)
        if weight < args.maximum_chunk_size and chunk_count > args.minimum_number_of_chunks:
            number_of_chunks += chunk_count
            profit = max(0.0, chunk_count - compressed_data_size / (2.0 * args.maximum_chunk_size))
            tables_to_merge.append((profit, str(table)))
            if args.print_only:
                print str(table), chunk_count


    if not args.print_only:
        tables_to_merge = map(lambda x: x[1], sorted(tables_to_merge))
        yt.set(args.queue_path, tables_to_merge)
        logger.info("Number of small chunks: %d", number_of_chunks)

