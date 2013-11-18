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
    parser.add_argument("--create-merge-queue", action="store_true", default=False)
    parser.add_argument("--calculate-chunk-sizes", action="store_true", default=False)
    parser.add_argument("--queue-path")
    parser.add_argument("--proxy")

    parser.add_argument("--minimum-number-of-chunks", type=int, default=10)
    parser.add_argument("--maximum-chunk-size", type=int, default=100 *1024 * 1024)
    parser.add_argument("--minimal-age", type=int, default=0)
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

        modification_time = parse(table.attributes["modification_time"]).replace(tzinfo=None)
        if  datetime.utcnow() - modification_time < timedelta(args.minimal_age):
            continue

        weight = float(table.attributes["compressed_data_size"]) / float(chunk_count)

        if args.calculate_chunk_sizes:
            sizes += [weight] * chunk_count

        if args.create_merge_queue:
            if weight < args.maximum_chunk_size and chunk_count > args.minimum_number_of_chunks:
                number_of_chunks += chunk_count
                #print table, chunk_count
                tables_to_merge.append(tuple([table.attributes["compressed_data_size"], table]))


    tables_to_merge = map(lambda x: x[1], sorted(tables_to_merge))
    if args.create_merge_queue:
        yt.set(args.queue_path, tables_to_merge)

    if args.calculate_chunk_sizes:
        for size in sizes:
            print size

    if args.create_merge_queue:
        logger.info("Number of small chunks: %d", number_of_chunks)

