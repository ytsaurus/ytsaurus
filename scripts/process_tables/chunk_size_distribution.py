#!/usr/bin/env python

import yt.wrapper as yt

from argparse import ArgumentParser

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--create-merge-queue", action="store_true", default=False)
    parser.add_argument("--calculate-chunk-sizes", action="store_true", default=False)

    parser.add_argument("--minimum-number-of-chunks", type=int, default=10)
    parser.add_argument("--maximum-chunk-size", type=int, default=100 *1024 * 1024)
    parser.add_argument("--filter-out", action="append")
    
    args = parser.parse_args()

    sizes = []
    tables_to_merge = []

    for dir in ["//home", "//statbox"]:
        for table in yt.search(dir, node_type="table", attributes=["compressed_data_size", "chunk_count"]):
            if args.filter_out is not None and any(word in table for word in args.filter_out):
                continue
            
            chunk_count = int(table.attributes["chunk_count"])
            if chunk_count == 0: continue

            weight = float(table.attributes["compressed_data_size"]) / float(chunk_count)

            if args.calculate_chunk_sizes:
                sizes += [weight] * chunk_count

            if args.create_merge_queue:
                if weight < args.maximum_chunk_size and chunk_count > args.minimum_number_of_chunks:
                    tables_to_merge.append(tuple([table.attributes["compressed_data_size"], table]))


    tables_to_merge = map(lambda x: x[1], sorted(tables_to_merge))
    if args.create_merge_queue:
        yt.set("//home/ignat/tables_to_merge", tables_to_merge)

    if args.calculate_chunk_sizes:
        for size in sizes:
            print size

