#!/usr/bin/env python

import yt.wrapper as yt

from argparse import ArgumentParser

import time
import logging

from dateutil.parser import parse
from datetime import datetime, timedelta

logger = logging.getLogger("Cron")

def configure_logger():
    global logger

    logger.setLevel(level="INFO")
    formatter = logging.Formatter('%(asctime)-15s [%(yt_proxy)s]: %(message)s')
    logger.addHandler(logging.StreamHandler())
    logger.handlers[0].setFormatter(formatter)
    logger = logging.LoggerAdapter(logger, extra={"yt_proxy": yt.config["proxy"]["url"]})

def configure_client():
    command_params = yt.config.get_option("COMMAND_PARAMS", None)
    command_params["suppress_access_tracking"] = True

def main():
    parser = ArgumentParser()
    parser.add_argument("--proxy")
    parser.add_argument("--queue-path")

    parser.add_argument("--root", default="/")
    parser.add_argument("--minimum-number-of-chunks", type=int, default=100)
    parser.add_argument("--maximum-chunk-size", type=int, default=100 *1024 * 1024)
    parser.add_argument("--account")
    parser.add_argument("--minimal-age", type=int, default=0)
    parser.add_argument("--filter-out", action="append", default=["//sys"])
    parser.add_argument("--ignore-suppress-nightly-merge", action="store_true", default=False)
    parser.add_argument("--append", action="store_true", default=False)
    parser.add_argument("--print-only", action="store_true", default=False)

    args = parser.parse_args()

    if args.proxy is not None:
        yt.config.set_proxy(args.proxy)

    configure_logger()
    configure_client()

    logger.info("Finding tables to merge")

    tables_to_merge = []

    number_of_chunks = 0
    number_of_fresh_tables = 0

    if not yt.exists(args.root):
        return

    for table in yt.search(args.root,
                           node_type="table",
                           attributes=["compressed_data_size", "chunk_count", "modification_time", "suppress_nightly_merge", "account"],
                           subtree_filter=lambda path, obj:
                               args.ignore_suppress_nightly_merge or
                               not obj.attributes.get("suppress_nightly_merge", False),
                           exclude=args.filter_out,
                           enable_batch_mode=True):
        chunk_count = int(table.attributes["chunk_count"])
        if chunk_count == 0:
            continue

        account = table.attributes.get("account")
        if args.account is not None and account != args.account:
            continue

        modification_time = parse(table.attributes["modification_time"]).replace(tzinfo=None)

        compressed_data_size = float(table.attributes["compressed_data_size"])
        weight = compressed_data_size / float(chunk_count)
        if weight < args.maximum_chunk_size and chunk_count > args.minimum_number_of_chunks:
            if  datetime.utcnow() - modification_time < timedelta(hours=args.minimal_age):
                number_of_fresh_tables += 1
                continue
            number_of_chunks += chunk_count
            profit = max(0.0, chunk_count - compressed_data_size / (2.0 * args.maximum_chunk_size))
            tables_to_merge.append((profit, str(table)))
            if args.print_only:
                print str(table), chunk_count


    logger.info("Found %d tables to merge (also %d tables need merge but too fresh)", len(tables_to_merge), number_of_fresh_tables)

    if not args.print_only:
        tables_to_merge = map(lambda x: x[1], sorted(tables_to_merge))
        if args.append:
            tables_to_merge = yt.get(args.queue_path) + tables_to_merge
        for index in xrange(5):
            try:
                # Hack to avoid failing merging processes
                yt.set(args.queue_path, tables_to_merge[:45000])
                break
            except yt.YtResponseError:
                time.sleep(5)
        logger.info("Number of small chunks: %d", number_of_chunks)

if __name__ == "__main__":
    main()
