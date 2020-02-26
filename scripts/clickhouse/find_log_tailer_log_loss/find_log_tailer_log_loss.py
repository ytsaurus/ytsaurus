#!/usr/bin/python

import yt.wrapper as yt
import yt_yson_bindings

import argparse


@yt.with_context
class FindLossReducer(object):
    def __init__(self, lower_limit, upper_limit):
        self.lower_limit = lower_limit
        self.upper_limit = upper_limit

    def __call__(self, key, rows, context):
        info_log_rows = []
        debug_log_rows = []

        for row in rows:
            # Increments in debug in info log tables differ.
            row.pop("increment")

            if self.lower_limit and row["timestamp"] < self.lower_limit:
                continue
            if self.upper_limit and row["timestamp"] > self.upper_limit:
                continue

            if context.table_index == 0:
                info_log_rows.append(row)
            elif context.table_index == 1:
                debug_log_rows.append(row)
            else:
                raise RuntimeError("Unknown table index")

        for row in info_log_rows:
            if row not in debug_log_rows:
                yield row


def find_loss(args):
    yt.config.set_proxy(args.proxy)

    info_log_table = args.info_log_table
    debug_log_table = args.debug_log_table
    output_table = args.output_table

    reducer = FindLossReducer(args.lower_limit, args.upper_limit)
    yt.run_reduce(
        reducer,
        source_table=[info_log_table, debug_log_table],
        destination_table=output_table,
        reduce_by=["job_id_shard", "timestamp", "job_id"])


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    find_loss_subparser = subparsers.add_parser("find-loss", help="find log tailer log loss")
    find_loss_subparser.add_argument("--info-log-table", required=True, help="path to table with info log")
    find_loss_subparser.add_argument("--debug-log-table", required=True, help="path to table with debug log")
    find_loss_subparser.add_argument("--output-table", required=True, help="path to output table")
    find_loss_subparser.add_argument("--lower-limit", help="time lower limit")
    find_loss_subparser.add_argument("--upper-limit", help="time upper limit")
    find_loss_subparser.add_argument("--proxy", help="proxy")
    find_loss_subparser.set_defaults(func=find_loss)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
