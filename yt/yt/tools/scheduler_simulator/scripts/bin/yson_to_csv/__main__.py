#!/usr/bin/env python3

# WARNING!
# This script assumes that all yson records have the same set of fields.

from yt.tools.scheduler_simulator.scripts.lib import print_info

import yt.yson as yson

import csv

from argparse import ArgumentParser


def main():
    parser = ArgumentParser(description="Convert yson to csv")
    parser.add_argument("input", help="path to input yson file")
    parser.add_argument("output", help="path to output csv file")
    args = parser.parse_args()

    print_info('Input: "{}"'.format(args.input))
    print_info('Output: "{}"'.format(args.output))

    with open(args.input, "rb") as log_input:
        with open(args.output, "wb") as log_output:
            csv_writer = None
            keys = None
            ysonable_keys = ["statistics", "resource_limits"]
            for log_entry in yson.load(log_input, "list_fragment"):
                if csv_writer is None:
                    keys = list(log_entry.keys())
                    csv_writer = csv.DictWriter(log_output, keys)
                    csv_writer.writeheader()
                csv_writer.writerow({
                    key: (log_entry[key] if key not in ysonable_keys else yson.dumps(log_entry[key])) for key in keys
                })


if __name__ == "__main__":
    main()
