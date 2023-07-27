#!/usr/bin/env python3

from yt.tools.scheduler_simulator.scripts.lib import print_info

import yt.yson as yson

from argparse import ArgumentParser

import zlib
import pickle


def decompress_jobs_descriptions(row):
    row["job_descriptions"] = pickle.loads(zlib.decompress(row["job_descriptions"]))
    return row


def main():
    parser = ArgumentParser(description='Decompress jobs descriptions')
    parser.add_argument('--input', default="fair_share_log")
    parser.add_argument('--output', default="fair_share_log_parsed")
    args = parser.parse_args()

    print_info('Input: "{}"'.format(args.input))
    print_info('Output: "{}"'.format(args.output))

    log_input = open(args.input, "rb")
    log_output = open(args.output, "wb")
    pow2 = 1
    for iteration, item in enumerate(yson.load(log_input, "list_fragment")):
        dump = yson.dumps(decompress_jobs_descriptions(item))
        log_output.write(dump + b";\n")
        if iteration >= pow2:
            print_info("{} items processed".format(iteration))
            pow2 *= 2
    log_output.close()


if __name__ == "__main__":
    main()
