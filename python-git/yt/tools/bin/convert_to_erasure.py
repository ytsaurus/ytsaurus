#!/usr/bin/env python

from yt.tools.conversion_tools import convert_to_erasure

import yt.wrapper as yt
from yt.wrapper.cli_helpers import run_main

from argparse import ArgumentParser


def main():
    parser = ArgumentParser()
    parser.add_argument("src")
    parser.add_argument("--dst")
    parser.add_argument("--erasure-codec", required=True)
    parser.add_argument("--compression-codec")
    parser.add_argument("--desired-chunk-size", type=int)
    parser.add_argument('--proxy')
    args = parser.parse_args()
        
    if args.proxy is not None:
        yt.config["proxy"]["url"] = args.proxy

    convert_to_erasure(args.src, args.dst, args.erasure_codec, args.compression_codec, args.desired_chunk_size)

if __name__ == "__main__":
    run_main(main)
