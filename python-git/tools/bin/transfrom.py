#!/usr/bin/env python

from yt.tools.conversion_tools import transform

import yt.wrapper as yt
from yt.wrapper.cli_helpers import run_main

from argparse import ArgumentParser

def main():
    parser = ArgumentParser()
    parser.add_argument("src")
    parser.add_argument("--dst")
    parser.add_argument("--erasure-codec")
    parser.add_argument("--compression-codec")
    parser.add_argument("--desired-chunk-size", type=int)
    parser.add_argument('--proxy')
    parser.add_argument("--check-codecs", action="store_true", default=False,
                        help="If enabled than do nothing if codecs of table remain unchanged")
    args = parser.parse_args()
        
    if args.proxy is not None:
        yt.config["proxy"]["url"] = args.proxy

    transform(args.src, args.dst, args.erasure_codec, args.compression_codec, args.desired_chunk_size, check_codecs=args.check_codecs)

if __name__ == "__main__":
    run_main(main)
