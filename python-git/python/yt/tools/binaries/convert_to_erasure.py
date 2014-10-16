#!/usr/bin/env python

from yt.tools.convertion_tools import convert_to_erasure

import yt.wrapper as yt
from yt.wrapper.common import die

import sys
import traceback
from argparse import ArgumentParser


def main():
    parser = ArgumentParser()
    parser.add_argument("src")
    parser.add_argument("--dst")
    parser.add_argument("--erasure-codec", required=True)
    parser.add_argument("--compression-codec")
    parser.add_argument("--desired-chunk-size", type=int, default=2 * 1024 ** 3)
    parser.add_argument('--proxy')
    args = parser.parse_args()
        
    if args.proxy is not None:
        yt.config.set_proxy(args.proxy)

    convert_to_erasure(args.src, args.dst, args.erasure_codec, args.compression_codec, args.desired_chunk_size)

if __name__ == "__main__":
    try:
        main()
    except yt.YtError as error:
        die(str(error))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()
