#!/usr/bin/env python

import yt.logger as logger
import yt.wrapper as yt

from argparse import ArgumentParser

def get_compression_ratio(table, codec):
    logger.info("Compress sample of '%s' to calculate compression ratio", table) 
    tmp = yt.create_temp_table()
    yt.set(tmp + "/@compression_codec", codec)
    yt.run_merge(table + "[#1:#10000]", tmp, mode="unordered", spec={"force_transform": "true"})
    ratio = yt.get(table + "/@compression_ratio")
    yt.remove(tmp)
    return ratio

def main():
    parser = ArgumentParser()
    parser.add_argument("src")
    parser.add_argument("--dst")
    parser.add_argument("--erasure-codec", required=True)
    parser.add_argument("--compression-codec")
    parser.add_argument("--desired-chunk-size", type=int, default=2 * 1024 ** 3)
    args = parser.parse_args()
        
    if args.dst is None:
        args.dst = args.src
    
    if args.compression_codec is not None:
        ratio = get_compression_ratio(args.src, args.compression_codec)
        yt.set(args.src + "/@compression_codec", args.compression_codec)
    else:
        ratio = yt.get(args.src + "/@compression_ratio")

    yt.set(args.dst + "/@erasure_codec", args.erasure_codec)

    data_size_per_job = max(1, int(args.desired_chunk_size / ratio))
    mode = "sorted" if yt.is_sorted(args.src) else "unordered"
    
    spec = {"combine_chunks": "true",
            "force_transform": "true",
            "data_size_per_job": data_size_per_job,
            "job_io": {
                "table_writer": {
                    "desired_chunk_size": args.desired_chunk_size
                }
            }}
   
    logger.info("Merge from '%s' to '%s' (mode: '%s', spec: '%s'", args.src, args.dst, mode, spec) 
    yt.run_merge(args.src, args.dst, mode, spec=spec)

if __name__ == "__main__":
    main()
