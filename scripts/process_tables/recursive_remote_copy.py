#!/usr/bin/env python

from yt.wrapper.client import Yt
import yt.logger as logger

from argparse import ArgumentParser

def main():
    parser = ArgumentParser()
    parser.add_argument("--src-dir", required=True)
    parser.add_argument("--dst-dir", required=True)
    parser.add_argument("--src-proxy", required=True)
    parser.add_argument("--dst-proxy", required=True)
    parser.add_argument("--src-cluster-name")
    args = parser.parse_args()

    if args.src_cluster_name is None:
        args.src_cluster_name = args.src_proxy.split(".")[0]

    src_yt = Yt(args.src_proxy)
    dst_yt = Yt(args.dst_proxy)
    src_dir = args.src_dir.rstrip("/")
    dst_dir = args.dst_dir.rstrip("/")

    for src_path in src_yt.search(src_dir, attributes=["type"]):
        dst_path = dst_dir + src_path[len(src_dir):]
        type = src_path.attributes["type"]
        logger.info("%s -> %s", src_path, dst_path)
        if type == "table":
            dst_yt.create("table", dst_path, recursive=True, ignore_existing=True)
            dst_yt.run_remote_copy(src_path, dst_path, cluster_name=args.src_cluster_name, network_name="fastbone")
        elif type == "map_node":
            dst_yt.mkdir(str(dst_path), recursive=True)
        else:
            logger.warning("Copying objects of type '%' is not supported", type)

if __name__ == "__main__":
    main()

