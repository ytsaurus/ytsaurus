from __future__ import print_function

from yp.local import (
    DbManager,
    get_db_version,
)

from yt.wrapper import YtClient

import argparse


def main(cluster, yp_path):
    def mapper(row):
        yield row
    yt_client = YtClient(cluster)
    db_version = get_db_version(yt_client, yp_path)
    db_manager = DbManager(yt_client, yp_path, version=db_version)
    db_manager.run_map("pods", mapper)
    db_manager.mount_unmounted_tables()
    db_manager.finalize()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Emulates YP database migration")
    parser.add_argument(
        "--cluster",
        type=str,
        required=True,
        help="YT cluster address",
    )
    parser.add_argument(
        "--yp-path",
        type=str,
        required=True,
        help="Path to yp Cypress node",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_arguments()
    main(arguments.cluster, arguments.yp_path)
