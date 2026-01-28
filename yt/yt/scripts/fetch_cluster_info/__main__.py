import argparse
import logging
from yt.admin.fetch_cluster_info import add_fetch_cluster_info_arguments


def main():
    parser = argparse.ArgumentParser(description="Fetch various debug info from YT cluster")
    add_fetch_cluster_info_arguments(parser)

    args = parser.parse_args()
    args.func(**vars(args))


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    main()
