import yt.wrapper as yt
from yt.admin.describe import _add_cluster_subparser, _add_table_subparser
from yt.admin._experimental import EXPERIMENTAL_HELP_SUFFIX

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch various debug info from YT cluster. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--proxy", type=yt.config.set_proxy)

    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_cluster_subparser(subparsers)
    _add_table_subparser(subparsers)

    args = parser.parse_args()
    args.func(**vars(args))


if __name__ == "__main__":
    main()
