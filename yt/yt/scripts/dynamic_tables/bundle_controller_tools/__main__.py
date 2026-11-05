import yt.wrapper as yt
from yt.admin.bundle_controller import add_bundle_controller_subparsers

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bundle controller cluster management tools.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")

    subparsers = parser.add_subparsers(dest="command", required=True)
    add_bundle_controller_subparsers(subparsers)

    args = parser.parse_args()
    args.func(**vars(args))


if __name__ == "__main__":
    main()
