from yt.admin.metrics.cli import _add_validate_subparser, _add_dump_subparser, _add_replay_subparser
from yt.admin._experimental import EXPERIMENTAL_HELP_SUFFIX

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch YT cluster metrics. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    _add_validate_subparser(subparsers)
    _add_dump_subparser(subparsers)
    _add_replay_subparser(subparsers)

    args = parser.parse_args()
    func_args = vars(args)
    func = func_args.pop("func")
    func_args.pop("command", None)
    func(**func_args)


if __name__ == "__main__":
    main()
