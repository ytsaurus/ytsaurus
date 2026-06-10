import yt.wrapper as yt
from yt.admin.remove_master_unrecognized_options import (
    _add_remove_master_unrecognized_options_arguments,
    run_remove_master_unrecognized_options,
)

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove unrecognized options from master dynamic config.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--proxy")
    _add_remove_master_unrecognized_options_arguments(parser)

    args = parser.parse_args()
    kwargs = vars(args)
    client = yt.YtClient(proxy=kwargs.pop("proxy"))
    run_remove_master_unrecognized_options(client=client, **kwargs)


if __name__ == "__main__":
    main()
