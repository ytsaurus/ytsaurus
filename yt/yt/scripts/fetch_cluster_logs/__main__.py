from yt.admin.logs_k8s import _add_logs_k8s_arguments, run_logs_k8s
from yt.admin._experimental import EXPERIMENTAL_HELP_SUFFIX

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch YT cluster logs via Kubernetes API. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    _add_logs_k8s_arguments(parser)

    args = parser.parse_args()
    run_logs_k8s(**vars(args))


if __name__ == "__main__":
    main()
