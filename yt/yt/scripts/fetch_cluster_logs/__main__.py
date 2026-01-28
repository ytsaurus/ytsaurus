import argparse
from yt.admin.fetch_cluster_logs import fetch_cluster_logs, add_fetch_cluster_logs_arguments


def main():
    parser = argparse.ArgumentParser(description="Fetch YT cluster logs")
    add_fetch_cluster_logs_arguments(parser)
    parser.set_defaults(func=fetch_cluster_logs)

    args = parser.parse_args()
    args.func(**vars(args))


if __name__ == "__main__":
    main()
