import argparse
import os

import yt.wrapper as yt

from yt.microservices.ytmsvc_initializer.id_to_path_updater.init_id_to_path_updater import init_id_to_path_updater


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default="//sys/admin/yt-microservices/node_id_dict/data")
    parser.add_argument("--tablet-cell-bundle", default="default")
    parser.add_argument("--primary-medium", default="default")
    parser.add_argument("--token-env-variable", default="YT_ID_TO_PATH_TOKEN")
    args = parser.parse_args()
    client = yt.YtClient(token=os.environ[args.token_env_var], config=yt.default_config.get_config_from_env())
    init_id_to_path_updater(client, args.path, args.tablet_cell_bundle, args.primary_medium)


if __name__ == "__main__":
    main()
