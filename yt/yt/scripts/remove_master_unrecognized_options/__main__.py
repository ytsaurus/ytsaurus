import argparse

from yt.wrapper import YtClient


def get_unrecognized(master_alerts):
    for alert in master_alerts:
        if alert["message"] == "Found unrecognized options in dynamic cluster config":
            return alert["attributes"]["unrecognized_options"]
    return None


def remove_unrecognized(unrecognized_options, config_path, path_suffix, yt_client, dry):
    if unrecognized_options is None:
        return

    path_to_remove = config_path + path_suffix

    if not isinstance(unrecognized_options, dict):
        print(path_to_remove)
        if not dry:
            yt_client.remove(path_to_remove)
        return

    for attr in unrecognized_options:
        remove_unrecognized(unrecognized_options[attr], config_path, path_suffix + "/" + attr, yt_client, dry)

    if yt_client.get(path_to_remove) == {}:
        print(path_to_remove)
        if not dry:
            yt_client.remove(path_to_remove)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--cluster",
        required=True,
        help="Cluster to remove unrecognized options from")
    parser.add_argument(
        "--dry",
        help="Just print unrecognized options without actually removing them",
        default=False,
        action="store_true")
    parser.add_argument(
        "--do-not-print-config",
        help="Do not print //sys/@config before removing anything (it is printed by default just in case)",
        default=False,
        action="store_true")
    args = parser.parse_args()
    cluster = args.cluster
    dry = args.dry
    do_not_print_config = args.do_not_print_config

    yt_client = YtClient(proxy=cluster)

    if not do_not_print_config:
        config_path = "//sys/@config"
        config = yt_client.get(config_path)
        print("Config before options removal: {}\n\n".format(config))

    master_alerts = yt_client.get("//sys/@master_alerts")

    unrecognized_options = get_unrecognized(master_alerts)
    remove_unrecognized(unrecognized_options, config_path, "", yt_client, dry)


if __name__ == "__main__":
    main()
