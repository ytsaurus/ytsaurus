import yt.logger as logger
import yt.wrapper as yt

import argparse
from typing import Any

SYS_CONFIG_PATH = "//sys/@config"
UNRECOGNIZED_OPTIONS_ALERT_MESSAGE = "Found unrecognized options in dynamic cluster config"


def get_unrecognized(master_alerts: list) -> Any:
    for alert in master_alerts:
        if alert["message"] == UNRECOGNIZED_OPTIONS_ALERT_MESSAGE:
            return alert["attributes"]["unrecognized_options"]
    return None


def remove_unrecognized(unrecognized_options: Any, path_suffix: str, dry: bool, client=None) -> None:
    if unrecognized_options is None:
        return

    path_to_remove = SYS_CONFIG_PATH + path_suffix

    if not isinstance(unrecognized_options, dict):
        logger.info("Would remove %s" if dry else "Removing %s", path_to_remove)
        if not dry:
            yt.remove(path_to_remove, client=client)
        return

    for attr in unrecognized_options:
        remove_unrecognized(unrecognized_options[attr], path_suffix + "/" + attr, dry, client=client)

    if yt.get(path_to_remove, client=client) == {}:
        logger.info("Would remove %s" if dry else "Removing %s", path_to_remove)
        if not dry:
            yt.remove(path_to_remove, client=client)


def run_remove_master_unrecognized_options(dry, do_not_print_config, client=None, **_) -> None:
    if not do_not_print_config:
        config = yt.get(SYS_CONFIG_PATH, client=client)
        logger.info("Config before options removal: %s", config)

    master_alerts = yt.get("//sys/@master_alerts", client=client)

    unrecognized_options = get_unrecognized(master_alerts)
    if unrecognized_options is None:
        logger.info("No unrecognized options found in master alerts, nothing to remove")
        return

    remove_unrecognized(unrecognized_options, "", dry, client=client)


def _add_remove_master_unrecognized_options_arguments(parser) -> None:
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


def add_remove_master_unrecognized_options_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "remove-master-unrecognized-options",
        help="Remove unrecognized options from master dynamic config",
        description="Remove unrecognized options from master dynamic config.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.set_defaults(func=run_remove_master_unrecognized_options)
    _add_remove_master_unrecognized_options_arguments(parser)
