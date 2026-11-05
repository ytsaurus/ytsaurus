from yt.cron.library.orm.logger import configure_logger

from yt.orm.library.garbage_collection import GARBAGE_SIGNATURE

from yt.wrapper import yson
from yt.wrapper.errors import YtResponseError

import argparse
import logging
import os
import time


def _collect_garbage(orm_client, garbage_traits, default_ttl):
    current_time = time.time()
    garbage = []
    for object_type in garbage_traits.collected_types:
        objects = orm_client.select_objects(
            object_type,
            selectors=["/meta/key", "/meta/creation_time", "/labels/garbage_collection"],
            filter="[/labels/garbage_collection] != #",
        )
        for object_key, creation_time, garbage_collection in objects:
            owner = garbage_collection.get("owner", "")
            creation_time /= 1000 * 1000
            ttl = garbage_collection.get("ttl", default_ttl)
            if not owner.endswith(GARBAGE_SIGNATURE):
                logging.warning(
                    "Found garbage object with unexpected owner signature ("
                    f"type: {object_type}, key: {object_key}, owner: {owner})"
                )
            elif creation_time + ttl < current_time:
                garbage.append((object_type, object_key))

    return garbage


def _parse_arguments(human_readable_orm_name, snake_case_orm_name):
    kebab_case_orm_name = snake_case_orm_name.replace("_", "-")
    parser = argparse.ArgumentParser(
        description=f"Clean up {human_readable_orm_name} objects after various monitorings",
    )
    parser.add_argument("--cluster", help="YT cluster name", default=os.environ.get("YT_PROXY"))
    parser.add_argument(
        f"--{kebab_case_orm_name}-client-config",
        help=f"{human_readable_orm_name} client config in Yson format",
        default=None,
    )
    parser.add_argument("--default-ttl", type=int, default=60 * 60, help="Default garbage TTL")
    return parser.parse_args()


def main(orm_client_cls, orm_logger, garbage_traits, human_readable_orm_name, orm_token):
    configure_logger(orm_logger=orm_logger)

    snake_case_orm_name = human_readable_orm_name.replace(" ", "_").lower()
    arguments = _parse_arguments(human_readable_orm_name, snake_case_orm_name)
    orm_cluster = arguments.cluster.replace("yp-", "")

    orm_client_config = getattr(arguments, f"{snake_case_orm_name}_client_config")
    if orm_client_config is None:
        orm_client_config = dict()
    else:
        orm_client_config = yson.loads(orm_client_config.encode())

    logging.info(f"Collecting garbage on {human_readable_orm_name} cluster {orm_cluster}")

    if orm_token is not None:
        orm_client_config["token"] = orm_token

    with orm_client_cls(orm_cluster, config=orm_client_config) as orm_client:
        garbage = _collect_garbage(orm_client, garbage_traits, arguments.default_ttl)
        logging.info(f"Found {len(garbage)} garbage objects")

        removed_count = 0
        last_error = None
        for object_type, object_key in garbage:
            object_description = f"(type: {object_type}, key: {object_key})"
            try:
                garbage_traits.remove_hook(orm_client, object_type, object_key)
            except YtResponseError as ex:
                last_error = ex
                logging.warning(f"Failed to remove object {object_description}: {ex}")
            else:
                logging.info(f"Successfully removed object {object_description}")
                removed_count += 1

    logging.info(f"Removed {removed_count} garbage objects")

    if removed_count < len(garbage):
        logging.warning(f"Failed to remove {len(garbage) - removed_count} garbage objects")

    if removed_count == 0 and len(garbage) > 2:
        raise RuntimeError(f"Failed to perform garbage collection (last_error: {last_error})")
