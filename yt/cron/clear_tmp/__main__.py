#!/usr/bin/env python3

"""
Script to clear tmp-like directories.

Script walks across specified directory and clears obsolete tables directories etc.

Special attribute @clear_tmp_config adjusts clear_tmp behavior on tables and directories

    @clear_tmp_config/dont_prune
        if true, current object will not be removed, but when set on directory
        children of this directory are still considered for removal
"""

from yt.common import _pretty_format_for_logging, date_string_to_datetime, utcnow
from yt.wrapper.common import chunk_iter_list, get_value

import yt.wrapper as yt
import yt.logger as yt_logger

from collections import namedtuple

import os
import datetime
import argparse
import logging

from contextlib import contextmanager


@contextmanager
def set_command_params(yt_client, params):
    previous_params = yt.config.get_option("COMMAND_PARAMS", yt_client)
    try:
        yt.config.set_option("COMMAND_PARAMS", params, yt_client)
        yield
    finally:
        yt.config.set_option("COMMAND_PARAMS", previous_params, yt_client)


class ParseTimedeltaDaysArgument(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, datetime.timedelta(days=int(values)))


class ParseTimedeltaMinutesArgument(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, datetime.timedelta(minutes=int(values)))


COMMON_ATTRIBUTES_TO_REQUEST = ("account", "acl", "revision", "type", "clear_tmp_config")

logger = logging.getLogger("clear_tmp")


class Resources:
    def __init__(self, max_disk_space, max_node_count, max_chunk_count, tag=""):
        self.max_disk_space = max_disk_space
        self.max_node_count = max_node_count
        self.max_chunk_count = max_chunk_count
        self.tag = tag

        self.disk_space = 0
        self.node_count = 0
        self.chunk_count = 0

        self.limit_exceeded = False

    def add(self, obj_attributes):
        reasons = []

        self.node_count += 1
        if self.max_node_count is not None and self.node_count > self.max_node_count:
            reasons.append("max node count {}violated".format(self.tag))

        if hasattr(obj_attributes, "resource_usage"):
            self.chunk_count += int(obj_attributes.resource_usage["chunk_count"])
            if self.max_chunk_count is not None and self.chunk_count > self.max_chunk_count:
                reasons.append(f"max chunk count {self.tag}violated")
            self.disk_space += int(obj_attributes.resource_usage["disk_space"])
            if self.max_disk_space is not None and self.disk_space > self.max_disk_space:
                reasons.append(f"max disk space {self.tag}violated")

        if reasons:
            self.limit_exceeded = True

        return reasons


def get_age(time_str):
    return utcnow() - date_string_to_datetime(time_str)


def is_empty(obj_attributes):
    dspm = obj_attributes.get("resource_usage", {}).get("disk_space_per_medium", {})
    return not sum(dspm.values())


def is_locked(obj):
    return any(map(lambda lock: lock["mode"] in ("exclusive", "shared"), obj.attributes["locks"]))


# TODO(ignat): refactor parameters of this method
def collect_objects_to_remove(yt_client, root_dir, dirs, links, time_attribute_name, object_to_attributes, resources, resources_per_user, args):
    remaining_dir_sizes = dict((obj.name, obj.count) for obj in dirs)

    objects = []
    attributes_to_request = set(list(COMMON_ATTRIBUTES_TO_REQUEST) +
                                ["locks", "resource_usage", "target_path", "owner", time_attribute_name])
    ObjectAttributes = namedtuple("ObjectAttributes", list(attributes_to_request))

    objects_iterator = yt_client.search(
        root_dir,
        enable_batch_mode=True,
        attributes=attributes_to_request)

    for obj in objects_iterator:
        if obj.attributes.get("type") in ("map_node", "list_node", "portal_entrance"):
            continue
        if is_locked(obj):
            continue
        if args.do_not_remove_objects_with_other_account and obj.attributes.get("account") != args.account:
            continue
        if obj.attributes.get("acl"):
            continue
        obj_attributes = ObjectAttributes(*[obj.attributes.get(attr) for attr in attributes_to_request])
        object_to_attributes[str(obj)] = obj_attributes
        objects.append((get_age(obj.attributes[time_attribute_name]), is_empty(obj.attributes), str(obj), obj_attributes))

    objects.sort()

    logger.info("Collected %d objects", len(objects))

    to_remove = []
    to_remove_set = set()

    def add_to_remove(obj, reason):
        obj = str(obj)
        if obj not in to_remove_set:
            to_remove.append((obj, reason))
            to_remove_set.add(obj)

    for age, empty, obj_name, obj_attributes in objects:
        has_locks = False
        dont_prune = False

        if obj_attributes.clear_tmp_config is not None:
            dont_prune = obj_attributes.clear_tmp_config.get("dont_prune", False)

        # filter by locks
        if args.do_not_remove_objects_with_locks:
            if obj_attributes.locks:
                has_locks = True
            if obj_attributes.type == "link":
                target_path = obj_attributes.target_path
                if target_path in object_to_attributes and object_to_attributes[target_path].locks:
                    has_locks = True

        reasons = []

        owner = obj_attributes.owner
        if owner is not None:
            if owner not in resources_per_user:
                resources_per_user[owner] = Resources(
                    max_disk_space=args.max_disk_space_per_owner,
                    max_node_count=args.max_node_count_per_owner,
                    max_chunk_count=args.max_chunk_count_per_owner,
                    tag=f"per owner '{owner}' ")
            reasons += resources_per_user[owner].add(obj_attributes)

        need_to_skip = has_locks or age < args.safe_age or dont_prune
        if not reasons or need_to_skip:
            reasons += resources.add(obj_attributes)

        if need_to_skip:
            continue

        if age > args.max_age:
            reasons.append("max age violated")

        if args.remove_empty and empty:
            reasons.append("empty table/file")

        # NB: ypath_dirname invokes ypath parsing that occur to be a bottleneck.
        obj_dir = yt.ypath_dirname(yt.YPath(obj_name, simplify=False))
        dir_node_count = remaining_dir_sizes.get(obj_dir, 0)
        if dir_node_count > args.max_dir_node_count:
            reasons.append("max dir node count violated")

        if reasons:
            if dir_node_count > 0:
                remaining_dir_sizes[obj_dir] -= 1

            add_to_remove(obj_name, str(reasons))

    return to_remove


def configure_logger(args):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s\t%(levelname)s\t{}\t%(message)s'.format(os.environ["YT_PROXY"])))
    logger.addHandler(handler)
    logger.setLevel(level=args.log_level.upper())

    if args.verbose:
        yt_logger.LOGGER.setLevel(logging.DEBUG)


def main():
    parser = argparse.ArgumentParser(description="Clean objects from cypress")
    parser.add_argument("--directory", default="//tmp")
    parser.add_argument("--account")
    parser.add_argument("--max-disk-space", type=int, default=None)
    parser.add_argument("--max-chunk-count", type=int, default=None)
    parser.add_argument("--max-node-count", type=int, default=None)
    parser.add_argument("--max-disk-space-per-owner", type=int, default=None)
    parser.add_argument("--max-chunk-count-per-owner", type=int, default=None)
    parser.add_argument("--max-node-count-per-owner", type=int, default=None)
    parser.add_argument("--account-usage-ratio-save-per-owner", type=float, default=None)
    parser.add_argument("--account-usage-ratio-save-total", type=float, default=0.5)
    # NB: default node count limit for `map_node` is 50000
    parser.add_argument("--max-dir-node-count", type=int, default=40000)
    parser.add_argument("--remove-batch-size", type=int)
    parser.add_argument("--safe-age-minutes", "--safe-age", dest="safe_age",
                        action=ParseTimedeltaMinutesArgument, default=datetime.timedelta(minutes=10),
                        help="Objects that younger than safe-age minutes will not be removed")
    parser.add_argument("--max-age-days", "--max-age", dest="max_age",
                        action=ParseTimedeltaDaysArgument, default=datetime.timedelta(days=7),
                        help="Objects that older than max-age days will be removed")
    parser.add_argument("--time-attribute-for-age", default="modification_time",
                        help="Attribute that will be used for age calculation")
    parser.add_argument("--do-not-remove-objects-with-other-account", action="store_true", default=False,
                        help="By default all objects in directory will be removed")
    parser.add_argument("--do-not-remove-objects-with-locks", action="store_true", default=False,
                        help="Do not remove objects with any locks on them")
    parser.add_argument("--remove-empty", action="store_true", default=False, help="Remove empty tables/files/dirs")
    parser.add_argument("--token-env-variable")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--verbose", action="store_true", default=False, help="Enable YT library DEBUG log level")
    args = parser.parse_args()

    configure_logger(args)

    time_attribute_name = args.time_attribute_for_age

    token = None if args.token_env_variable is None else os.environ.get(args.token_env_variable)

    yt_client = yt.YtClient(config=yt.default_config.get_config_from_env(), token=token)

    if args.account is not None:
        account_disk_space = yt_client.get(f"//sys/accounts/{args.account}/@resource_limits/disk_space")
        account_node_count = yt_client.get(f"//sys/accounts/{args.account}/@resource_limits/node_count")
        account_chunk_count = yt_client.get(f"//sys/accounts/{args.account}/@resource_limits/chunk_count")
        if args.max_disk_space is None:
            args.max_disk_space = int(account_disk_space * args.account_usage_ratio_save_total)
        if args.max_node_count is None:
            args.max_node_count = int(account_node_count * args.account_usage_ratio_save_total)
        if args.max_chunk_count is None:
            args.max_chunk_count = int(account_chunk_count * args.account_usage_ratio_save_total)

        if args.account_usage_ratio_save_per_owner is not None:
            if args.max_disk_space_per_owner is None:
                args.max_disk_space_per_owner = int(account_disk_space * args.account_usage_ratio_save_per_owner)
            if args.max_node_count_per_owner is None:
                args.max_node_count_per_owner = int(account_node_count * args.account_usage_ratio_save_per_owner)
            if args.max_chunk_count_per_owner is None:
                args.max_chunk_count_per_owner = int(account_chunk_count * args.account_usage_ratio_save_per_owner)

    if not yt_client.exists(args.directory):
        return

    # collect aux objects
    logger.info("Start collecting links and dirs")

    aux_object_attributes_to_request = set(list(COMMON_ATTRIBUTES_TO_REQUEST) + ["count", time_attribute_name])
    DirAttributes = namedtuple("DirAttributes", list(aux_object_attributes_to_request) + ["name"])

    dirs = []
    links = set()
    aux_objects_iterator = yt_client.search(
        args.directory,
        object_filter=lambda obj: obj.attributes["type"] in ("link", "map_node", "list_node", "portal_entrance"),
        attributes=aux_object_attributes_to_request,
        enable_batch_mode=True,
    )
    for obj in aux_objects_iterator:
        if obj.attributes["type"] == "link":
            links.add(str(obj))
        else:  # dir case
            def append_dirs(dirs, name, obj, aux_object_attributes_to_request):
                dirs.append(DirAttributes(
                    name=name,
                    **dict([(key, obj.attributes.get(key)) for key in aux_object_attributes_to_request])
                ))

            if obj.attributes["type"] == "portal_entrance":
                name = str(obj)
                obj = yt_client.get(name, attributes=list(aux_object_attributes_to_request) + ["name"])
                append_dirs(dirs, name, obj, aux_object_attributes_to_request)
            else:
                append_dirs(dirs, str(obj), obj, aux_object_attributes_to_request)

    dir_sizes = dict((obj.name, obj.count) for obj in dirs)
    logger.info("Finish collecting links and dirs (link_count: %d, dir_count: %d)", len(links), len(dirs))

    object_to_attributes = {}

    resources_per_user = {}
    resources = Resources(
        max_disk_space=args.max_disk_space,
        max_node_count=args.max_node_count,
        max_chunk_count=args.max_chunk_count)

    logger.info("Start collecting objects to remove")
    to_remove = collect_objects_to_remove(
        yt_client,
        args.directory,
        dirs,
        links,
        time_attribute_name,
        object_to_attributes,
        resources,
        resources_per_user,
        args)
    logger.info("Finished collecting objects to remove")

    max_batch_size = get_value(args.remove_batch_size, yt_client.config["max_batch_size"])
    batch_client = yt_client.create_batch_client()

    logger.info("Start removing")

    batch_index = 0
    for objects in chunk_iter_list(to_remove, max_batch_size):
        batch_index += 1
        logger.info("Removing batch %d", batch_index)
        remove_results = []
        for obj, reason in objects:
            attributes = object_to_attributes.get(obj)
            # It can be missing for link that we skip during search.
            if attributes is None:
                continue
            if get_age(getattr(attributes, time_attribute_name)) <= args.safe_age:
                continue

            info = f"(reason={reason})"
            if obj in object_to_attributes:
                attrs = object_to_attributes[obj]
                time = getattr(attrs, time_attribute_name)
                info = info + f" ({time_attribute_name}={time})"
                if "resource_usage" in attrs:
                    disk_space = attrs.resource_usage["disk_space"]
                    info = info + f" (size={disk_space})"
            logger.debug("Removing %s %s", obj, info)

            # NB: ypath_dirname invokes ypath parsing that occur to be a bottleneck.
            obj_dir = yt.ypath_dirname(yt.YPath(obj, simplify=False))

            # Directory may be missing since we separately search for dirs and files.
            if obj_dir in dir_sizes:
                dir_sizes[obj_dir] -= 1

            params = {"prerequisite_revisions": [{
                "path": obj + "&",
                "revision": attributes.revision,
                "transaction_id": "0-0-0-0"
            }]}

            with set_command_params(batch_client, params):
                remove_results.append(batch_client.remove(obj, force=True))

        batch_client.commit_batch()

        for obj, remove_result in zip(objects, remove_results):
            path, _ = obj
            if remove_result.is_ok():
                logger.debug("%s removed", path)
            else:
                error = yt.YtResponseError(remove_result.get_error())
                if error.is_concurrent_transaction_lock_conflict() or error.is_prerequisite_check_failed() or error.is_resolve_error():
                    logger.debug("Failed to remove %s (error: %s)", path, _pretty_format_for_logging(error))
                else:
                    raise error

    del to_remove[:]

    logger.info("Finish removing")

    # check broken links
    logger.info("Start removing broken links")
    links_iterator = yt_client.search(
        args.directory,
        node_type=["link"],
        attributes=["broken", "account"],
        enable_batch_mode=True
    )
    for obj in links_iterator:
        if args.do_not_remove_objects_with_other_account and obj.attributes.get("account") != args.account:
            continue
        if obj.attributes["broken"]:
            logger.debug("Removing broken link %s", obj)
            batch_client.remove(obj, force=True)

    batch_client.commit_batch()

    logger.info("Finished removing broken links")

    logger.info("Start removing empty dirs")
    while True:
        removed_dirs = []
        for dir in dirs:
            # NB: avoid removing root directory itself
            if args.directory.startswith(dir.name):
                continue
            if args.do_not_remove_objects_with_other_account and dir.account != args.account:
                continue
            if dir.acl:
                continue
            if dir_sizes[dir.name] != 0:
                continue
            if dir.clear_tmp_config is not None and dir.clear_tmp_config.get("dont_prune", False):
                continue

            reasons = resources.add(dir)
            if get_age(getattr(dir, time_attribute_name)) > args.max_age:
                reasons.append("max age violated")

            parent_dir = yt.ypath_dirname(yt.YPath(dir.name, simplify=False))
            if dir_sizes.get(parent_dir, 0) > args.max_dir_node_count:
                reasons.append("max dir node count violated")

            if args.remove_empty:
                reasons.append("empty dir")

            if reasons:
                logger.debug("Removing empty dir %s (reasons: %s)", dir, str(reasons))

                # To avoid removing twice
                dir_sizes[dir.name] = -1

                remove_result = batch_client.remove(dir.name, force=True)
                removed_dirs.append((dir.name, remove_result))

        batch_client.commit_batch()

        successfully_removed_dir_count = 0

        for dir_name, remove_result in removed_dirs:
            if remove_result.get_error():
                error = yt.YtResponseError(remove_result.get_error())
                if not error.is_concurrent_transaction_lock_conflict():
                    logger.debug("Failed to remove dir %s (error: %s)", dir_name, _pretty_format_for_logging(error))
            else:
                successfully_removed_dir_count += 1
                # NB: ypath_dirname invokes ypath parsing that occur to be a bottleneck.
                base_dir_name = yt.ypath_dirname(yt.YPath(dir_name, simplify=False))
                if base_dir_name in dir_sizes:
                    dir_sizes[base_dir_name] -= 1

        if successfully_removed_dir_count == 0:
            break

    logger.info("Finished removing empty dirs")

    logger.info(
        "Total saved resources (disk_space: %d, node_count: %d, chunk_count: %d)",
        resources.disk_space, resources.node_count, resources.chunk_count)

    for user, resources in resources_per_user.items():
        if resources.limit_exceeded:
            logger.info(
                "Resource limit reached for user (user: %s, disk_space: %d, node_count: %d, chunk_count: %d)",
                user, resources.disk_space, resources.node_count, resources.chunk_count)


if __name__ == "__main__":
    main()
