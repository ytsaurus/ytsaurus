#!/usr/bin/env python3

"""
Script to clear tmp-like directories.

Script walks across specified directory and clears obsolete tables directories etc.

Special attribute @clear_tmp_config adjusts clear_tmp behavior on tables and directories

    @clear_tmp_config/dont_prune
        if true, current object will not be removed, but when set on directory
        children of this directory are still considered for removal
        --dont-prune-white-list restricts this protection to the listed owners
"""

from yt.common import _pretty_format_for_logging, date_string_to_datetime, update_inplace, utcnow
from yt.wrapper.common import chunk_iter_list, get_value
from yt.wrapper.default_config import retries_config
from yt.cron.library.solomon import push_cluster_data_to_solomon

import yt.wrapper as yt
import yt.logger as yt_logger

import os
import datetime
import argparse
import logging

from collections import Counter, namedtuple
from contextlib import contextmanager
from enum import Enum
from typing import List, Tuple, Dict, Set, Any


SECOND = 1000
MINUTE = 60 * SECOND


def make_retries_config():
    return retries_config(count=12, enable=True, total_timeout=10 * MINUTE, backoff={
        "policy": "exponential",
        "exponential_policy": {
            "start_timeout": 5 * SECOND,
            "base": 2,
            "max_timeout": 2 * MINUTE,
            "decay_factor_bound": 0.3,
        },
    })


def configure_proxy_timeouts(config):
    update_inplace(config, {
        "proxy": {
            "request_timeout": 2 * MINUTE,
            "heavy_request_timeout": 2 * MINUTE,
            "retries": make_retries_config(),
        },
        "batch_requests_retries": make_retries_config(),
    })


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


COMMON_ATTRIBUTES_TO_REQUEST = ("account", "acl", "revision", "type", "clear_tmp_config", "owner")

logger = logging.getLogger("clear_tmp")


class RemovalReason(str, Enum):
    NODE_COUNT = "max node count violated"
    CHUNK_COUNT = "max chunk count violated"
    DISK_SPACE = "max disk space violated"
    OWNER_NODE_COUNT = "max node count per owner violated"
    OWNER_CHUNK_COUNT = "max chunk count per owner violated"
    OWNER_DISK_SPACE = "max disk space per owner violated"
    MAX_AGE = "max age violated"
    EMPTY_OBJECT = "empty table/file"
    DIRECTORY_NODE_COUNT = "max dir node count violated"
    EMPTY_DIRECTORY = "empty dir"
    BROKEN_LINK = "broken link"


class SkipReason(str, Enum):
    SAFE_AGE = "safe_age"
    DONT_PRUNE = "dont_prune"
    LOCKED = "locked"
    TARGET_LOCKED = "target_locked"
    OTHER_ACCOUNT = "other_account"
    ACL = "acl"
    ROOT_DIRECTORY = "root_directory"
    NON_EMPTY_DIRECTORY = "non_empty_directory"
    NO_REMOVAL_REASON = "no_removal_reason"
    MISSING_ATTRIBUTES = "missing_attributes"
    REMOVE_FAILED = "remove_failed"
    DRY_RUN = "dry_run"


class CleanupCounters:
    """Track one final outcome per path, including across directory passes."""

    def __init__(self):
        self.outcomes = {}
        self.removal_reasons = {}

    def collect(self, path):
        self.outcomes.setdefault(str(path), SkipReason.NO_REMOVAL_REASON)

    def skip(self, path, reason):
        self.outcomes[str(path)] = reason
        self.removal_reasons.pop(str(path), None)

    def remove(self, path, reasons):
        self.outcomes[str(path)] = None
        self.removal_reasons[str(path)] = frozenset(reasons)

    def removed_by_reason(self):
        return Counter(reason for reasons in self.removal_reasons.values() for reason in reasons)

    def log(self):
        skipped = Counter(reason for reason in self.outcomes.values() if reason is not None)
        removed = sum(reason is None for reason in self.outcomes.values())
        logger.info("Cleanup counters: collected=%d, skipped=%d, removed=%d",
                    len(self.outcomes), sum(skipped.values()), removed)
        for reason, count in sorted(skipped.items(), key=lambda item: item[0].value):
            logger.info("Skipped (%s): %d", reason.value, count)
        for reason, count in sorted(self.removed_by_reason().items(), key=lambda item: item[0].name):
            logger.info("Removed (%s): %d", reason.name.lower(), count)


def report_counters(counters, args, cluster):
    counters.log()
    if not args.solomon_service:
        return

    skipped = Counter(reason for reason in counters.outcomes.values() if reason is not None)
    labels = {"directory": args.directory, "dry_run": str(args.dry_run).lower()}
    if args.account is not None:
        labels["account"] = args.account

    counts = {
        "collected": len(counters.outcomes),
        "skipped_total": sum(skipped.values()),
        "removed_total": sum(reason is None for reason in counters.outcomes.values()),
    }
    sensors = [
        {"labels": dict(labels, sensor=f"clear_tmp.{name}"), "value": count}
        for name, count in counts.items()
    ]
    # Send zeros too, so a previous run's nonzero reason counts do not linger.
    sensors.extend(
        {"labels": dict(labels, sensor=f"clear_tmp.skipped_{reason.name.lower()}"),
         "value": skipped[reason]}
        for reason in SkipReason
    )
    removed = counters.removed_by_reason()
    sensors.extend(
        {"labels": dict(labels, sensor=f"clear_tmp.removed_{reason.name.lower()}"),
         "value": removed[reason]}
        for reason in RemovalReason
    )

    try:
        push_cluster_data_to_solomon(args.solomon_cluster or cluster, args.solomon_service, sensors)
        logger.info("Successfully pushed cleanup counters to Solomon")
    except Exception:
        logger.exception("Failed to push cleanup counters to Solomon")


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

    def consider_object_usage(self, obj_attributes, count_node=True):
        reasons = []

        if count_node:
            self.node_count += 1
        if self.max_node_count is not None and self.node_count > self.max_node_count:
            reasons.append(RemovalReason.OWNER_NODE_COUNT if self.tag else RemovalReason.NODE_COUNT)

        if hasattr(obj_attributes, "resource_usage"):
            self.chunk_count += int(obj_attributes.resource_usage["chunk_count"])
            if self.max_chunk_count is not None and self.chunk_count > self.max_chunk_count:
                reasons.append(RemovalReason.OWNER_CHUNK_COUNT if self.tag else RemovalReason.CHUNK_COUNT)
            self.disk_space += int(obj_attributes.resource_usage["disk_space"])
            if self.max_disk_space is not None and self.disk_space > self.max_disk_space:
                reasons.append(RemovalReason.OWNER_DISK_SPACE if self.tag else RemovalReason.DISK_SPACE)

        if reasons:
            self.limit_exceeded = True

        return reasons


def get_age(time_str):
    return utcnow() - date_string_to_datetime(time_str)


def is_zero_size(obj_attributes):
    dspm = obj_attributes.get("resource_usage", {}).get("disk_space_per_medium", {})
    return not sum(dspm.values())


def is_locked(obj):
    return any(map(lambda lock: lock["mode"] in ("exclusive", "shared"), obj.attributes["locks"]))


def _should_honor_dont_prune(obj_attributes, args):
    return (
        obj_attributes.clear_tmp_config is not None
        and obj_attributes.clear_tmp_config.get("dont_prune", False)
        and (args.dont_prune_white_list is None or obj_attributes.owner in args.dont_prune_white_list)
    )


def _is_deletion_candidate(
    obj_name: str,
    age: datetime.timedelta,
    obj_attributes,
    args: argparse.Namespace,
    object_to_attributes: dict,
    counters: CleanupCounters,
) -> bool:
    """Check for safe age, user config (dont_prune), locks
    """
    if age < args.safe_age:
        counters.skip(obj_name, SkipReason.SAFE_AGE)
        return False
    if _should_honor_dont_prune(obj_attributes, args):
        counters.skip(obj_name, SkipReason.DONT_PRUNE)
        logger.debug("Skipping: %s has \"dont_prune\"", obj_name)
        return False
    if args.do_not_remove_objects_with_locks:
        if obj_attributes.locks:
            counters.skip(obj_name, SkipReason.LOCKED)
            logger.debug("Skipping: %s has locks", obj_name)
            return False
        if obj_attributes.type == "link":
            target_path = obj_attributes.target_path
            if target_path in object_to_attributes and object_to_attributes[target_path].locks:
                counters.skip(obj_name, SkipReason.TARGET_LOCKED)
                logger.debug("Skipping: link %s has locks", obj_name)
                return False
    return True


# TODO(ignat): refactor parameters of this method
def collect_objects_to_remove(
    yt_client,
    root_dir,
    dirs: List[Any],
    links,
    time_attribute_name,
    object_to_attributes,
    total_resources: Resources,
    resources_per_user: Dict[str, Resources],
    args: argparse.Namespace,
    counters: CleanupCounters,
) -> List[Tuple[str, List[RemovalReason]]]:
    dir_childrens_count: Dict[str, int] = dict((obj.name, obj.count) for obj in dirs)

    attributes_to_request = set(list(COMMON_ATTRIBUTES_TO_REQUEST) +
                                ["locks", "resource_usage", "target_path", time_attribute_name])
    ObjectAttributes = namedtuple("ObjectAttributes", list(attributes_to_request))

    collected_objects: List[Tuple[bool, datetime.timedelta, bool, str, ObjectAttributes]] = []

    objects_iterator = yt_client.search(
        root_dir,
        enable_batch_mode=True,
        attributes=attributes_to_request)

    for obj in objects_iterator:
        counters.collect(obj)
        if obj.attributes.get("type") in ("map_node", "list_node", "portal_entrance"):
            continue
        if is_locked(obj):
            counters.skip(obj, SkipReason.LOCKED)
            continue
        if args.do_not_remove_objects_with_other_account and obj.attributes.get("account") != args.account:
            counters.skip(obj, SkipReason.OTHER_ACCOUNT)
            continue
        if obj.attributes.get("acl"):
            counters.skip(obj, SkipReason.ACL)
            continue
        obj_attributes = ObjectAttributes(*[obj.attributes.get(attr) for attr in attributes_to_request])
        object_to_attributes[str(obj)] = obj_attributes
        collected_objects.append([
            None,  # is candidate to remove
            get_age(obj.attributes[time_attribute_name]),  # object age
            is_zero_size(obj.attributes),  # is empty
            str(obj),  # obj name
            obj_attributes,  # obj attrs
        ])

    non_deletable_count = 0
    for collected_object in collected_objects:
        _, age, empty, obj_name, obj_attributes = collected_object
        collected_object[0] = _is_deletion_candidate(obj_name, age, obj_attributes, args, object_to_attributes, counters)
        if not collected_object[0]:
            non_deletable_count += 1

    collected_objects.sort()

    logger.info(
        f"Collected {len(collected_objects):_} objects ({non_deletable_count:_} non-deletable, {len(collected_objects) - non_deletable_count:_} deletion candidates)",
    )

    to_remove: List[Tuple[str, List[RemovalReason]]] = []
    to_remove_set = set()

    def add_to_remove(obj: str, reasons: List[RemovalReason]):
        obj = str(obj)
        if obj not in to_remove_set:
            to_remove.append((obj, reasons))
            to_remove_set.add(obj)

    def _ensure_user_resources(owner: str, resources_per_user: Dict[str, Resources], args: argparse.Namespace) -> None:
        if owner not in resources_per_user:
            resources_per_user[owner] = Resources(
                max_disk_space=args.max_disk_space_per_owner,
                max_node_count=args.max_node_count_per_owner,
                max_chunk_count=args.max_chunk_count_per_owner,
                tag=f"per owner '{owner}' ")

    for is_candidate, age, empty, obj_name, obj_attributes in collected_objects:
        owner = obj_attributes.owner
        if owner is not None:
            _ensure_user_resources(owner, resources_per_user, args)

        if not is_candidate:
            if owner is not None:
                resources_per_user[owner].consider_object_usage(obj_attributes)
            total_resources.consider_object_usage(obj_attributes)
            continue

        reasons = []
        if owner is not None:
            reasons += resources_per_user[owner].consider_object_usage(obj_attributes)
        reasons += total_resources.consider_object_usage(obj_attributes)

        if age > args.max_age:
            reasons.append(RemovalReason.MAX_AGE)

        if args.remove_empty and empty:
            reasons.append(RemovalReason.EMPTY_OBJECT)

        # NB: bottleneck (ypath_dirname).
        obj_dir = yt.ypath_dirname(yt.YPath(obj_name, simplify=False))
        dir_node_count = dir_childrens_count.get(obj_dir, 0)
        if dir_node_count > args.max_dir_node_count:
            reasons.append(RemovalReason.DIRECTORY_NODE_COUNT)

        if reasons:
            if dir_node_count > 0:
                dir_childrens_count[obj_dir] -= 1
            add_to_remove(obj_name, reasons)

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
    parser.add_argument("--dont-prune-white-list", nargs="+", action="extend", metavar="OWNER",
                        help="Honor dont_prune only for these node owners; defaults to all owners. May be repeated")
    parser.add_argument("--token-env-variable")
    parser.add_argument("--solomon-service", help="Solomon service to push cleanup counters to; disabled by default")
    parser.add_argument("--solomon-cluster", help="Solomon cluster label; defaults to the YT proxy")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Do not remove anything")
    parser.add_argument("--verbose", action="store_true", default=False, help="Enable YT library DEBUG log level")
    args = parser.parse_args()

    configure_logger(args)
    counters = CleanupCounters()

    time_attribute_name = args.time_attribute_for_age

    token = None if args.token_env_variable is None else os.environ.get(args.token_env_variable)

    config = yt.default_config.get_config_from_env()
    configure_proxy_timeouts(config)
    yt_client = yt.YtClient(config=config, token=token)

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
        report_counters(counters, args, yt_client.config["proxy"]["url"])
        return

    # collect aux objects
    logger.info("Start collecting links and dirs")

    aux_object_attributes_to_request = set(list(COMMON_ATTRIBUTES_TO_REQUEST) + ["count", time_attribute_name])
    DirAttributes = namedtuple("DirAttributes", list(aux_object_attributes_to_request) + ["name"])

    dirs: List[DirAttributes] = []
    links: Set[str] = set()
    aux_objects_iterator = yt_client.search(
        args.directory,
        object_filter=lambda obj: obj.attributes["type"] in ("link", "map_node", "list_node", "portal_entrance"),
        attributes=aux_object_attributes_to_request,
        enable_batch_mode=True,
    )
    for obj in aux_objects_iterator:
        counters.collect(obj)
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

    dir_childrens_count: Dict[str, int] = dict((obj.name, obj.count) for obj in dirs)
    logger.info("Finish collecting links and dirs (link_count: %d, dir_count: %d)", len(links), len(dirs))

    object_to_attributes = {}

    resources_per_user = {}
    total_resources = Resources(
        max_disk_space=args.max_disk_space,
        max_node_count=args.max_node_count,
        max_chunk_count=args.max_chunk_count,
    )

    # Collect objects considering nodes occupied by all dirs
    total_resources.node_count += len(dirs)

    logger.info("Start collecting objects to remove")
    to_remove = collect_objects_to_remove(
        yt_client,
        args.directory,
        dirs,
        links,
        time_attribute_name,
        object_to_attributes,
        total_resources,
        resources_per_user,
        args,
        counters)
    logger.info("Finished collecting objects to remove")

    max_batch_size = get_value(args.remove_batch_size, yt_client.config["max_batch_size"])
    batch_client = yt_client.create_batch_client()

    if args.dry_run:
        logger.info("Dry-run mode. Skipping removing.")
        for obj, _ in to_remove:
            counters.skip(obj, SkipReason.DRY_RUN)
        for dir in dirs:
            counters.skip(dir.name, SkipReason.DRY_RUN)
    else:
        logger.info("Start removing")

        batch_index = 0
        for objects in chunk_iter_list(to_remove, max_batch_size):
            batch_index += 1
            logger.info("Removing batch %d", batch_index)
            remove_results = []
            for obj, reasons in objects:
                attributes = object_to_attributes.get(obj)
                # It can be missing for link that we skip during search.
                if attributes is None:
                    counters.skip(obj, SkipReason.MISSING_ATTRIBUTES)
                    continue
                if get_age(getattr(attributes, time_attribute_name)) <= args.safe_age:
                    counters.skip(obj, SkipReason.SAFE_AGE)
                    continue

                info = f"(reason={[reason.value for reason in reasons]})"
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
                if obj_dir in dir_childrens_count:
                    dir_childrens_count[obj_dir] -= 1

                params = {"prerequisite_revisions": [{
                    "path": obj + "&",
                    "revision": attributes.revision,
                    "transaction_id": "0-0-0-0"
                }]}

                with set_command_params(batch_client, params):
                    remove_results.append((obj, reasons, batch_client.remove(obj, force=True)))

            batch_client.commit_batch()

            for path, reasons, remove_result in remove_results:
                if remove_result.is_ok():
                    counters.remove(path, reasons)
                    logger.debug("%s removed", path)
                else:
                    counters.skip(path, SkipReason.REMOVE_FAILED)
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
        removed_links = []
        for obj in links_iterator:
            counters.collect(obj)
            if args.do_not_remove_objects_with_other_account and obj.attributes.get("account") != args.account:
                counters.skip(obj, SkipReason.OTHER_ACCOUNT)
                continue
            if obj.attributes["broken"]:
                logger.debug("Removing %s (reason: %s)", obj, RemovalReason.BROKEN_LINK.value)
                removed_links.append((str(obj), batch_client.remove(obj, force=True)))

        batch_client.commit_batch()
        for path, remove_result in removed_links:
            if remove_result.is_ok():
                counters.remove(path, [RemovalReason.BROKEN_LINK])
            else:
                counters.skip(path, SkipReason.REMOVE_FAILED)

        logger.info("Finished removing broken links")

        logger.info("Start removing empty dirs")
        while True:
            removed_dirs = []
            for dir in dirs:
                # NB: avoid removing root directory itself
                if args.directory.startswith(dir.name):
                    counters.skip(dir.name, SkipReason.ROOT_DIRECTORY)
                    continue
                if dir_childrens_count[dir.name] == -1:
                    continue
                if args.do_not_remove_objects_with_other_account and dir.account != args.account:
                    counters.skip(dir.name, SkipReason.OTHER_ACCOUNT)
                    continue
                if dir.acl:
                    counters.skip(dir.name, SkipReason.ACL)
                    continue
                if dir_childrens_count[dir.name] != 0:
                    counters.skip(dir.name, SkipReason.NON_EMPTY_DIRECTORY)
                    continue
                if _should_honor_dont_prune(dir, args):
                    counters.skip(dir.name, SkipReason.DONT_PRUNE)
                    continue

                # Directory nodes were already counted before collecting objects.
                reasons = total_resources.consider_object_usage(dir, count_node=False)
                if get_age(getattr(dir, time_attribute_name)) > args.max_age:
                    reasons.append(RemovalReason.MAX_AGE)

                parent_dir = yt.ypath_dirname(yt.YPath(dir.name, simplify=False))
                if dir_childrens_count.get(parent_dir, 0) > args.max_dir_node_count:
                    reasons.append(RemovalReason.DIRECTORY_NODE_COUNT)

                if args.remove_empty:
                    reasons.append(RemovalReason.EMPTY_DIRECTORY)

                if reasons:
                    logger.debug("Removing empty dir %s (reasons: %s)", dir, [reason.value for reason in reasons])

                    # To avoid removing twice
                    dir_childrens_count[dir.name] = -1

                    remove_result = batch_client.remove(dir.name, force=True)
                    removed_dirs.append((dir.name, reasons, remove_result))
                else:
                    counters.skip(dir.name, SkipReason.NO_REMOVAL_REASON)

            batch_client.commit_batch()

            successfully_removed_dir_count = 0

            for dir_name, reasons, remove_result in removed_dirs:
                if remove_result.get_error():
                    counters.skip(dir_name, SkipReason.REMOVE_FAILED)
                    error = yt.YtResponseError(remove_result.get_error())
                    if not error.is_concurrent_transaction_lock_conflict():
                        logger.debug("Failed to remove dir %s (error: %s)", dir_name, _pretty_format_for_logging(error))
                else:
                    counters.remove(dir_name, reasons)
                    successfully_removed_dir_count += 1
                    # NB: ypath_dirname invokes ypath parsing that occur to be a bottleneck.
                    base_dir_name = yt.ypath_dirname(yt.YPath(dir_name, simplify=False))
                    if base_dir_name in dir_childrens_count:
                        dir_childrens_count[base_dir_name] -= 1

            if successfully_removed_dir_count == 0:
                break

        logger.info("Finished removing empty dirs")

    logger.info(
        f"Total saved resources (disk_space: {total_resources.disk_space:_}, node_count: {total_resources.node_count:_}, chunk_count: {total_resources.node_count:_})",
    )

    for user, user_resources in resources_per_user.items():
        if user_resources.limit_exceeded:
            logger.info(
                f"Resource limit reached for user (user: {user}, disk_space: {user_resources.disk_space:_}, node_count: {user_resources.node_count:_}, chunk_count: {user_resources.chunk_count:_})",
            )

    report_counters(counters, args, yt_client.config["proxy"]["url"])


if __name__ == "__main__":
    main()
