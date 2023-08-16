from .logger import logger
from .process_runner import process_runner

import yt.wrapper as yt
from yt.wrapper.client import YtClient
from yt.wrapper.http_helpers import get_proxy_url
from yt.test_helpers import wait

from time import sleep
import random
import copy

def create_client(proxy=None, config=yt.config.config, api_version=None):
    if proxy is None:
        proxy = get_proxy_url()
    if api_version is not None:
        config = copy.deepcopy(config)
        config["api_version"] = api_version
    return YtClient(proxy, config=config)

def wait_for_preload(table):
    # XXX @preload_state
    if yt.get(table + "/@in_memory_mode") == "none":
        return
    def check():
        statistics = yt.get(table + "/@tablets/0/statistics")
        return statistics["preload_pending_store_count"] == 0 and \
               statistics["store_count"] > 0
    wait(check, sleep_backoff=0.3)

def mount_table(path):
    #  sys.stdout.write("Mounting table %s... " % (path))
    yt.mount_table(path, sync=True)
    wait_for_preload(path)
    #  print "done"
def unmount_table(path):
    #  sys.stdout.write("Unmounting table %s... " % (path))
    yt.unmount_table(path, sync=True)
    #  print "done"
def freeze_table(path):
    #  sys.stdout.write("Freezing table %s... " % (path))
    yt.freeze_table(path, sync=True)
    #  print "done"
def unfreeze_table(path):
    #  sys.stdout.write("Unfreezing table %s... " % (path))
    yt.unfreeze_table(path, sync=True)
    #  print "done"

def sync_flush_table(path):
    freeze_table(path)
    unfreeze_table(path)

def sync_switch_bundle_options(tablet_cell_bundle, account, acl):
    assert tablet_cell_bundle not in ["sys", "default"]

    bundle_path = "//sys/tablet_cell_bundles/{}".format(tablet_cell_bundle)

    tablet_cell_ids = yt.get("{}/@tablet_cell_ids".format(bundle_path))
    def _check_cells_are_healthy():
        for cell_id in tablet_cell_ids:
            if yt.get("//sys/tablet_cells/{}/@health".format(cell_id)) != "good":
                return False
        return True

    wait(_check_cells_are_healthy)

    logger.info("Switching '{}' bundle's options (new account: '{}', new acl: '{}')".format(
        tablet_cell_bundle, account, acl))
    yt.set("{}/@options/changelog_account".format(bundle_path), account)
    yt.set("{}/@options/snapshot_account".format(bundle_path), account)
    yt.set("{}/@options/changelog_acl".format(bundle_path), acl)
    yt.set("{}/@options/snapshot_acl".format(bundle_path), acl)

    # TODO(akozhikhov): add checks for each tablet cell.

    wait(_check_cells_are_healthy)

def sync_compact_table(path):
    logger.info("Compacting table %s", path)
    chunks = set(yt.get(path + "/@chunk_ids"))
    yt.set(path + "/@forced_compaction_revision", 1)
    yt.remount_table(path)
    iter = 0
    while True:
        new_chunks = set(yt.get(path + "/@chunk_ids"))
        intersection = chunks.intersection(new_chunks)
        if not intersection:
            return
        sleep(0.5)
        iter += 1
        if iter % 10 == 0:
            logger.info("Still compacting table %s, iter = %s, %s of %s chunks remaining",
                path, iter, len(intersection), len(chunks))
            if iter >= 100 and len(intersection) < 5:
                logger.info("Chunks yet to compact: %s", ", ".join(intersection))

def compact_chunk_views(path):
    root_chunk_list_id = yt.get(path + "/@chunk_list_id")
    root_tree = yt.get("#{}/@tree".format(root_chunk_list_id))

    def _has_chunk_view(tree):
        type = tree.attributes.get("type")
        if type == "chunk_list":
            for child in tree:
                if _has_chunk_view(child):
                    return True
            return False
        elif type == "chunk_view":
            return True
        elif type == "dynamic_store":
            return False
        return False

    if _has_chunk_view(root_tree):
        logger.info("Table %s contains chunk views, will compact", path)
        sync_compact_table(path)

def remove_existing(paths, force):
    for path in paths:
        if yt.exists(path):
            if force:
                yt.remove(path)
            else:
                raise Exception("Table %s already exists. Use --force" % path)

def get_tablet_sizes(tablet_size_table, tablet_count, min_index=0):
    # XXX: order by
    tablets_info = list(yt.select_rows(
        "* from [%s] where tablet_index >= %s and tablet_index < %s" % (
            tablet_size_table, min_index, tablet_count,
        )
    ))
    tablets_info.sort(key=lambda x: int(x["tablet_index"]))
    return [tablet_info["size"] for tablet_info in tablets_info]

def equal_table_rows(columns, lhs, rhs):
    if (lhs == None) + (rhs == None) > 0:
        return (lhs == None) == (rhs == None)
    for c in columns:
        if ((c in lhs) != (c in rhs)) or ((c in lhs) and (lhs[c] != rhs[c])):
            return False
    return True

def is_table_empty(table):
    assert yt.exists(table)
    return yt.get("{}/@row_count".format(table)) == 0
