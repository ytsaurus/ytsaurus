#!/usr/bin/env python2

import yt.wrapper as yt

from time import sleep

import argparse
import sys

def ask_user_confirmation(question, default=None):
    answer = raw_input(question)
    while True:
        if answer.lower() in ("y", "yes"):
            return True
        elif answer.lower() in ("n", "no"):
            return False
        elif default is not None:
            return default
        else:
            answer = input(question)

################################################################################
########## A simple logging function with scoped indentation support. ##########

_indent = [0]
class IndentGuard:
    def __init__(self, indent):
        self._indent = indent

    def __enter__(self):
        self._indent[0] += 2

    def __exit__(self, etype, evalue, traceback):
        self._indent[0] -= 2

def log_indent():
    return IndentGuard(_indent)

def _log_color(color, *args):
    if color:
        color_begin = "\033[" + str(color) + "m"
        color_end = "\033[0m"
    else:
        color_begin = ""
        color_end = ""

    print color_begin + (" " * _indent[0]) + "".join([str(a) for a in args]) + color_end

def log(*args):
    _log_color(None, *args)

def log_warn(*args):
    # 91?
    _log_color(91, *args)

################################################################################

def do_inherit_acl(path, inherit_acl):
    if not inherit_acl:
        return

    log("Setting '@inherit_acl' to '%false'")
    yt.set(path + "/@inherit_acl", False)

def undo_inherit_acl(path, inherit_acl):
    if not inherit_acl:
        return

    log("Attempting to restore '@inherit_acl' to '%true'")
    with log_indent():
        try:
            yt.set(path + "/@inherit_acl", True)
            log("Done")
        except BaseException as e:
            log("Failed: {}".format(e))
            log_warn("Please be sure to restore '@inherit_acl' to '%true' if it's not")

def do_acl(path):
    log("Setting '@acl' to allow admins - and only admins - everything")
    yt.set(path + "/@acl", [{
        "action": "allow",
        "subjects": ["admins"],
        "permissions": ["read", "write", "remove", "create", "mount", "administer"],
        "inheritance_mode": "object_and_descendants"
    }])

def undo_acl(path, acl, effective_acl):
    log("Attempting to restore '@acl' to its original value")
    with log_indent():
        try:
            yt.set(path + "/@acl", acl)
            log("Done")
        except BaseException as e:
            log("Failed: {}".format(e))
            log_warn("Please be sure to restore '@acl' to its original value:")
            log(acl)
            log("JFYI, original '@effective_acl' was:")
            log("", effective_acl)

def do_acl_post_externalization(path, acl, effective_acl):
    log("Setting '@acl' to formerly effective acl")
    try:
        yt.set(path + "/@acl", effective_acl)
    except BaseException as e:
        log("Failed: {}".format(e))
        log_warn("Please be sure to set '@acl' to its effective value:")
        log(effective_acl)
        log("JFYI, original '@acl' was:")
        log("", acl)

def pause():
    seconds = 6 * 60
    log("Waiting ", seconds, " seconds")
    log("Press Ctrl+C once to skip waiting")
    try:
        sleep(seconds)
    except KeyboardInterrupt:
        with log_indent():
            log("Skipped")

def abort_txs(path):
    log("Aborting transactions in the directory")
    with log_indent():
        for item in yt.search(path, attributes=["lock_count"]):
            item_path = str(item)
            if item.attributes["lock_count"] > 0:
                log("Unlocking: ", item_path)
                with log_indent():
                    for lock in yt.get(item_path + "/@locks"):
                        tx_id = lock["transaction_id"]
                        log("Aborting: ", tx_id)
                        yt.abort_transaction(tx_id)

def find_cross_cell_symlinks(path, cell_tag):
    log("Searching for would-be cross-cell symlinks in the directory")

    result = []

    with log_indent():
        for item in yt.search(path, node_type=["link"], attributes=["target_path"]):
            item_path = str(item)
            target_path = item.attributes["target_path"]
            if not target_path.startswith(path):
                result.append((item_path, target_path))

    return result

def find_non_externalizable_tables(path, cell_tag):
    log("Searching for non-externalizable tables in the directory")

    non_external_tables = []
    cell_conflict_tables = []

    with log_indent():
        for item in yt.search(path, node_type=["table", "file", "journal"], attributes=["external", "external_cell_tag"]):
            item_path = str(item)
            if not item.attributes["external"]:
                non_external_tables.append(item_path)
            elif item.attributes["external_cell_tag"] == cell_tag:
                cell_conflict_tables.append(item_path)

    return non_external_tables, cell_conflict_tables

def externalize(path, cell_tag):
    log("Externalizing '", path, "' to cell ", cell_tag)
    yt.externalize(path, cell_tag=cell_tag)

def main():
    if not hasattr(yt, "externalize"):
        log("YT python library does not seem to support the 'externalize' command")
        return 1

    yt.config["api_version"] = "v4"

    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument("cell_tag", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    p = args.path
    c = args.cell_tag

    log_warn("Going to externalize '", p, "' to cell ", c, ". This usually takes several minutes and the directory will be inaccessible for the duration of this process")
    if not ask_user_confirmation("Do you wish to continue? [y,N] ", False):
        return 0

    if p.endswith("/") or p.endswith("&") or p.endswith("@") or p.endswith("]"):
        log("The path provided has an unexpected suffix")
        return 2

    # NB: this also check p's existence.
    if yt.get(p + "&/@type") == "link":
        log("Node '", p, "' is a symlink")
        log("Exiting...")
        return 3

    if not yt.exists("//sys/secondary_masters/" + str(c)):
        log("Cell tag '", c, "' is unknown")
        log("Exiting...")
        return 4

    non_external_tables, cell_conflict_tables = find_non_externalizable_tables(p, c)
    if len(non_external_tables) > 0 or len(cell_conflict_tables) > 0:
        log("Found non-externalizable tables in the directory; externalization is not possible")
        if len(non_external_tables) > 0:
            log("Tables that are not external:\n  ", "\n  ".join(non_external_tables))
        if len(cell_conflict_tables) > 0:
            log("Tables that are externalized to cell ", c, ":\n  ", "\n  ".join(cell_conflict_tables))
        return 5

    cross_cell_symlinks = find_cross_cell_symlinks(p, c)
    if len(cross_cell_symlinks) > 0:
        log("Found would-be cross-cell symlinks in the directory; externalization is not possible")
        log("Would-be cross-cell symlinks:\n  ", "\n  ".join([link_path + " -> " + target_path for link_path, target_path in cross_cell_symlinks]))

    inherit_acl = yt.get(p + "/@inherit_acl")
    acl = yt.get(p + "/@acl")
    effective_acl = yt.get(p + "/@effective_acl")

    if args.dry_run:
        return 0

    try:
        acl_modified = False
        do_inherit_acl(p, inherit_acl)
        do_acl(p)
        acl_modified = True

        pause()

        abort_txs(p)
        externalize(p, c)
    except BaseException as e:
        with log_indent():
            log("Failed: {}".format(e))
            undo_inherit_acl(p, inherit_acl)
            if acl_modified:
                undo_acl(p, acl, effective_acl)
            log("Exiting...")
            return 7 if acl_modified else 6

    do_acl_post_externalization(p, acl, effective_acl)

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
