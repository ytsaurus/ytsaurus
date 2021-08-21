from __future__ import print_function

from yt.common import YtError, wait, format_error

import sys


def abort_transactions(list_action, abort_action):
    for tx in list_action("//sys/transactions", attributes=["title"]):
        title = tx.attributes.get("title", "")
        if "Scheduler lock" in title:
            continue
        if "Controller agent incarnation" in title:
            continue
        if "Lease for" in title:
            continue
        if "Prerequisite for" in title:
            continue
        if "Chunk merger" in title:
            continue
        try:
            abort_action(tx)
        except YtError:
            pass


def cleanup_operations(list_action, abort_action, remove_action, remove_operations_archive=True):
    operations_from_orchid = []
    try:
        operations_from_orchid = list_action("//sys/scheduler/orchid/scheduler/operations")
    except YtError as err:
        print(format_error(err), file=sys.stderr)

    for operation_id in operations_from_orchid:
        if not operation_id.startswith("*"):
            try:
                abort_action(operation_id)
            except YtError:
                pass
    remove_action(path="//sys/operations/*")
    if remove_operations_archive:
        remove_action(path="//sys/operations_archive", force=True, recursive=True)


def cleanup_objects(list_multiple_action, remove_multiple_action, exists_multiple_action,
                    enable_secondary_cells_cleanup=False, object_ids_to_ignore=None):
    TYPES = [
        "accounts",
        "users",
        "groups",
        "racks",
        "data_centers",
        "tablet_cells",
        "areas",
        "tablet_cell_bundles",
        "chaos_cells",
        "chaos_cell_bundles",
        "network_projects",
        "http_proxy_roles",
        "rpc_proxy_roles",
    ]

    if enable_secondary_cells_cleanup:
        TYPES = TYPES + ["tablet_actions"]

    list_args = []
    for type in TYPES:
        list_args.append(dict(
            path="//sys/" + ("account_tree" if type == "accounts" else type),
            attributes=["id", "builtin", "life_stage"],
        ))
    list_results = list_multiple_action(list_args)

    object_ids_to_remove = []
    object_ids_to_check = []
    for type, objects in zip(TYPES, list_results):
        for object in objects:
            if object.attributes["builtin"]:
                continue

            object_id = object.attributes["id"]
            if object_ids_to_ignore is not None and object_id in object_ids_to_ignore:
                continue

            object_ids_to_check.append(object_id)
            life_stage = object.attributes["life_stage"]
            if life_stage == "creation_committed" or life_stage == "creation_pre_committed":
                object_ids_to_remove.append(object_id)

    def remove_and_check():
        remove_multiple_action([dict(path="#" + object_id, force=True, recursive=True) for object_id in object_ids_to_remove])
        exists_result = exists_multiple_action(["#" + object_id for object_id in object_ids_to_check])
        return not any(exists_result)

    wait(remove_and_check)
