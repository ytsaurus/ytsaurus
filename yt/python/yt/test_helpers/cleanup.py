from yt.common import YtError, wait

import logging


logger = logging.getLogger("TestHelpers")


def abort_transactions(list_action, abort_action, exists_action, get_action):
    sequoia_tablet_cells = []
    # COMPAT(gritukan, aleksandra-zh)
    if exists_action("//sys/tablet_cell_bundles/sequoia"):
        sequoia_tablet_cells = get_action("//sys/tablet_cell_bundles/sequoia/@tablet_cell_ids")

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
        if "Chunk autotomizer" in title:
            continue
        if "QueueAgent" in title:
            continue

        sequoia = False
        for cell_id in sequoia_tablet_cells:
            if cell_id in title:
                sequoia = True
        if sequoia:
            continue

        try:
            abort_action(tx)
        except YtError:
            pass


def cleanup_operations(list_action, abort_action, remove_action, remove_operations_archive=True):
    operations_from_orchid = []
    try:
        operations_from_orchid = list_action("//sys/scheduler/orchid/scheduler/operations")
    except YtError:
        logger.exception("Failed to fetch operations from orchid")

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
    VIRTUAL_MAPS = [
        "account_tree",
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
        "access_control_object_namespaces",
        "zookeeper_shards",
    ]

    if enable_secondary_cells_cleanup:
        VIRTUAL_MAPS.append("tablet_actions")

    list_args = [
        {
            "path": "//sys/{}".format(m),
            "attributes": ["id", "builtin", "life_stage"]
        }
        for m in VIRTUAL_MAPS
    ]
    list_results = list_multiple_action(list_args)

    object_ids_to_remove = []
    object_ids_to_check = []
    for objects in list_results:
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
