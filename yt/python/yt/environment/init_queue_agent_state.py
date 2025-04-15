#!/usr/bin/python3

from yt.wrapper import config, YtClient
from yt.wrapper.ypath import ypath_join

from yt.environment.migrationlib import TableInfo, Migration, Conversion

import argparse

from copy import deepcopy

################################################################################


def _replicated_tables_filter_callback(client, table_path):
    """
    Filters out replicated tables.
    """
    upstream_replica_id = client.get("{}/@upstream_replica_id".format(table_path))
    return upstream_replica_id != "0-0-0-0"


DEFAULT_TABLET_CELL_BUNDLE = "default"
SYS_TABLET_CELL_BUNDLE = "sys"
YT_QUEUE_AGENT_TABLET_CELL_BUNDLE = "yt-queue-agent"
# NB(apachee): Bundle that will be used if no tablet_cell_bundle was specified in table attributes. See prepare_migration.
# This variable SHOULD NOT be changed to ensure consistency between different versions of this script.
QUEUE_AGENT_STATE_DEFAULT_TABLET_CELL_BUNDLE = YT_QUEUE_AGENT_TABLET_CELL_BUNDLE

# NB(apachee): List of all bundles, which can be used for queue agent state
# in order from most favorable to least favorable.
#
# When creating tables with one of these bundles we try to create table in this
# bundle, and if the bundle does not exists we try the next bundle in the list
# after this one and so on.
QUEUE_AGENT_STATE_TABLET_CELL_BUNDLE_PRIORITY_LIST = [
    YT_QUEUE_AGENT_TABLET_CELL_BUNDLE,
    SYS_TABLET_CELL_BUNDLE,
    DEFAULT_TABLET_CELL_BUNDLE,
]

DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE = {
    "tablet_cell_bundle": SYS_TABLET_CELL_BUNDLE,
}
DEFAULT_TABLE_ATTRIBUTES = {
    "tablet_cell_bundle": YT_QUEUE_AGENT_TABLET_CELL_BUNDLE,
}

DEFAULT_SHARD_COUNT = 1

INITIAL_TABLE_INFOS = {
    "queues": TableInfo(
        [
            ("cluster", "string"),
            ("path", "string"),
        ],
        [
            ("row_revision", "uint64"),
            ("revision", "uint64"),
            ("object_type", "string"),
            ("dynamic", "boolean"),
            ("sorted", "boolean"),
            ("auto_trim_config", "any"),
            ("static_export_config", "any"),
            ("queue_agent_stage", "string"),
            ("object_id", "string"),
            ("synchronization_error", "any"),
        ],
        optimize_for="lookup",
        attributes=DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE,
    ),
    "consumers": TableInfo(
        [
            ("cluster", "string"),
            ("path", "string"),
        ],
        [
            ("row_revision", "uint64"),
            ("revision", "uint64"),
            ("object_type", "string"),
            ("treat_as_queue_consumer", "boolean"),
            ("schema", "any"),
            ("queue_agent_stage", "string"),
            ("synchronization_error", "any"),
        ],
        optimize_for="lookup",
        attributes=DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE,
    ),
    "queue_agent_object_mapping": TableInfo(
        [
            ("object", "string"),
        ],
        [
            ("host", "string"),
        ],
        optimize_for="lookup",
        attributes=DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE,
    ),
    "consumer_registrations": TableInfo(
        [
            ("queue_cluster", "string"),
            ("queue_path", "string"),
            ("consumer_cluster", "string"),
            ("consumer_path", "string"),
        ],
        [
            ("vital", "boolean"),
            ("partitions", "any"),
        ],
        optimize_for="lookup",
        attributes=DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE,
    ),
    "replicated_table_mapping": TableInfo(
        [
            ("cluster", "string"),
            ("path", "string"),
        ],
        [
            ("revision", "uint64"),
            ("object_type", "string"),
            ("meta", "any"),
            ("synchronization_error", "any"),
        ],
        optimize_for="lookup",
        attributes=DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE,
    ),
}

DEFAULT_ROOT = "//sys/queue_agents"

INITIAL_VERSION = 0
TRANSFORMS = {}
ACTIONS = {}

# NB(apachee): Don't forget to add _replicated_tables_filter_callback as filter_callback
# for all conversions on tables, which might be replicated in some environments such as
# consumer_registrations or replicated_table_mapping.

# Add queue_agent_banned column for queues table.
TRANSFORMS[1] = [
    Conversion(
        "queues",
        table_info=TableInfo(
            [
                ("cluster", "string"),
                ("path", "string"),
            ],
            [
                ("row_revision", "uint64"),
                ("revision", "uint64"),
                ("object_type", "string"),
                ("dynamic", "boolean"),
                ("sorted", "boolean"),
                ("auto_trim_config", "any"),
                ("static_export_config", "any"),
                ("queue_agent_stage", "string"),
                ("object_id", "string"),
                ("queue_agent_banned", "boolean"),  # New field.
                ("synchronization_error", "any"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE,
        ),
    ),
]

# Add queue_agent_banned column for consumers table.
TRANSFORMS[2] = [
    Conversion(
        "consumers",
        table_info=TableInfo(
            [
                ("cluster", "string"),
                ("path", "string"),
            ],
            [
                ("row_revision", "uint64"),
                ("revision", "uint64"),
                ("object_type", "string"),
                ("treat_as_queue_consumer", "boolean"),
                ("schema", "any"),
                ("queue_agent_stage", "string"),
                ("queue_agent_banned", "boolean"),  # New field.
                ("synchronization_error", "any"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES_WITH_OLD_BUNDLE,
        ),
    )
]

# Change bundle of every table to yt-queue-agent
TRANSFORMS[3] = [
    Conversion(
        "queues",
        table_info=TableInfo(
            [
                ("cluster", "string"),
                ("path", "string"),
            ],
            [
                ("row_revision", "uint64"),
                ("revision", "uint64"),
                ("object_type", "string"),
                ("dynamic", "boolean"),
                ("sorted", "boolean"),
                ("auto_trim_config", "any"),
                ("static_export_config", "any"),
                ("queue_agent_stage", "string"),
                ("object_id", "string"),
                ("queue_agent_banned", "boolean"),
                ("synchronization_error", "any"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
    ),
    Conversion(
        "consumers",
        table_info=TableInfo(
            [
                ("cluster", "string"),
                ("path", "string"),
            ],
            [
                ("row_revision", "uint64"),
                ("revision", "uint64"),
                ("object_type", "string"),
                ("treat_as_queue_consumer", "boolean"),
                ("schema", "any"),
                ("queue_agent_stage", "string"),
                ("queue_agent_banned", "boolean"),
                ("synchronization_error", "any"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
    ),
    Conversion(
        "queue_agent_object_mapping",
        table_info=TableInfo(
            [
                ("object", "string"),
            ],
            [
                ("host", "string"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
    ),
    Conversion(
        "consumer_registrations",
        table_info=TableInfo(
            [
                ("queue_cluster", "string"),
                ("queue_path", "string"),
                ("consumer_cluster", "string"),
                ("consumer_path", "string"),
            ],
            [
                ("vital", "boolean"),
                ("partitions", "any"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
        filter_callback=_replicated_tables_filter_callback,
    ),
    Conversion(
        "replicated_table_mapping",
        table_info=TableInfo(
            [
                ("cluster", "string"),
                ("path", "string"),
            ],
            [
                ("revision", "uint64"),
                ("object_type", "string"),
                ("meta", "any"),
                ("synchronization_error", "any"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
        filter_callback=_replicated_tables_filter_callback,
    ),
]

MIGRATION = Migration(
    initial_table_infos=INITIAL_TABLE_INFOS,
    initial_version=INITIAL_VERSION,
    transforms=TRANSFORMS,
    actions=ACTIONS,
)


################################################################################


MIGRATION_SCHEMAS = MIGRATION.get_schemas()

QUEUE_TABLE_SCHEMA = MIGRATION_SCHEMAS["queues"]

CONSUMER_TABLE_SCHEMA = MIGRATION_SCHEMAS["consumers"]

QUEUE_AGENT_OBJECT_MAPPING_TABLE_SCHEMA = MIGRATION_SCHEMAS["queue_agent_object_mapping"]

REGISTRATION_TABLE_SCHEMA = MIGRATION_SCHEMAS["consumer_registrations"]

REPLICATED_TABLE_MAPPING_TABLE_SCHEMA = MIGRATION_SCHEMAS["replicated_table_mapping"]

CONSUMER_OBJECT_TABLE_SCHEMA_WITHOUT_META = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "partition_index", "type": "uint64", "sort_order": "ascending", "required": True},
    {"name": "offset", "type": "uint64", "required": True},
]

CONSUMER_OBJECT_TABLE_SCHEMA = CONSUMER_OBJECT_TABLE_SCHEMA_WITHOUT_META + [
    {"name": "meta", "type": "any", "required": False},
]

PRODUCER_OBJECT_TABLE_SCHEMA = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "session_id", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "sequence_number", "type": "int64", "required": True},
    {"name": "epoch", "type": "int64", "required": True},
    {"name": "user_meta", "type": "any", "required": False},
    {"name": "system_meta", "type": "any", "required": False},
]


def _select_tablet_cell_bundle(client, desired_tablet_cell_bundle):
    # NB(apachee): Iterate over all choices and fallback to the next one, if desired does not exist.
    for index, tablet_cell_bundle in enumerate(QUEUE_AGENT_STATE_TABLET_CELL_BUNDLE_PRIORITY_LIST):
        if desired_tablet_cell_bundle == tablet_cell_bundle and not client.exists("//sys/tablet_cell_bundles/{}".format(tablet_cell_bundle)):
            if index + 1 == len(QUEUE_AGENT_STATE_TABLET_CELL_BUNDLE_PRIORITY_LIST):
                raise Exception("Last fallback tablet cell bundle {} does not exist".format(tablet_cell_bundle))
            desired_tablet_cell_bundle = QUEUE_AGENT_STATE_TABLET_CELL_BUNDLE_PRIORITY_LIST[index + 1]
    return desired_tablet_cell_bundle


def prepare_migration(client):
    def update_tablet_cell_bundle(table_info):
        tablet_cell_bundle = table_info.attributes.get("tablet_cell_bundle", QUEUE_AGENT_STATE_DEFAULT_TABLET_CELL_BUNDLE)
        table_info.attributes["tablet_cell_bundle"] = _select_tablet_cell_bundle(client, tablet_cell_bundle)

    initial_table_infos = deepcopy(INITIAL_TABLE_INFOS)
    transforms = deepcopy(TRANSFORMS)

    for table_info in initial_table_infos.values():
        update_tablet_cell_bundle(table_info)

    for transform in transforms.values():
        for conversion in transform:
            if conversion.table_info:
                update_tablet_cell_bundle(conversion.table_info)

    return Migration(
        initial_table_infos=initial_table_infos,
        initial_version=INITIAL_VERSION,
        transforms=transforms,
        actions=ACTIONS,
    )


def get_latest_version():
    """ Get latest version of the queue agent state migration """
    return MIGRATION.get_latest_version()


# Warning! This function does NOT perform actual transformations, it only creates tables with latest schemas.
def create_tables_latest_version(client, root=DEFAULT_ROOT, shard_count=DEFAULT_SHARD_COUNT, override_tablet_cell_bundle="default"):
    """ Creates queue agent state tables of latest version """

    if override_tablet_cell_bundle is None:
        migration = prepare_migration(client)
    else:
        # NB(apachee): No reason to prepare migration if override_tablet_cell_bundle is not None
        migration = MIGRATION

    migration.create_tables(
        client=client,
        target_version=MIGRATION.get_latest_version(),
        tables_path=root,
        shard_count=shard_count,
        override_tablet_cell_bundle=override_tablet_cell_bundle,
        force_initialize=True,
    )


def delete_all_tables(client, root=DEFAULT_ROOT):
    """ Deletes all of queue agent state tables """
    for table_name in MIGRATION_SCHEMAS.keys():
        client.remove("{}/{}".format(root, table_name), force=True)
    client.remove("{}/@version".format(root), force=True)


################################################################################


# NB(apachee): Script version returned by --version flag. Used
# to write code compatible across different version of this script,
# e.g. k8s operator.
SCRIPT_VERSION = 1


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Transform queue agent state")

    parser.add_argument("--version", action="version", version=str(SCRIPT_VERSION))

    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])
    parser.add_argument("--root", type=str, default=DEFAULT_ROOT,
                        help="Root directory for state tables; defaults to {}".format(DEFAULT_ROOT))
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--force", action="store_true", default=False)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--target-version", type=int)
    group.add_argument("--latest", action="store_true")

    return parser


# Creates missing tables for compatability with previous version of init_queue_agent_state,
# and handle cases when some tables were removed manually.
def _create_missing_tables(client, root, migration):
    if not client.exists(root):
        # This case is handled by migrationlib
        return

    for table, table_info in migration.initial_table_infos.items():
        table_path = ypath_join(root, table)
        if not client.exists(table_path):
            table_info.create_dynamic_table(client, table_path)


def run_migration(migration, client, tables_path, shard_count, target_version, force):
    migration.run(
        client,
        tables_path=tables_path,
        shard_count=shard_count,
        target_version=target_version,
        force=force,
    )


def main():
    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])

    migration = prepare_migration(client)

    _create_missing_tables(client, args.root, migration)

    target_version = args.target_version
    if args.latest:
        target_version = MIGRATION.get_latest_version()

    run_migration(migration, client, args.root, args.shard_count, target_version, args.force)


if __name__ == "__main__":
    main()
