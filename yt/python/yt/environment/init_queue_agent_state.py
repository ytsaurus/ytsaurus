#!/usr/bin/python3

from yt.wrapper import config, YtClient

from yt.environment.migrationlib import TableInfo, Migration, Conversion

import argparse

################################################################################


def _replicated_tables_filter_callback(client, table_path):
    """
    Filters out replicated tables.
    """
    upstream_replica_id = client.get("{}/@upstream_replica_id".format(table_path))
    return upstream_replica_id != "0-0-0-0"


DEFAULT_TABLET_CELL_BUNDLE = "sys"
DEFAULT_SHARD_COUNT = 1

DEFAULT_TABLE_ATTRIBUTES = {
    "tablet_cell_bundle": DEFAULT_TABLET_CELL_BUNDLE,
}

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
        attributes=DEFAULT_TABLE_ATTRIBUTES,
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
        attributes=DEFAULT_TABLE_ATTRIBUTES,
    ),
    "queue_agent_object_mapping": TableInfo(
        [
            ("object", "string"),
        ],
        [
            ("host", "string"),
        ],
        optimize_for="lookup",
        attributes=DEFAULT_TABLE_ATTRIBUTES,
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
        attributes=DEFAULT_TABLE_ATTRIBUTES,
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
        attributes=DEFAULT_TABLE_ATTRIBUTES,
    ),
}

DEFAULT_ROOT = "//sys/queue_agents"

INITIAL_VERSION = 0
TRANSFORMS = {}
ACTIONS = {}

# NB(apachee): Don't forget to add _replicated_tables_filter_callback as filter_callback
# for all conversions on tables, which might be replicated in some environments such as
# consumer_registrations or replicated_table_mapping.

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
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
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
    {"name": "sequence_number", "type": "uint64", "required": True},
    {"name": "epoch", "type": "uint64", "required": True},
    {"name": "user_meta", "type": "any", "required": False},
    {"name": "system_meta", "type": "any", "required": False},
]


def get_latest_version():
    """ Get latest version of the queue agent state migration """
    return MIGRATION.get_latest_version()


# Warning! This function does NOT perform actual transformations, it only creates tables with latest schemas.
def create_tables_latest_version(client, root=DEFAULT_ROOT, shard_count=DEFAULT_SHARD_COUNT, override_tablet_cell_bundle=None):
    """ Creates queue agent state tables of latest version """

    MIGRATION.create_tables(
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


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Transform queue agent state")

    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])
    parser.add_argument("--root", type=str, default=DEFAULT_ROOT,
                        help="Root directory for state tables; defaults to {}".format(DEFAULT_ROOT))
    parser.add_argument("--tablet-cell-bundle", type=str, default=DEFAULT_TABLET_CELL_BUNDLE,
                        help="Tablet cell bundle for queue agent state tables; defaults to {}".format(DEFAULT_TABLET_CELL_BUNDLE))
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--force", action="store_true", default=False)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--target-version", type=int)
    group.add_argument("--latest", action="store_true")

    return parser


def main():
    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])

    target_version = args.target_version
    if args.latest:
        target_version = MIGRATION.get_latest_version()

    MIGRATION.run(
        client,
        tables_path=args.root,
        shard_count=args.shard_count,
        target_version=target_version,
        force=args.force
    )


if __name__ == "__main__":
    main()
