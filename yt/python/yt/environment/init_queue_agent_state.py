#!/usr/bin/python3

from yt.wrapper import YtClient, ypath_split, ypath_join
from yt.wrapper.default_config import get_config_from_env

from yt.environment.migrationlib import TableInfo, Migration, Conversion, TypeV3

import argparse

from abc import ABC, abstractmethod

from copy import deepcopy

import logging

################################################################################


def _replicated_table_filter_callback(client, table_path):
    """
    Filters out replicated tables and replicas.
    """
    if not client.exists(table_path):
        return True

    table_type = client.get("{}/@type".format(table_path))

    if table_type != "table":
        return False

    upstream_replica_id = client.get("{}/@upstream_replica_id".format(table_path))
    return upstream_replica_id == "0-0-0-0"


def _create_replicated_table_index_filter_callback(source_table_name):
    def replicated_table_index_filter_callback(client, table_path):
        root, _ = ypath_split(table_path)
        return (
            _replicated_table_filter_callback(client=client, table_path=ypath_join(root, source_table_name))
            and _replicated_table_filter_callback(client=client, table_path=table_path)
        )

    return replicated_table_index_filter_callback


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


class ReconfigurableAction(ABC):
    @abstractmethod
    def reconfigure(self, config):
        pass

    @abstractmethod
    def __call__(self, client):
        pass


class CreateSecondaryIndexAction(ReconfigurableAction):
    def __init__(self, table_name, index_table_name, secondary_index_attributes, table_filter_callback=None, index_table_filter_callback=None):
        super().__init__()

        self.table_name = table_name
        self.index_table_name = index_table_name
        self.secondary_index_attributes = secondary_index_attributes
        self.table_filter_callback = table_filter_callback
        self.index_table_filter_callback = index_table_filter_callback

    def reconfigure(self, config):
        root = config["root"]
        self.table_path = f"{root}/{self.table_name}"
        self.index_table_path = f"{root}/{self.index_table_name}"
        self.secondary_index_attributes.update({
            "table_path": self.table_path,
            "index_table_path": self.index_table_path,
        })

    def __call__(self, client):
        if self.table_filter_callback and not self.table_filter_callback(client=client, table_path=self.table_path):
            logging.info(f"Secondary index {self.index_table_name} skipped by table filter callback")
            return

        if self.index_table_filter_callback and not self.index_table_filter_callback(client=client, table_path=self.index_table_path):
            logging.info(f"Secondary index {self.index_table_name} skipped by index table filter callback")
            return

        logging.info(f"Secondary index debug info: {self.secondary_index_attributes=}, "
                     f"{client.get(f'{self.table_path}/@schema')=} "
                     f"{client.get(f'{self.index_table_path}/@schema')=}")

        client.unmount_table(self.secondary_index_attributes["table_path"], sync=True)
        client.unmount_table(self.secondary_index_attributes["index_table_path"], sync=True)

        client.create("secondary_index", attributes=self.secondary_index_attributes)

        client.mount_table(self.secondary_index_attributes["table_path"], sync=True)
        client.mount_table(self.secondary_index_attributes["index_table_path"], sync=True)


DEFAULT_ROOT = "//sys/queue_agents"

INITIAL_VERSION = 0
TRANSFORMS = {}
ACTIONS = {}

# NB(apachee): Don't forget to add _replicated_table_filter_callback as filter_callback
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
        filter_callback=_replicated_table_filter_callback,
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
        filter_callback=_replicated_table_filter_callback,
    ),
]

# Add profiling tags to queues, consumers tables.
TRANSFORMS[4] = [
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
                ("queue_profiling_tag", "string"),  # new field
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
                ("queue_consumer_profiling_tag", "string"),  # new field
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
    ),
]

# Add replica_mapping index.
TRANSFORMS[5] = [
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
                ("replica_list", TypeV3({
                    "type_name": "optional",
                    "item": {
                        "type_name": "list",
                        "item": "string",
                    },
                })),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
        filter_callback=_replicated_table_filter_callback,
    ),
    Conversion(
        "replica_mapping",
        table_info=TableInfo(
            [
                ("replica_list", "string", None, {"required": True}),
                ("cluster", "string"),
                ("path", "string"),
            ],
            [
                ("$empty", "int64"),
            ],
            optimize_for="lookup",
            attributes=DEFAULT_TABLE_ATTRIBUTES,
        ),
        filter_callback=_create_replicated_table_index_filter_callback("replicated_table_mapping"),
    ),
]

# Add secondary_index between replica_mapping and replicated_table_mapping.
# Actual paths are set in prepare_migration.
ACTIONS[6] = [
    CreateSecondaryIndexAction(
        table_name="replicated_table_mapping",
        index_table_name="replica_mapping",
        secondary_index_attributes={
            "kind": "unfolding",
            "unfolded_column": "replica_list",
            "table_to_index_correspondence": "bijective",
        },
        table_filter_callback=_replicated_table_filter_callback,
        index_table_filter_callback=_create_replicated_table_index_filter_callback("replicated_table_mapping"),
    )
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

REPLICA_MAPPING_TABLE_SCHEMA = MIGRATION_SCHEMAS["replica_mapping"]

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


def _select_tablet_cell_bundle(client, desired_tablet_cell_bundle, override_tablet_cell_bundle=None):
    if override_tablet_cell_bundle:
        return override_tablet_cell_bundle

    # NB(apachee): Iterate over all choices and fallback to the next one, if desired does not exist.
    for index, tablet_cell_bundle in enumerate(QUEUE_AGENT_STATE_TABLET_CELL_BUNDLE_PRIORITY_LIST):
        if desired_tablet_cell_bundle == tablet_cell_bundle and not client.exists("//sys/tablet_cell_bundles/{}".format(tablet_cell_bundle)):
            if index + 1 == len(QUEUE_AGENT_STATE_TABLET_CELL_BUNDLE_PRIORITY_LIST):
                raise Exception("Last fallback tablet cell bundle {} does not exist".format(tablet_cell_bundle))
            desired_tablet_cell_bundle = QUEUE_AGENT_STATE_TABLET_CELL_BUNDLE_PRIORITY_LIST[index + 1]
    return desired_tablet_cell_bundle


def prepare_migration(client, root, override_tablet_cell_bundle=None):
    def update_tablet_cell_bundle(table_info):
        tablet_cell_bundle = table_info.attributes.get("tablet_cell_bundle", QUEUE_AGENT_STATE_DEFAULT_TABLET_CELL_BUNDLE)
        table_info.attributes["tablet_cell_bundle"] = _select_tablet_cell_bundle(client, tablet_cell_bundle, override_tablet_cell_bundle=override_tablet_cell_bundle)

    initial_table_infos = deepcopy(INITIAL_TABLE_INFOS)
    transforms = deepcopy(TRANSFORMS)
    actions = deepcopy(ACTIONS)

    for table_info in initial_table_infos.values():
        update_tablet_cell_bundle(table_info)

    for transform in transforms.values():
        for conversion in transform:
            if conversion.table_info:
                update_tablet_cell_bundle(conversion.table_info)

    reconfigureable_action_config = {
        "root": root,
    }

    for _, version_actions in actions.items():
        for action in version_actions:
            if isinstance(action, ReconfigurableAction):
                action.reconfigure(config=reconfigureable_action_config)

    return Migration(
        initial_table_infos=initial_table_infos,
        initial_version=INITIAL_VERSION,
        transforms=transforms,
        actions=actions,
    )


def get_latest_version():
    """ Get latest version of the queue agent state migration """
    return MIGRATION.get_latest_version()


# Warning! This function does NOT perform actual transformations, it only creates tables with latest schemas.
def create_tables_latest_version(client, root=DEFAULT_ROOT, shard_count=DEFAULT_SHARD_COUNT, override_tablet_cell_bundle="default"):
    """ Creates queue agent state tables of latest version """

    migration = prepare_migration(client, root=root, override_tablet_cell_bundle=override_tablet_cell_bundle)

    delete_all_tables(client, root)

    migration.run(
        client=client,
        target_version=migration.get_latest_version(),
        tables_path=root,
        shard_count=shard_count,
        force=False,
        retransform=False,
        pool=None,
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

    parser.add_argument("--proxy", type=str, default=None)
    parser.add_argument("--root", type=str, default=DEFAULT_ROOT,
                        help="Root directory for state tables; defaults to {}".format(DEFAULT_ROOT))
    parser.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    parser.add_argument("--force", action="store_true", default=False)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--target-version", type=int)
    group.add_argument("--latest", action="store_true")

    return parser


def run_migration(client, root, target_version=None, shard_count=1, force=False, migration=None):
    if migration is None:
        migration = prepare_migration(client, root=root)

    if target_version is None:
        target_version = migration.get_latest_version()

    migration.run(
        client=client,
        tables_path=root,
        shard_count=shard_count,
        target_version=target_version,
        force=force,
        retransform=False,
        pool=None,
    )


def main():
    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, config=get_config_from_env())

    target_version = args.target_version
    if args.latest:
        target_version = MIGRATION.get_latest_version()

    run_migration(
        client=client,
        root=args.root,
        target_version=target_version,
        shard_count=args.shard_count,
        force=args.force)


if __name__ == "__main__":
    main()
