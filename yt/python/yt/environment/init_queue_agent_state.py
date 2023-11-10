#!/usr/bin/python3

from yt.wrapper import config, YtClient

import argparse

################################################################################


QUEUE_TABLE_SCHEMA = [
    {"name": "cluster", "type": "string", "sort_order": "ascending"},
    {"name": "path", "type": "string", "sort_order": "ascending"},
    {"name": "row_revision", "type": "uint64"},
    {"name": "revision", "type": "uint64"},
    {"name": "object_type", "type": "string"},
    {"name": "dynamic", "type": "boolean"},
    {"name": "sorted", "type": "boolean"},
    {"name": "auto_trim_config", "type": "any"},
    {"name": "static_export_config", "type": "any"},
    {"name": "queue_agent_stage", "type": "string"},
    {"name": "object_id", "type": "string"},
    {"name": "synchronization_error", "type": "any"},
]
DEFAULT_QUEUE_TABLE_NAME = "queues"

CONSUMER_TABLE_SCHEMA = [
    {"name": "cluster", "type": "string", "sort_order": "ascending"},
    {"name": "path", "type": "string", "sort_order": "ascending"},
    {"name": "row_revision", "type": "uint64"},
    {"name": "revision", "type": "uint64"},
    {"name": "object_type", "type": "string"},
    {"name": "treat_as_queue_consumer", "type": "boolean"},
    {"name": "schema", "type": "any"},
    {"name": "queue_agent_stage", "type": "string"},
    {"name": "synchronization_error", "type": "any"},
]
DEFAULT_CONSUMER_TABLE_NAME = "consumers"

QUEUE_AGENT_OBJECT_MAPPING_TABLE_SCHEMA = [
    {"name": "object", "type": "string", "sort_order": "ascending"},
    {"name": "host", "type": "string"},
]
DEFAULT_QUEUE_AGENT_OBJECT_MAPPING_TABLE_NAME = "queue_agent_object_mapping"

REGISTRATION_TABLE_SCHEMA = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending"},
    {"name": "queue_path", "type": "string", "sort_order": "ascending"},
    {"name": "consumer_cluster", "type": "string", "sort_order": "ascending"},
    {"name": "consumer_path", "type": "string", "sort_order": "ascending"},
    {"name": "vital", "type": "boolean"},
    {"name": "partitions", "type": "any"},
]

REPLICATED_TABLE_MAPPING_TABLE_SCHEMA = [
    {"name": "cluster", "type": "string", "sort_order": "ascending"},
    {"name": "path", "type": "string", "sort_order": "ascending"},
    {"name": "revision", "type": "uint64"},
    {"name": "object_type", "type": "string"},
    {"name": "meta", "type": "any"},
    {"name": "synchronization_error", "type": "any"},
]

DEFAULT_ROOT = "//sys/queue_agents"
DEFAULT_REGISTRATION_TABLE_PATH = DEFAULT_ROOT + "/consumer_registrations"
DEFAULT_REPLICATED_TABLE_MAPPING_TABLE_PATH = DEFAULT_ROOT + "/replicated_table_mapping"

CONSUMER_OBJECT_TABLE_SCHEMA = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "partition_index", "type": "uint64", "sort_order": "ascending", "required": True},
    {"name": "offset", "type": "uint64", "required": True},
]


################################################################################


def create_table(client, path, schema, **kwargs):
    client.create("table", path, attributes={"dynamic": True, "schema": schema}, **kwargs)
    client.mount_table(path, sync=True)


def create_tables(client,
                  root=DEFAULT_ROOT,
                  registration_table_path=DEFAULT_REGISTRATION_TABLE_PATH,
                  replicated_table_mapping_table_path=DEFAULT_REPLICATED_TABLE_MAPPING_TABLE_PATH,
                  skip_queues=False,
                  skip_consumers=False,
                  skip_object_mapping=False,
                  create_registration_table=False,
                  create_replicated_table_mapping_table=False,
                  queue_table_schema=None, consumer_table_schema=None, object_mapping_schema=None,
                  registration_table_schema=None,
                  replicated_table_mapping_table_schema=None,
                  **kwargs):
    queue_table_schema = queue_table_schema or QUEUE_TABLE_SCHEMA
    consumer_table_schema = consumer_table_schema or CONSUMER_TABLE_SCHEMA
    object_mapping_schema = object_mapping_schema or QUEUE_AGENT_OBJECT_MAPPING_TABLE_SCHEMA
    registration_table_schema = registration_table_schema or REGISTRATION_TABLE_SCHEMA
    replicated_table_mapping_table_schema = replicated_table_mapping_table_schema or REPLICATED_TABLE_MAPPING_TABLE_SCHEMA

    if not skip_queues:
        create_table(client, "{}/{}".format(root, DEFAULT_QUEUE_TABLE_NAME), queue_table_schema, **kwargs)
    if not skip_consumers:
        create_table(client, "{}/{}".format(root, DEFAULT_CONSUMER_TABLE_NAME), consumer_table_schema, **kwargs)
    if not skip_object_mapping:
        create_table(client, "{}/{}".format(root, DEFAULT_QUEUE_AGENT_OBJECT_MAPPING_TABLE_NAME), object_mapping_schema, **kwargs)
    if create_registration_table:
        create_table(client, registration_table_path, registration_table_schema, **kwargs)
    if create_replicated_table_mapping_table:
        create_table(client, replicated_table_mapping_table_path, replicated_table_mapping_table_schema, **kwargs)


def delete_tables(client,
                  root=DEFAULT_ROOT,
                  registration_table_path=DEFAULT_REGISTRATION_TABLE_PATH,
                  replicated_table_mapping_table_path=DEFAULT_REPLICATED_TABLE_MAPPING_TABLE_PATH,
                  skip_queues=False, skip_consumers=False, skip_object_mapping=False,
                  skip_registration_table=False, skip_replicated_table_mapping_table=False):
    if not skip_queues:
        client.remove("{}/{}".format(root, DEFAULT_QUEUE_TABLE_NAME), force=True)
    if not skip_consumers:
        client.remove("{}/{}".format(root, DEFAULT_CONSUMER_TABLE_NAME), force=True)
    if not skip_object_mapping:
        client.remove("{}/{}".format(root, DEFAULT_QUEUE_AGENT_OBJECT_MAPPING_TABLE_NAME), force=True)
    if not skip_registration_table:
        client.remove(registration_table_path, force=True)
    if not skip_replicated_table_mapping_table:
        client.remove(replicated_table_mapping_table_path, force=True)


################################################################################


def build_arguments_parser():
    parser = argparse.ArgumentParser(description="Create queue agent state tables")

    parser.add_argument("--proxy", type=str, default=config["proxy"]["url"])

    parser.add_argument("--root", type=str, default=DEFAULT_ROOT,
                        help="Root directory for state tables; defaults to {}".format(DEFAULT_ROOT))
    parser.add_argument("--registration-table-path", type=str, default=DEFAULT_REGISTRATION_TABLE_PATH,
                        help="Path to table with queue consumer registrations; defaults to {}".format(DEFAULT_REGISTRATION_TABLE_PATH))
    parser.add_argument("--replicated-table-mapping-table-path", type=str, default=DEFAULT_REPLICATED_TABLE_MAPPING_TABLE_PATH,
                        help="Path to table with replicated table mapping; defaults to {}".format(DEFAULT_REPLICATED_TABLE_MAPPING_TABLE_PATH))

    parser.add_argument("--skip-queues", action="store_true", help="Do not create queue state table")
    parser.add_argument("--skip-consumers", action="store_true", help="Do not create consumer state table")
    parser.add_argument("--skip-object-mapping", action="store_true", help="Do not create queue agent object mapping table")
    parser.add_argument("--create-registration-table", action="store_true", help="Create registration state table")
    parser.add_argument("--create-replicated-table-mapping-table", action="store_true", help="Create replicated table mapping cache table")

    parser.add_argument("--recursive", action="store_true", help="Create all state tables with the --recursive option")
    parser.add_argument("--ignore-existing", action="store_true", help="Create all state tables with the --ignore-existing option")

    return parser


def main():
    args = build_arguments_parser().parse_args()
    client = YtClient(proxy=args.proxy, token=config["token"])
    create_tables(client,
                  root=args.root,
                  registration_table_path=args.registration_table_path,
                  replicated_table_mapping_table_path=args.replicated_table_mapping_table_path,
                  skip_queues=args.skip_queues,
                  skip_consumers=args.skip_consumers,
                  skip_object_mapping=args.skip_object_mapping,
                  create_registration_table=args.create_registration_table,
                  create_replicated_table_mapping_table=args.create_replicated_table_mapping_table,
                  recursive=args.recursive,
                  ignore_existing=args.ignore_existing)


if __name__ == "__main__":
    main()
