QUEUE_TABLE_SCHEMA = [
    {"name": "cluster", "type": "string", "sort_order": "ascending"},
    {"name": "path", "type": "string", "sort_order": "ascending"},
    {"name": "row_revision", "type": "uint64"},
    {"name": "revision", "type": "uint64"},
    {"name": "object_type", "type": "string"},
    {"name": "dynamic", "type": "boolean"},
    {"name": "sorted", "type": "boolean"},
    {"name": "synchronization_error", "type": "any"},
]

CONSUMER_TABLE_SCHEMA = [
    {"name": "cluster", "type": "string", "sort_order": "ascending"},
    {"name": "path", "type": "string", "sort_order": "ascending"},
    {"name": "row_revision", "type": "uint64"},
    {"name": "revision", "type": "uint64"},
    {"name": "target_cluster", "type": "string"},
    {"name": "target_path", "type": "string"},
    {"name": "object_type", "type": "string"},
    {"name": "treat_as_queue_consumer", "type": "boolean"},
    {"name": "schema", "type": "any"},
    {"name": "vital", "type": "boolean"},
    {"name": "owner", "type": "string"},
    {"name": "synchronization_error", "type": "any"},
]


def create_tables(client, root="//sys/queue_agents", skip_queues=False, skip_consumers=False, queue_table_schema=None,
                  consumer_table_schema=None):
    queue_table_schema = queue_table_schema or QUEUE_TABLE_SCHEMA
    consumer_table_schema = consumer_table_schema or CONSUMER_TABLE_SCHEMA
    if not skip_queues:
        client.create("table", root + "/queues", attributes={"dynamic": True, "schema": queue_table_schema})
    if not skip_consumers:
        client.create("table", root + "/consumers", attributes={"dynamic": True, "schema": consumer_table_schema})
