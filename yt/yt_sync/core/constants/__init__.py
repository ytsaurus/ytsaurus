from typing import Any

KB: int = 1024
MB: int = KB * KB
GB: int = MB * KB

CONSUMER_SCHEMA: list[dict[str, Any]] = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "partition_index", "type": "uint64", "sort_order": "ascending", "required": True},
    {"name": "offset", "type": "uint64", "required": True},
    {"name": "meta", "type": "any", "required": False},
]

CONSUMER_ATTRS: dict[str, Any] = {
    "dynamic": True,
    "enable_dynamic_store_read": True,
    "mount_config": {
        "merge_rows_on_flush": True,
        "min_compaction_store_count": 2,
        "min_data_ttl": 0,
        "min_partitioning_data_size": 1,
    },
    "treat_as_queue_consumer": True,
}
