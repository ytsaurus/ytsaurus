from typing import Any

PIPELINE_FILES: list[str] = [
    "flow_view",
    "spec",
    "dynamic_spec",
    "important_versions",
]

PIPELINE_TABLES: dict[str, Any] = {
    "timer_messages": {
        "schema": [
            {"name": "computation_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "key", "type": "any", "sort_order": "ascending", "group": "default"},
            {"name": "message_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "message", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "system_timestamp", "type": "uint64", "group": "default"},
            {"name": "codec", "type": "int64", "group": "default"},
        ],
    },
    "input_messages": {
        "schema": [
            {"name": "computation_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "key", "type": "any", "sort_order": "ascending", "group": "default"},
            {"name": "message_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "system_timestamp", "type": "uint64", "group": "default"},
        ],
    },
    "output_messages": {
        "schema": [
            {
                "name": "hash",
                "expression": "farm_hash(partition_id)",
                "type": "uint64",
                "sort_order": "ascending",
                "group": "default",
            },
            {"name": "partition_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "message_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "message", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "system_timestamp", "type": "uint64", "group": "default"},
            {"name": "codec", "type": "int64", "group": "default"},
        ],
    },
    "partition_data": {
        "schema": [
            {
                "name": "hash",
                "expression": "farm_hash(partition_id)",
                "type": "uint64",
                "sort_order": "ascending",
                "group": "default",
            },
            {"name": "partition_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "watermarks", "type": "string", "group": "default"},
            {"name": "offsets", "type": "string", "group": "default"},
            {"name": "meta_field_setter", "type": "string", "group": "default"},
        ],
    },
}

PIPELINE_QUEUES: dict[str, Any] = {
    "controller_logs": {
        "schema": [
            {"name": "host", "type": "string"},
            {"name": "data", "type": "string"},
            {"name": "codec", "type": "string"},
            {"name": "$timestamp", "type": "uint64"},
            {"name": "$cumulative_data_weight", "type": "int64"},
        ],
    },
}
