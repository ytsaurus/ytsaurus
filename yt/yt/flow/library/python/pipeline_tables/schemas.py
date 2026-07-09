from typing import Any

PIPELINE_FILES: list[str] = []


PIPELINE_TABLES: dict[str, Any] = {
    "timers": {
        "schema": [
            {"name": "computation_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "key", "type": "any", "sort_order": "ascending", "group": "default"},
            {"name": "message_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "stream_id", "type": "string", "group": "default"},
            {"name": "system_timestamp", "type": "uint64", "group": "default"},
            {"name": "event_timestamp", "type": "uint64", "group": "default"},
            {"name": "trigger_timestamp", "type": "uint64", "group": "default"},
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
    "compact_input_messages": {
        "schema": [
            {"name": "deduplication_message_key", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "system_timestamp", "type": "uint64", "group": "default"},
        ],
    },
    "compact_output_messages": {
        "schema": [
            {"name": "computation_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "key", "type": "any", "sort_order": "ascending", "group": "default"},
            {"name": "stream_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "chunk_id", "type": "int64", "sort_order": "ascending", "group": "default"},
            {"name": "data", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "data_codec", "type": "int64", "group": "default"},
            {"name": "processed_mask", "type": "string", "group": "default"},
        ],
    },
    "compact_partition_output_messages": {
        "schema": [
            {
                "name": "hash",
                "expression": "farm_hash(partition_id)",
                "type": "uint64",
                "sort_order": "ascending",
                "group": "default",
            },
            {"name": "partition_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "stream_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "chunk_id", "type": "int64", "sort_order": "ascending", "group": "default"},
            {"name": "data", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "data_codec", "type": "int64", "group": "default"},
            {"name": "processed_mask", "type": "string", "group": "default"},
        ],
    },
    "states": {
        "schema": [
            {"name": "computation_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "key", "type": "any", "sort_order": "ascending", "group": "default"},
            {"name": "name", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "state", "type": "any", "group": "default", "max_inline_hunk_size": 128},
            {"name": "compressed", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "compressed_patch", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "format", "type": "any", "group": "default", "max_inline_hunk_size": 128},
        ],
    },
    "partition_states": {
        "schema": [
            {
                "name": "hash",
                "expression": "farm_hash(partition_id)",
                "type": "uint64",
                "sort_order": "ascending",
                "group": "default",
            },
            {"name": "partition_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "name", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "state", "type": "any", "group": "default", "max_inline_hunk_size": 128},
            {"name": "compressed", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "compressed_patch", "type": "string", "group": "default", "max_inline_hunk_size": 128},
            {"name": "format", "type": "any", "group": "default", "max_inline_hunk_size": 128},
        ],
    },
    "flow_state": {
        "schema": [
            {"name": "sequence_id", "type": "int64", "sort_order": "ascending", "group": "default"},
            {"name": "flags", "type": "uint64", "group": "default"},
            {"name": "state_name", "type": "string", "group": "default"},
            {"name": "key_left", "type": "string", "group": "default"},
            {"name": "key_right", "type": "string", "group": "default"},
            {"name": "value", "type": "any", "group": "default"},
        ],
    },
    "flow_state_obsolete": {
        "schema": [
            {"name": "key", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "value", "type": "any", "group": "default"},
        ],
    },
    "flow_control": {
        "schema": [
            {"name": "key", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "value", "type": "any", "group": "default"},
        ],
    },
    "key_visitor_states": {
        "schema": [
            {"name": "computation_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "stream_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "key", "type": "any", "sort_order": "ascending", "group": "default"},
            {"name": "is_lower", "type": "boolean", "sort_order": "ascending", "group": "default"},
            {"name": "state", "type": "any", "group": "default", "max_inline_hunk_size": 128},
        ],
    },
    "partition_transactions": {
        "schema": [
            {
                "name": "hash",
                "expression": "farm_hash(partition_id)",
                "type": "uint64",
                "sort_order": "ascending",
                "group": "default",
            },
            {"name": "partition_id", "type": "string", "sort_order": "ascending", "group": "default"},
            {"name": "last_transaction_start_timestamp", "type": "uint64", "group": "default"},
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
